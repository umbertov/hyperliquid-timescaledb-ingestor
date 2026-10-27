//! Collects settled hourly funding rates from Hyperliquid's `fundingHistory` info request.
//!
//! The poller visits each collected perpetual market one request at a time. For each
//! market it starts just after the newest stored settlement, or at the configured
//! backfill start when no settlement is stored. It pages forward until the API returns
//! no rows. Rows insert with `ON CONFLICT DO NOTHING`, so a restart or an overlapping
//! poll creates no duplicates.

use crate::models::FundingRateRow;
use crate::{millis_to_datetime, newest_funding_time, write_funding_rates};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use color_eyre::eyre::{eyre, Result, WrapErr};
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::PgConnection;
use hyperliquid_rust_sdk::{FundingHistoryResponse, InfoClient};
use std::time::Duration;
use tracing::info;

/// Wait before each `fundingHistory` request. The info endpoint allows 1,200 REST
/// weight per minute per IP, and the documented base weight of an info request is 20.
/// One request per second stays under that budget.
const REQUEST_SPACING: Duration = Duration::from_secs(1);

type DbPool = Pool<ConnectionManager<PgConnection>>;

/// A perpetual market to poll, with its `symbols` table ID.
#[derive(Clone, Debug, PartialEq)]
pub struct PerpMarket {
    pub name: String,
    pub symbol: i32,
}

/// Returns the first millisecond to request. It starts one millisecond after the newest
/// stored settlement, so the stored row is not requested again.
pub fn start_millis(newest: Option<DateTime<Utc>>, backfill_start: DateTime<Utc>) -> Result<u64> {
    let start = match newest {
        Some(time) => time + chrono::Duration::milliseconds(1),
        None => backfill_start,
    };
    u64::try_from(start.timestamp_millis())
        .wrap_err_with(|| format!("funding start {start} is before the Unix epoch"))
}

/// Returns the start of the next page, or `None` when the page is empty and polling of
/// this market is complete. Fails if a row precedes `start`, because the API must honor
/// `startTime`.
pub fn next_cursor(start: u64, page: &[FundingHistoryResponse]) -> Result<Option<u64>> {
    let mut newest = None;
    for row in page {
        if row.time < start {
            return Err(eyre!(
                "fundingHistory returned time {} before startTime {start}",
                row.time
            ));
        }
        newest = Some(newest.map_or(row.time, |newest: u64| newest.max(row.time)));
    }
    Ok(newest.map(|time| time + 1))
}

/// Converts one `fundingHistory` page into rows. Fails on a row for another coin or on a
/// decimal field that is not an exact decimal number.
pub fn funding_rows(
    coin: &str,
    symbol: i32,
    page: Vec<FundingHistoryResponse>,
) -> Result<Vec<FundingRateRow>> {
    page.into_iter()
        .map(|row| -> Result<FundingRateRow> {
            if row.coin != coin {
                return Err(eyre!(
                    "fundingHistory for {coin} returned a row for {}",
                    row.coin
                ));
            }
            Ok(FundingRateRow {
                time: millis_to_datetime(row.time)?,
                symbol,
                funding_rate: parse_decimal(&row.funding_rate, "fundingRate", coin)?,
                premium: parse_decimal(&row.premium, "premium", coin)?,
            })
        })
        .collect()
}

fn parse_decimal(value: &str, field: &str, coin: &str) -> Result<BigDecimal> {
    value
        .parse::<BigDecimal>()
        .wrap_err_with(|| format!("parsing {coin} funding {field} {value:?}"))
}

/// Polls every market in `markets` forever, sleeping `poll_interval` after each full
/// pass. The first pass starts at once, so startup backfills the gap since the last
/// stored settlement.
pub async fn run(
    markets: Vec<PerpMarket>,
    info: InfoClient,
    pool: DbPool,
    poll_interval: Duration,
    backfill_start: DateTime<Utc>,
) -> Result<()> {
    loop {
        for market in &markets {
            poll_market(&info, &pool, market, backfill_start).await?;
        }
        info!(market_count = markets.len(), "polled funding history");
        tokio::time::sleep(poll_interval).await;
    }
}

async fn poll_market(
    info: &InfoClient,
    pool: &DbPool,
    market: &PerpMarket,
    backfill_start: DateTime<Utc>,
) -> Result<()> {
    let newest = {
        let pool = pool.clone();
        let symbol = market.symbol;
        tokio::task::spawn_blocking(move || -> Result<Option<DateTime<Utc>>> {
            let mut conn = pool
                .get()
                .wrap_err("getting a pooled database connection")?;
            newest_funding_time(&mut conn, symbol)
        })
        .await
        .wrap_err("reading newest funding time failed")??
    };
    let mut cursor = start_millis(newest, backfill_start)?;
    loop {
        tokio::time::sleep(REQUEST_SPACING).await;
        let page = info
            .funding_history(market.name.clone(), cursor, None)
            .await
            .wrap_err_with(|| format!("requesting {} funding history", market.name))?;
        let Some(next) = next_cursor(cursor, &page)? else {
            return Ok(());
        };
        let rows = funding_rows(&market.name, market.symbol, page)?;
        let pool = pool.clone();
        tokio::task::spawn_blocking(move || -> Result<()> {
            let mut conn = pool
                .get()
                .wrap_err("getting a pooled database connection")?;
            write_funding_rates(&rows, &mut conn)
        })
        .await
        .wrap_err("writing funding rates failed")??;
        cursor = next;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn page(json: &str) -> Vec<FundingHistoryResponse> {
        serde_json::from_str(json).expect("fixture parses as fundingHistory rows")
    }

    const ETH_PAGE: &str = r#"[
        {"coin":"ETH","fundingRate":"-0.00022196","premium":"-0.00052196","time":1683849600076},
        {"coin":"ETH","fundingRate":"0.0000125","premium":"0.00000100","time":1683853200076}
    ]"#;

    #[test]
    fn parses_exact_decimals_and_settlement_time() {
        let rows = funding_rows("ETH", 7, page(ETH_PAGE)).expect("page converts");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].symbol, 7);
        assert_eq!(rows[0].funding_rate.to_string(), "-0.00022196");
        assert_eq!(rows[0].premium.to_string(), "-0.00052196");
        assert_eq!(rows[0].time.timestamp_millis(), 1683849600076);
        assert_eq!(rows[1].funding_rate.to_string(), "0.0000125");
        assert_eq!(rows[1].premium.to_string(), "0.00000100");
    }

    #[test]
    fn accepts_builder_dex_coin() {
        // Live fundingHistory response for coin "xyz:NVDA" on 2026-10-27.
        let text = r#"[{"coin":"xyz:NVDA","fundingRate":"0.00000625","premium":"0.0002929116","time":1793106000049}]"#;
        let rows = funding_rows("xyz:NVDA", 9, page(text)).expect("page converts");
        assert_eq!(rows[0].funding_rate.to_string(), "0.00000625");
        assert!(funding_rows("NVDA", 9, page(text)).is_err());
    }

    #[test]
    fn rejects_numeric_funding_rate() {
        let text = r#"[{"coin":"ETH","fundingRate":-0.00022196,"premium":"0","time":1}]"#;
        assert!(serde_json::from_str::<Vec<FundingHistoryResponse>>(text).is_err());
    }

    #[test]
    fn rejects_missing_premium() {
        let text = r#"[{"coin":"ETH","fundingRate":"0.1","time":1}]"#;
        assert!(serde_json::from_str::<Vec<FundingHistoryResponse>>(text).is_err());
    }

    #[test]
    fn rejects_top_level_object() {
        let text = r#"{"coin":"ETH","fundingRate":"0.1","premium":"0","time":1}"#;
        assert!(serde_json::from_str::<Vec<FundingHistoryResponse>>(text).is_err());
    }

    #[test]
    fn rejects_non_decimal_rate() {
        let rows = page(r#"[{"coin":"ETH","fundingRate":"abc","premium":"0","time":1}]"#);
        assert!(funding_rows("ETH", 7, rows).is_err());
    }

    #[test]
    fn rejects_row_for_another_coin() {
        assert!(funding_rows("ETH", 7, page(ETH_PAGE)).is_ok());
        let rows = page(r#"[{"coin":"BTC","fundingRate":"0.1","premium":"0","time":1}]"#);
        assert!(funding_rows("ETH", 7, rows).is_err());
    }

    #[test]
    fn start_follows_newest_stored_settlement() {
        let backfill = Utc
            .timestamp_millis_opt(1_600_000_000_000)
            .single()
            .unwrap();
        let newest = Utc
            .timestamp_millis_opt(1_683_849_600_000)
            .single()
            .unwrap();
        assert_eq!(
            start_millis(Some(newest), backfill).expect("start computes"),
            1_683_849_600_001
        );
    }

    #[test]
    fn start_uses_backfill_without_stored_settlement() {
        let backfill = Utc
            .timestamp_millis_opt(1_600_000_000_000)
            .single()
            .unwrap();
        assert_eq!(
            start_millis(None, backfill).expect("start computes"),
            1_600_000_000_000
        );
    }

    #[test]
    fn next_cursor_pages_past_the_newest_row() {
        let rows = page(ETH_PAGE);
        assert_eq!(
            next_cursor(1_683_849_600_000, &rows).expect("cursor computes"),
            Some(1_683_853_200_077)
        );
    }

    #[test]
    fn next_cursor_ends_on_empty_page() {
        assert_eq!(next_cursor(1, &[]).expect("cursor computes"), None);
    }

    #[test]
    fn next_cursor_rejects_row_before_start() {
        let rows = page(ETH_PAGE);
        assert!(next_cursor(1_683_853_200_000, &rows).is_err());
    }
}
