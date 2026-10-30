pub mod bbo;
pub mod dexes;
pub mod export;
pub mod funding;
pub mod models;
pub mod schema;
pub mod spot_map;

use chrono::{DateTime, Utc};
use color_eyre::eyre::{eyre, Result, WrapErr};
use diesel::dsl::{delete, insert_into, max};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use diesel::upsert::excluded;
use models::{BboRow, FundingRateRow, NewSymbol, OrderbookRow, PerpSpotPairRow, Symbol, TradeRow};
use schema::{bbo_messages, funding_rates, orderbook_messages, perp_spot_pairs, symbols, trades};
use std::collections::HashMap;

/// Converts a Hyperliquid millisecond timestamp into a UTC `DateTime`.
pub fn millis_to_datetime(millis: u64) -> Result<DateTime<Utc>> {
    let millis = i64::try_from(millis).wrap_err("timestamp exceeds signed range")?;
    DateTime::from_timestamp_millis(millis).ok_or_else(|| eyre!("invalid timestamp: {millis}"))
}

pub fn connection_pool(url: &str) -> Result<Pool<ConnectionManager<PgConnection>>> {
    Pool::builder()
        .max_size(8)
        .build(ConnectionManager::<PgConnection>::new(url))
        .wrap_err("creating PostgreSQL connection pool")
}

pub fn sync_symbols(
    markets: &[(String, &'static str)],
    conn: &mut PgConnection,
) -> Result<HashMap<String, i32>> {
    let mut unique_names = HashMap::new();
    for (name, market_type) in markets {
        if !matches!(*market_type, "perp" | "spot") {
            return Err(color_eyre::eyre::eyre!(
                "unsupported market type {market_type:?}"
            ));
        }
        if let Some(previous_type) = unique_names.insert(name.as_str(), *market_type) {
            if previous_type != *market_type {
                return Err(color_eyre::eyre::eyre!(
                    "market name {name:?} has conflicting market types"
                ));
            }
        }
    }
    conn.transaction::<_, color_eyre::Report, _>(|conn| {
        for (market, market_type) in markets {
            insert_into(symbols::table)
                .values(NewSymbol {
                    name: market,
                    market_type,
                })
                .on_conflict(symbols::name)
                .do_nothing()
                .execute(conn)
                .wrap_err_with(|| format!("upserting market {market}"))?;
        }
        let rows = symbols::table
            .select(Symbol::as_select())
            .load::<Symbol>(conn)
            .wrap_err("loading symbols")?;
        Ok(rows.into_iter().map(|row| (row.name, row.id)).collect())
    })
}

pub fn write_trades(rows: &[TradeRow], conn: &mut PgConnection) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    insert_into(trades::table)
        .values(rows)
        .on_conflict_do_nothing()
        .execute(conn)
        .wrap_err("inserting trades")?;
    Ok(())
}

pub fn write_orderbooks(rows: &[OrderbookRow], conn: &mut PgConnection) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    insert_into(orderbook_messages::table)
        .values(rows)
        .execute(conn)
        .wrap_err("inserting order-book messages")?;
    Ok(())
}

pub fn write_bbo(rows: &[BboRow], conn: &mut PgConnection) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    insert_into(bbo_messages::table)
        .values(rows)
        .execute(conn)
        .wrap_err("inserting best bid/offer messages")?;
    Ok(())
}

pub fn newest_funding_time(conn: &mut PgConnection, symbol: i32) -> Result<Option<DateTime<Utc>>> {
    funding_rates::table
        .filter(funding_rates::symbol.eq(symbol))
        .select(max(funding_rates::time))
        .first(conn)
        .wrap_err("loading newest funding settlement")
}

pub fn write_funding_rates(rows: &[FundingRateRow], conn: &mut PgConnection) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    insert_into(funding_rates::table)
        .values(rows)
        .on_conflict_do_nothing()
        .execute(conn)
        .wrap_err("inserting funding rates")?;
    Ok(())
}

/// Makes `perp_spot_pairs` match `rows`. Rows update on a perp conflict. Rows for perps
/// that `rows` no longer lists are deleted.
pub fn sync_perp_spot_pairs(rows: &[PerpSpotPairRow], conn: &mut PgConnection) -> Result<()> {
    conn.transaction::<_, color_eyre::Report, _>(|conn| {
        let perp_symbols: Vec<i32> = rows.iter().map(|row| row.perp_symbol).collect();
        delete(perp_spot_pairs::table.filter(perp_spot_pairs::perp_symbol.ne_all(perp_symbols)))
            .execute(conn)
            .wrap_err("deleting perp-spot pairs that the mapping file no longer lists")?;
        if rows.is_empty() {
            return Ok(());
        }
        insert_into(perp_spot_pairs::table)
            .values(rows)
            .on_conflict(perp_spot_pairs::perp_symbol)
            .do_update()
            .set((
                perp_spot_pairs::spot_symbol.eq(excluded(perp_spot_pairs::spot_symbol)),
                perp_spot_pairs::source.eq(excluded(perp_spot_pairs::source)),
                perp_spot_pairs::rationale.eq(excluded(perp_spot_pairs::rationale)),
            ))
            .execute(conn)
            .wrap_err("upserting perp-spot pairs")?;
        Ok(())
    })
}
