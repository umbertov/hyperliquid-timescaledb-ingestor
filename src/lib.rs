pub mod bbo;
pub mod export;
pub mod models;
pub mod schema;

use chrono::{DateTime, Utc};
use color_eyre::eyre::{eyre, Result, WrapErr};
use diesel::dsl::insert_into;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use models::{BboRow, NewSymbol, OrderbookRow, Symbol, TradeRow};
use schema::{bbo_messages, orderbook_messages, symbols, trades};
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
