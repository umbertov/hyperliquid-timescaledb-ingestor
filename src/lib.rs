pub mod models;
pub mod schema;

use color_eyre::eyre::{Result, WrapErr};
use diesel::dsl::insert_into;
use diesel::pg::PgConnection;
use diesel::prelude::*;
use diesel::r2d2::{ConnectionManager, Pool};
use models::{NewSymbol, OrderbookRow, Symbol, TradeRow};
use schema::{orderbook_messages, symbols, trades};
use std::collections::HashMap;

pub fn connection_pool(url: &str) -> Result<Pool<ConnectionManager<PgConnection>>> {
    Pool::builder()
        .max_size(8)
        .build(ConnectionManager::<PgConnection>::new(url))
        .wrap_err("creating PostgreSQL connection pool")
}

pub fn sync_symbols(markets: &[String], conn: &mut PgConnection) -> Result<HashMap<String, i32>> {
    conn.transaction::<_, color_eyre::Report, _>(|conn| {
        for market in markets {
            insert_into(symbols::table)
                .values(NewSymbol {
                    name: market,
                    market_type: "perp",
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
