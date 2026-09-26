use crate::schema::{orderbook_messages, symbols, trades};
use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use diesel::prelude::*;

#[derive(Queryable, Selectable, Debug)]
#[diesel(table_name = symbols)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Symbol {
    pub id: i32,
    pub name: String,
    pub market_type: String,
}

#[derive(Insertable)]
#[diesel(table_name = symbols)]
pub struct NewSymbol<'a> {
    pub name: &'a str,
    pub market_type: &'a str,
}

#[derive(Insertable)]
#[diesel(table_name = trades)]
pub struct TradeRow {
    pub time: DateTime<Utc>,
    pub symbol: i32,
    pub hyperliquid_trade_id: i64,
    pub trade_hash: String,
    pub side: String,
    pub price: BigDecimal,
    pub size: BigDecimal,
}

#[derive(Insertable)]
#[diesel(table_name = orderbook_messages)]
pub struct OrderbookRow {
    pub time: DateTime<Utc>,
    pub symbol: i32,
    pub bids: serde_json::Value,
    pub asks: serde_json::Value,
}
