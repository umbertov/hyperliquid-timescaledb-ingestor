//! Raw WebSocket collection of Hyperliquid's `bbo` (best bid/offer) channel.
//!
//! `hyperliquid_rust_sdk` 0.6.0 exposes a closed `Subscription` enum with no `bbo`
//! variant and no raw-message passthrough, so this module speaks the documented
//! Hyperliquid WebSocket protocol directly over `tokio-tungstenite` (already present
//! in the dependency tree via `hyperliquid_rust_sdk` itself). `bbo` updates far more
//! frequently than `l2Book` and is cheaper to store, which is the reason to collect it
//! at all: `l2Book` arrives roughly once every 5.4 seconds per market, while `bbo`
//! arrives many times a second on liquid markets.
//!
//! A dropped connection is normal for a long-lived feed and is retried with a log
//! line. A message that does not match the documented shape is not normal: it is
//! logged with its raw text and propagated as a fatal error, never silently skipped.

use crate::millis_to_datetime;
use crate::models::BboRow;
use bigdecimal::BigDecimal;
use color_eyre::eyre::{eyre, Result, WrapErr};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message as WsMessage;
use tracing::{info, warn};

const HYPERLIQUID_WS_URL: &str = "wss://api.hyperliquid.xyz/ws";
const PING_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Debug, Deserialize, PartialEq)]
pub struct WsLevel {
    pub px: String,
    pub sz: String,
    #[allow(dead_code)]
    pub n: u64,
}

#[derive(Debug, Deserialize, PartialEq)]
pub struct WsBboData {
    pub coin: String,
    pub time: u64,
    pub bbo: [Option<WsLevel>; 2],
}

/// Parses one raw WebSocket text frame from the `bbo` connection.
///
/// Returns `Ok(None)` for an expected non-data control message (a subscription
/// acknowledgement or a pong), `Ok(Some(..))` for a `bbo` data message, and `Err` for
/// anything else, including a message whose shape does not match the documented
/// `bbo` schema. The raw text is included in every error so a malformed message is
/// never lost.
pub fn parse_bbo_envelope(text: &str) -> Result<Option<WsBboData>> {
    let value: serde_json::Value = serde_json::from_str(text)
        .wrap_err_with(|| format!("parsing bbo WebSocket message: {text}"))?;
    let channel = value
        .get("channel")
        .and_then(|channel| channel.as_str())
        .ok_or_else(|| eyre!("bbo WebSocket message has no channel field: {text}"))?;
    match channel {
        "subscriptionResponse" | "pong" => Ok(None),
        "bbo" => {
            let data = value
                .get("data")
                .ok_or_else(|| eyre!("bbo message has no data field: {text}"))?;
            let parsed: WsBboData = serde_json::from_value(data.clone())
                .wrap_err_with(|| format!("parsing bbo data: {text}"))?;
            Ok(Some(parsed))
        }
        other => Err(eyre!("unexpected bbo WebSocket channel {other:?}: {text}")),
    }
}

fn parse_decimal(value: &str, field: &str) -> Result<BigDecimal> {
    value
        .parse::<BigDecimal>()
        .wrap_err_with(|| format!("parsing bbo {field} {value:?}"))
}

/// Converts a parsed `bbo` message into the row to store, resolving its market to the
/// `symbols` table ID already synced at startup.
pub fn bbo_row(data: WsBboData, symbol: i32) -> Result<BboRow> {
    let time = millis_to_datetime(data.time)?;
    let [bid, ask] = data.bbo;
    let bid_px = bid
        .as_ref()
        .map(|level| parse_decimal(&level.px, "bid price"))
        .transpose()?;
    let bid_sz = bid
        .as_ref()
        .map(|level| parse_decimal(&level.sz, "bid size"))
        .transpose()?;
    let ask_px = ask
        .as_ref()
        .map(|level| parse_decimal(&level.px, "ask price"))
        .transpose()?;
    let ask_sz = ask
        .as_ref()
        .map(|level| parse_decimal(&level.sz, "ask size"))
        .transpose()?;
    Ok(BboRow {
        time,
        symbol,
        bid_px,
        bid_sz,
        ask_px,
        ask_sz,
    })
}

/// Why a single connection attempt ended.
enum RunOnceError {
    /// The connection dropped or a transport-level error occurred. Normal for a
    /// long-lived feed; the caller reconnects.
    Disconnected(color_eyre::Report),
    /// A message did not match the documented `bbo` schema, an unknown market was
    /// named, or the downstream writer stopped. Fatal; the caller must not retry.
    Fatal(color_eyre::Report),
}

/// Runs the `bbo` WebSocket collector for `markets` until a fatal error occurs,
/// reconnecting and resubscribing on every disconnect.
pub async fn run(
    markets: Vec<String>,
    symbol_ids: HashMap<String, i32>,
    tx: mpsc::Sender<BboRow>,
) -> Result<()> {
    loop {
        match run_once(&markets, &symbol_ids, &tx).await {
            Ok(never) => match never {},
            Err(RunOnceError::Disconnected(error)) => {
                warn!("bbo WebSocket disconnected, reconnecting: {error}");
            }
            Err(RunOnceError::Fatal(error)) => return Err(error),
        }
    }
}

async fn run_once(
    markets: &[String],
    symbol_ids: &HashMap<String, i32>,
    tx: &mpsc::Sender<BboRow>,
) -> std::result::Result<std::convert::Infallible, RunOnceError> {
    let (ws_stream, _response) = tokio_tungstenite::connect_async(HYPERLIQUID_WS_URL)
        .await
        .map_err(|error| RunOnceError::Disconnected(eyre!(error)))?;
    let (mut sink, mut stream) = ws_stream.split();

    for market in markets {
        let subscribe = serde_json::json!({
            "method": "subscribe",
            "subscription": {"type": "bbo", "coin": market},
        });
        sink.send(WsMessage::Text(subscribe.to_string()))
            .await
            .map_err(|error| RunOnceError::Disconnected(eyre!(error)))?;
    }
    info!(market_count = markets.len(), "subscribed to bbo channel");

    let mut ping_ticker = tokio::time::interval(PING_INTERVAL);
    ping_ticker.tick().await;

    loop {
        tokio::select! {
            _ = ping_ticker.tick() => {
                sink.send(WsMessage::Text(serde_json::json!({"method": "ping"}).to_string()))
                    .await
                    .map_err(|error| RunOnceError::Disconnected(eyre!(error)))?;
            }
            message = stream.next() => {
                let message = message
                    .ok_or_else(|| RunOnceError::Disconnected(eyre!("bbo WebSocket stream closed")))?
                    .map_err(|error| RunOnceError::Disconnected(eyre!(error)))?;
                match message {
                    WsMessage::Text(text) => {
                        let data = parse_bbo_envelope(&text).map_err(RunOnceError::Fatal)?;
                        let Some(data) = data else { continue };
                        let symbol = *symbol_ids
                            .get(&data.coin)
                            .ok_or_else(|| RunOnceError::Fatal(eyre!("unknown bbo market {}", data.coin)))?;
                        let row = bbo_row(data, symbol).map_err(RunOnceError::Fatal)?;
                        tx.send(row)
                            .await
                            .map_err(|_| RunOnceError::Fatal(eyre!("database writer stopped")))?;
                    }
                    WsMessage::Ping(payload) => {
                        sink.send(WsMessage::Pong(payload))
                            .await
                            .map_err(|error| RunOnceError::Disconnected(eyre!(error)))?;
                    }
                    WsMessage::Pong(_) => {}
                    WsMessage::Close(frame) => {
                        return Err(RunOnceError::Disconnected(eyre!(
                            "bbo WebSocket closed: {frame:?}"
                        )));
                    }
                    other => {
                        return Err(RunOnceError::Fatal(eyre!(
                            "unexpected bbo WebSocket frame: {other:?}"
                        )));
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_bbo_message_with_both_sides() {
        let text = r#"{"channel":"bbo","data":{"coin":"BTC","time":1700000000000,"bbo":[{"px":"50000.1","sz":"1.5","n":3},{"px":"50000.2","sz":"2.0","n":4}]}}"#;
        let data = parse_bbo_envelope(text)
            .expect("message parses")
            .expect("message carries bbo data");
        assert_eq!(data.coin, "BTC");
        assert_eq!(data.time, 1700000000000);
        let bid = data.bbo[0].as_ref().expect("bid present");
        assert_eq!(bid.px, "50000.1");
        assert_eq!(bid.sz, "1.5");
        assert_eq!(bid.n, 3);
        let ask = data.bbo[1].as_ref().expect("ask present");
        assert_eq!(ask.px, "50000.2");
        assert_eq!(ask.sz, "2.0");

        let row = bbo_row(data, 7).expect("row builds");
        assert_eq!(row.symbol, 7);
        assert_eq!(row.bid_px, Some("50000.1".parse().unwrap()));
        assert_eq!(row.bid_sz, Some("1.5".parse().unwrap()));
        assert_eq!(row.ask_px, Some("50000.2".parse().unwrap()));
        assert_eq!(row.ask_sz, Some("2.0".parse().unwrap()));
    }

    #[test]
    fn parses_bbo_message_with_one_side_missing() {
        let text = r#"{"channel":"bbo","data":{"coin":"@107","time":1700000000123,"bbo":[null,{"px":"1.2345","sz":"100","n":1}]}}"#;
        let data = parse_bbo_envelope(text)
            .expect("message parses")
            .expect("message carries bbo data");
        assert!(data.bbo[0].is_none());
        assert!(data.bbo[1].is_some());

        let row = bbo_row(data, 42).expect("row builds");
        assert_eq!(row.bid_px, None);
        assert_eq!(row.bid_sz, None);
        assert_eq!(row.ask_px, Some("1.2345".parse().unwrap()));
        assert_eq!(row.ask_sz, Some("100".parse().unwrap()));
    }

    #[test]
    fn subscription_response_and_pong_are_not_data_messages() {
        let subscription_response = r#"{"channel":"subscriptionResponse","data":{"method":"subscribe","subscription":{"type":"bbo","coin":"BTC"}}}"#;
        assert!(parse_bbo_envelope(subscription_response)
            .expect("message parses")
            .is_none());

        let pong = r#"{"channel":"pong"}"#;
        assert!(parse_bbo_envelope(pong).expect("message parses").is_none());
    }

    #[test]
    fn malformed_message_is_an_error_not_a_skip() {
        let missing_channel = r#"{"data":{"coin":"BTC"}}"#;
        assert!(parse_bbo_envelope(missing_channel).is_err());

        let wrong_shape =
            r#"{"channel":"bbo","data":{"coin":"BTC","time":1700000000000,"bbo":"not-an-array"}}"#;
        assert!(parse_bbo_envelope(wrong_shape).is_err());

        let not_json = "this is not json";
        assert!(parse_bbo_envelope(not_json).is_err());

        let unexpected_channel = r#"{"channel":"trades","data":[]}"#;
        assert!(parse_bbo_envelope(unexpected_channel).is_err());
    }
}
