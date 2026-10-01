use bigdecimal::BigDecimal;
use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use color_eyre::eyre::{eyre, Result, WrapErr};
use diesel::{Connection, PgConnection};
use hyperliquid_rust_sdk::{BaseUrl, BookLevel, InfoClient, Message, Subscription};
use hyperliquid_timescaledb_ingestor::models::{OrderbookRow, TradeRow};
use mimalloc::MiMalloc;
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info};

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

const BATCH_CAPACITY: usize = 128;
const FLUSH_INTERVAL: Duration = Duration::from_secs(1);
const SUBSCRIPTIONS_PER_MARKET: usize = 2;
const MAX_MARKETS: usize = 500;

#[derive(Parser)]
#[command(name = "hyperliquid-timescaledb-ingestor")]
struct Args {
    #[arg(
        long,
        help = "Collect only symbols with both spot and perpetual markets"
    )]
    paired_perp_spot: bool,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand)]
enum Command {
    Export {
        #[arg(long)]
        output_dir: PathBuf,
        #[arg(long = "symbol")]
        symbols: Vec<String>,
        #[arg(long, default_value = "both", value_parser = ["both", "trades", "orderbooks"])]
        dataset: String,
    },
}

enum WriteMsg {
    Trade(TradeRow),
    Orderbook(OrderbookRow),
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    color_eyre::install()?;
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = Args::parse();
    if args.paired_perp_spot && args.command.is_some() {
        return Err(eyre!("--paired-perp-spot applies only to live collection"));
    }
    if let Some(Command::Export {
        output_dir,
        symbols,
        dataset,
    }) = args.command
    {
        let database_url = std::env::var("DATABASE_URL").wrap_err("DATABASE_URL must be set")?;
        return tokio::task::spawn_blocking(move || {
            hyperliquid_timescaledb_ingestor::export::export_parquet(
                &database_url,
                &output_dir,
                &symbols,
                &dataset,
            )
        })
        .await
        .wrap_err("export task failed")?;
    }

    let database_url = std::env::var("DATABASE_URL").wrap_err("DATABASE_URL must be set")?;
    let mut conn = PgConnection::establish(&database_url).wrap_err("connecting to PostgreSQL")?;
    let info_client = InfoClient::with_reconnect(None, Some(BaseUrl::Mainnet))
        .await
        .wrap_err("connecting to Hyperliquid")?;
    let meta = info_client
        .meta()
        .await
        .wrap_err("loading perpetual markets")?;
    if meta.universe.is_empty() {
        return Err(eyre!("Hyperliquid returned no perpetual markets"));
    }
    let spot_meta = info_client
        .spot_meta()
        .await
        .wrap_err("loading spot markets")?;
    if spot_meta.universe.is_empty() {
        return Err(eyre!("Hyperliquid returned no spot markets"));
    }
    let token_names: HashMap<usize, &str> = spot_meta
        .tokens
        .iter()
        .map(|token| (token.index, token.name.as_str()))
        .collect();
    let perpetual_markets: Vec<String> = meta
        .universe
        .into_iter()
        .map(|market| market.name)
        .collect();
    let mut spot_markets = Vec::with_capacity(spot_meta.universe.len());
    for market in spot_meta.universe {
        let base = token_names
            .get(&market.tokens[0])
            .ok_or_else(|| eyre!("spot market {} has an unknown base token", market.name))?;
        let quote = token_names
            .get(&market.tokens[1])
            .ok_or_else(|| eyre!("spot market {} has an unknown quote token", market.name))?;
        let coin = if *base == "PURR" && *quote == "USDC" {
            "PURR/USDC".to_string()
        } else {
            format!("@{}", market.index)
        };
        spot_markets.push((coin, (*base).to_string()));
    }
    let markets = if args.paired_perp_spot {
        select_paired_markets(&perpetual_markets, &spot_markets, MAX_MARKETS)?
    } else {
        let markets = perpetual_markets
            .iter()
            .cloned()
            .map(|name| (name, "perp"))
            .chain(spot_markets.iter().map(|(name, _)| (name.clone(), "spot")))
            .collect::<Vec<_>>();
        if markets.len() > MAX_MARKETS {
            return Err(eyre!(
                "all {} markets need {} subscriptions, above the per-IP limit of {}; run with --paired-perp-spot",
                markets.len(),
                markets.len() * SUBSCRIPTIONS_PER_MARKET,
                MAX_MARKETS * SUBSCRIPTIONS_PER_MARKET
            ));
        }
        markets
    };
    let market_names: Vec<String> = markets.iter().map(|(name, _)| name.clone()).collect();
    let symbol_ids = hyperliquid_timescaledb_ingestor::sync_symbols(&markets, &mut conn)?;
    info!(
        market_count = markets.len(),
        "synced perpetual and spot markets"
    );

    let pool = hyperliquid_timescaledb_ingestor::connection_pool(&database_url)?;
    let (tx, rx) = mpsc::channel(BATCH_CAPACITY * 4);
    tokio::spawn(writer_task(rx, pool));
    let (ws_tx, mut ws_rx) = mpsc::unbounded_channel();

    let mut websocket_client = InfoClient::with_reconnect(None, Some(BaseUrl::Mainnet))
        .await
        .wrap_err("connecting to Hyperliquid WebSocket")?;
    for market in &market_names {
        websocket_client
            .subscribe(
                Subscription::L2Book {
                    coin: market.clone(),
                },
                ws_tx.clone(),
            )
            .await
            .wrap_err_with(|| format!("subscribing to {market} order book"))?;
        websocket_client
            .subscribe(
                Subscription::Trades {
                    coin: market.clone(),
                },
                ws_tx.clone(),
            )
            .await
            .wrap_err_with(|| format!("subscribing to {market} trades"))?;
    }
    info!(
        market_count = market_names.len(),
        subscription_count = market_names.len() * SUBSCRIPTIONS_PER_MARKET,
        "subscribed to trades and order books"
    );

    loop {
        match ws_rx
            .recv()
            .await
            .ok_or_else(|| eyre!("Hyperliquid SDK stream closed"))?
        {
            Message::Trades(message) => {
                for trade in message.data {
                    let symbol = *symbol_ids
                        .get(&trade.coin)
                        .ok_or_else(|| eyre!("unknown trade market {}", trade.coin))?;
                    let time = millis_to_datetime(trade.time)?;
                    let hyperliquid_trade_id =
                        i64::try_from(trade.tid).wrap_err("trade ID exceeds BIGINT")?;
                    tx.send(WriteMsg::Trade(TradeRow {
                        time,
                        symbol,
                        hyperliquid_trade_id,
                        trade_hash: trade.hash,
                        side: trade.side,
                        price: trade
                            .px
                            .parse::<BigDecimal>()
                            .wrap_err("parsing trade price")?,
                        size: trade
                            .sz
                            .parse::<BigDecimal>()
                            .wrap_err("parsing trade size")?,
                    }))
                    .await
                    .map_err(|_| eyre!("database writer stopped"))?;
                }
            }
            Message::L2Book(message) => {
                let symbol = *symbol_ids
                    .get(&message.data.coin)
                    .ok_or_else(|| eyre!("unknown order-book market {}", message.data.coin))?;
                let mut levels = message.data.levels.into_iter();
                let bids = levels
                    .next()
                    .ok_or_else(|| eyre!("order book has no bid levels"))?;
                let asks = levels
                    .next()
                    .ok_or_else(|| eyre!("order book has no ask levels"))?;
                if levels.next().is_some() {
                    return Err(eyre!("order book has unexpected level groups"));
                }
                let to_json = |levels: Vec<BookLevel>| {
                    levels.into_iter().map(|level| serde_json::json!({"price": level.px, "size": level.sz, "count": level.n})).collect::<Vec<_>>()
                };
                tx.send(WriteMsg::Orderbook(OrderbookRow {
                    time: millis_to_datetime(message.data.time)?,
                    symbol,
                    bids: serde_json::Value::Array(to_json(bids)),
                    asks: serde_json::Value::Array(to_json(asks)),
                }))
                .await
                .map_err(|_| eyre!("database writer stopped"))?;
            }
            Message::HyperliquidError(message) => {
                if message.starts_with("Reader error:")
                    || message.starts_with("Reader text conversion error:")
                {
                    tracing::warn!("WebSocket reader error; SDK will reconnect: {message}");
                    continue;
                }
                return Err(eyre!("Hyperliquid error: {message}"));
            }
            Message::NoData | Message::SubscriptionResponse => {}
            other => return Err(eyre!("unexpected Hyperliquid message: {other:?}")),
        }
    }
}

fn select_paired_markets(
    perpetual_markets: &[String],
    spot_markets: &[(String, String)],
    limit: usize,
) -> Result<Vec<(String, &'static str)>> {
    let spot_bases: HashSet<&str> = spot_markets
        .iter()
        .map(|(_, base)| perpetual_symbol_for_spot_base(base))
        .collect();
    let paired_perpetuals: Vec<&String> = perpetual_markets
        .iter()
        .filter(|name| spot_bases.contains(name.as_str()))
        .collect();
    if paired_perpetuals.is_empty() {
        return Err(eyre!(
            "Hyperliquid returned no symbols with both spot and perpetual markets"
        ));
    }
    let paired_bases: HashSet<&str> = paired_perpetuals.iter().map(|name| name.as_str()).collect();
    let mut selected = Vec::with_capacity(paired_perpetuals.len() * 2);
    let mut selected_names = HashSet::with_capacity(limit);

    for name in &paired_perpetuals {
        push_market(&mut selected, &mut selected_names, (*name).clone(), "perp");
    }
    for (name, base) in spot_markets {
        if paired_bases.contains(perpetual_symbol_for_spot_base(base)) {
            push_market(&mut selected, &mut selected_names, name.clone(), "spot");
        }
    }
    if selected.len() > limit {
        return Err(eyre!(
            "{} paired spot and perpetual markets need {} subscriptions, above the limit of {}",
            selected.len(),
            selected.len() * SUBSCRIPTIONS_PER_MARKET,
            limit * SUBSCRIPTIONS_PER_MARKET
        ));
    }
    let paired_perpetual_count = paired_perpetuals.len();
    let perpetual_count = selected.iter().filter(|(_, kind)| *kind == "perp").count();
    let spot_count = selected.len() - perpetual_count;
    info!(
        market_count = selected.len(),
        paired_perpetual_count,
        perpetual_count,
        spot_count,
        subscription_count = selected.len() * SUBSCRIPTIONS_PER_MARKET,
        "selected markets with both spot and perpetual data"
    );
    Ok(selected)
}

fn perpetual_symbol_for_spot_base(base: &str) -> &str {
    match base {
        "UBTC" => "BTC",
        "UETH" => "ETH",
        "USOL" => "SOL",
        "UZEC" => "ZEC",
        "ONEAR" => "NEAR",
        "UENA" => "ENA",
        "UXPL" => "XPL",
        "HPENGU" => "PENGU",
        "UAVAX" => "AVAX",
        "HSEI" => "SEI",
        "UFART" => "FARTCOIN",
        "UUUSPX" => "SPX",
        "UVIRT" => "VIRTUAL",
        _ => base,
    }
}

fn push_market(
    selected: &mut Vec<(String, &'static str)>,
    selected_names: &mut HashSet<String>,
    name: String,
    kind: &'static str,
) {
    if selected_names.insert(name.clone()) {
        selected.push((name, kind));
    }
}

async fn writer_task(
    mut rx: mpsc::Receiver<WriteMsg>,
    pool: diesel::r2d2::Pool<diesel::r2d2::ConnectionManager<PgConnection>>,
) {
    let mut ticker = tokio::time::interval(FLUSH_INTERVAL);
    ticker.tick().await;
    let mut batch = Vec::with_capacity(BATCH_CAPACITY);
    loop {
        tokio::select! {
            message = rx.recv() => match message {
                Some(message) => {
                    batch.push(message);
                    if batch.len() >= BATCH_CAPACITY { flush(&mut batch, &pool).await; }
                }
                None => { flush(&mut batch, &pool).await; return; }
            },
            _ = ticker.tick() => flush(&mut batch, &pool).await,
        }
    }
}

async fn flush(
    batch: &mut Vec<WriteMsg>,
    pool: &diesel::r2d2::Pool<diesel::r2d2::ConnectionManager<PgConnection>>,
) {
    if batch.is_empty() {
        return;
    }
    let drained = std::mem::take(batch);
    let (mut trades, mut orderbooks) = (Vec::new(), Vec::new());
    for message in drained {
        match message {
            WriteMsg::Trade(row) => trades.push(row),
            WriteMsg::Orderbook(row) => orderbooks.push(row),
        }
    }
    let pool = pool.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<()> {
        let mut conn = pool
            .get()
            .wrap_err("getting a pooled database connection")?;
        conn.transaction::<_, color_eyre::Report, _>(|conn| {
            hyperliquid_timescaledb_ingestor::write_trades(&trades, conn)?;
            hyperliquid_timescaledb_ingestor::write_orderbooks(&orderbooks, conn)?;
            Ok(())
        })
    })
    .await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            error!("database batch write failed: {error}");
            std::process::exit(1);
        }
        Err(error) => {
            error!("database writer task failed: {error}");
            std::process::exit(1);
        }
    }
}

fn millis_to_datetime(millis: u64) -> Result<DateTime<Utc>> {
    let millis = i64::try_from(millis).wrap_err("timestamp exceeds signed range")?;
    DateTime::from_timestamp_millis(millis).ok_or_else(|| eyre!("invalid timestamp: {millis}"))
}
