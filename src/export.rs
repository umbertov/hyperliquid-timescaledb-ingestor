use arrow_array::builder::StringBuilder;
use arrow_array::{ArrayRef, RecordBatch, TimestampMicrosecondArray};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use color_eyre::eyre::{eyre, Result, WrapErr};
use fallible_iterator::FallibleIterator;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use postgres::{Client, NoTls};
use std::fs::{self, OpenOptions};
use std::path::Path;
use std::sync::Arc;

const BATCH_SIZE: usize = 8192;

pub fn export_parquet(url: &str, output: &Path, symbols: &[String], dataset: &str) -> Result<()> {
    if !matches!(dataset, "both" | "trades" | "orderbooks") {
        return Err(eyre!("dataset must be both, trades, or orderbooks"));
    }
    let mut client = Client::connect(url, NoTls).wrap_err("connecting to PostgreSQL")?;
    let filter = symbol_filter(&mut client, symbols)?;
    fs::create_dir_all(output).wrap_err("creating export directory")?;
    let paths = [
        (dataset != "orderbooks").then(|| output.join("trades.parquet")),
        (dataset != "trades").then(|| output.join("orderbook_messages.parquet")),
    ];
    for path in paths.iter().flatten() {
        if path.exists() {
            return Err(eyre!("output file already exists: {}", path.display()));
        }
    }
    if dataset != "orderbooks" {
        export_trades(&mut client, &output.join("trades.parquet"), &filter)?;
    }
    if dataset != "trades" {
        export_books(
            &mut client,
            &output.join("orderbook_messages.parquet"),
            &filter,
        )?;
    }
    Ok(())
}

fn symbol_filter(client: &mut Client, symbols: &[String]) -> Result<String> {
    if symbols.is_empty() {
        return Ok(String::new());
    }
    let rows = client.query("SELECT name FROM symbols WHERE name = ANY($1)", &[&symbols])?;
    let known: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    let unknown: Vec<_> = symbols
        .iter()
        .filter(|name| !known.contains(name))
        .collect();
    if !unknown.is_empty() {
        return Err(eyre!(
            "unknown symbol name(s): {}",
            unknown
                .iter()
                .map(|s| s.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    Ok(format!(
        " WHERE s.name IN ({})",
        symbols
            .iter()
            .map(|s| format!("'{}'", s.replace('\'', "''")))
            .collect::<Vec<_>>()
            .join(",")
    ))
}

fn writer(path: &Path, schema: Arc<Schema>) -> Result<ArrowWriter<std::fs::File>> {
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .wrap_err_with(|| format!("creating {}", path.display()))?;
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_max_row_group_size(BATCH_SIZE)
        .build();
    ArrowWriter::try_new(file, schema, Some(props)).wrap_err("creating Parquet writer")
}

fn schema(fields: Vec<Field>) -> Arc<Schema> {
    Arc::new(Schema::new(fields))
}
fn time_field() -> Field {
    Field::new(
        "time",
        DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        false,
    )
}
fn text(name: &str) -> Field {
    Field::new(name, DataType::Utf8, false)
}
fn batch(
    schema: &Arc<Schema>,
    times: &mut Vec<i64>,
    strings: &mut [StringBuilder],
) -> Result<RecordBatch> {
    let mut cols: Vec<ArrayRef> = vec![Arc::new(
        TimestampMicrosecondArray::from(std::mem::take(times)).with_timezone("UTC"),
    )];
    cols.extend(strings.iter_mut().map(|b| Arc::new(b.finish()) as ArrayRef));
    RecordBatch::try_new(schema.clone(), cols).wrap_err("building Parquet batch")
}
fn write_batch(
    w: &mut ArrowWriter<std::fs::File>,
    schema: &Arc<Schema>,
    times: &mut Vec<i64>,
    strings: &mut [StringBuilder],
) -> Result<()> {
    w.write(&batch(schema, times, strings)?)
        .wrap_err("writing Parquet batch")
}
fn export_trades(client: &mut Client, path: &Path, filter: &str) -> Result<()> {
    let sc = schema(vec![
        time_field(),
        text("symbol"),
        text("market_type"),
        text("hyperliquid_trade_id"),
        text("trade_hash"),
        text("side"),
        text("price"),
        text("size"),
    ]);
    let mut w = writer(path, sc.clone())?;
    let sql = format!("SELECT t.time,s.name,s.market_type,t.hyperliquid_trade_id,t.trade_hash,t.side,t.price::text,t.size::text FROM trades t JOIN symbols s ON s.id=t.symbol{filter} ORDER BY t.time,t.id");
    let mut rows = client.query_raw(&sql, std::iter::empty::<&str>())?;
    let mut times = Vec::with_capacity(BATCH_SIZE);
    let mut cols: Vec<StringBuilder> = (0..7).map(|_| StringBuilder::new()).collect();
    while let Some(r) = rows.next()? {
        times.push(
            r.get::<_, chrono::DateTime<chrono::Utc>>(0)
                .timestamp_micros(),
        );
        cols[0].append_value(r.get::<_, String>(1));
        cols[1].append_value(r.get::<_, String>(2));
        cols[2].append_value(r.get::<_, i64>(3).to_string());
        cols[3].append_value(r.get::<_, String>(4));
        cols[4].append_value(r.get::<_, String>(5));
        cols[5].append_value(r.get::<_, String>(6));
        cols[6].append_value(r.get::<_, String>(7));
        if times.len() == BATCH_SIZE {
            write_batch(&mut w, &sc, &mut times, &mut cols)?;
        }
    }
    if !times.is_empty() {
        write_batch(&mut w, &sc, &mut times, &mut cols)?;
    }
    w.close().wrap_err("closing trades Parquet file")?;
    Ok(())
}
fn export_books(client: &mut Client, path: &Path, filter: &str) -> Result<()> {
    let sc = schema(vec![
        time_field(),
        text("symbol"),
        text("market_type"),
        text("bids"),
        text("asks"),
    ]);
    let mut w = writer(path, sc.clone())?;
    let sql=format!("SELECT o.time,s.name,s.market_type,o.bids,o.asks FROM orderbook_messages o JOIN symbols s ON s.id=o.symbol{filter} ORDER BY o.time,o.id");
    let mut rows = client.query_raw(&sql, std::iter::empty::<&str>())?;
    let mut times = Vec::with_capacity(BATCH_SIZE);
    let mut cols: Vec<StringBuilder> = (0..4).map(|_| StringBuilder::new()).collect();
    while let Some(r) = rows.next()? {
        times.push(
            r.get::<_, chrono::DateTime<chrono::Utc>>(0)
                .timestamp_micros(),
        );
        cols[0].append_value(r.get::<_, String>(1));
        cols[1].append_value(r.get::<_, String>(2));
        cols[2].append_value(serde_json::to_string(&r.get::<_, serde_json::Value>(3))?);
        cols[3].append_value(serde_json::to_string(&r.get::<_, serde_json::Value>(4))?);
        if times.len() == BATCH_SIZE {
            write_batch(&mut w, &sc, &mut times, &mut cols)?;
        }
    }
    if !times.is_empty() {
        write_batch(&mut w, &sc, &mut times, &mut cols)?;
    }
    w.close().wrap_err("closing order-book Parquet file")?;
    Ok(())
}
