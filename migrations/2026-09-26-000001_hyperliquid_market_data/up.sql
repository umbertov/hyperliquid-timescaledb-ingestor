CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS symbols (
  id SERIAL PRIMARY KEY,
  name TEXT NOT NULL UNIQUE,
  market_type TEXT NOT NULL CHECK (market_type IN ('perp', 'spot'))
);

CREATE TABLE IF NOT EXISTS trades (
  id BIGSERIAL,
  time TIMESTAMPTZ NOT NULL,
  symbol INTEGER NOT NULL REFERENCES symbols(id) ON DELETE CASCADE,
  hyperliquid_trade_id BIGINT NOT NULL,
  trade_hash TEXT NOT NULL,
  side TEXT NOT NULL CHECK (side IN ('B', 'A')),
  price NUMERIC NOT NULL,
  size NUMERIC NOT NULL,
  PRIMARY KEY (time, id)
) WITH (
  tsdb.hypertable,
  tsdb.partition_column='time',
  timescaledb.enable_columnstore=false
);

CREATE UNIQUE INDEX IF NOT EXISTS trades_dedup_uniq
  ON trades (time, symbol, hyperliquid_trade_id);

CREATE TABLE IF NOT EXISTS orderbook_messages (
  id BIGSERIAL,
  time TIMESTAMPTZ NOT NULL,
  symbol INTEGER NOT NULL REFERENCES symbols(id) ON DELETE CASCADE,
  bids JSONB NOT NULL,
  asks JSONB NOT NULL,
  PRIMARY KEY (time, id)
) WITH (
  tsdb.hypertable,
  tsdb.partition_column='time',
  timescaledb.enable_columnstore=false
);
