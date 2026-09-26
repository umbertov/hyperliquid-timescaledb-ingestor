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
  price DOUBLE PRECISION NOT NULL,
  size DOUBLE PRECISION NOT NULL,
  PRIMARY KEY (time, id)
) WITH (
  tsdb.hypertable,
  tsdb.partition_column='time',
  tsdb.segmentby='symbol',
  tsdb.orderby='time',
  tsdb.chunk_interval='1 day'
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
  tsdb.segmentby='symbol',
  tsdb.orderby='time',
  tsdb.chunk_interval='1 day'
);

CREATE INDEX IF NOT EXISTS orderbook_messages_symbol_time_idx
  ON orderbook_messages (symbol, time DESC);
CREATE INDEX IF NOT EXISTS orderbook_messages_time_brin
  ON orderbook_messages USING BRIN(time);

SELECT add_retention_policy(
  'orderbook_messages',
  drop_after => INTERVAL '4 days',
  if_not_exists => TRUE
);
