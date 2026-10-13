CREATE TABLE IF NOT EXISTS bbo_messages (
  id BIGSERIAL,
  time TIMESTAMPTZ NOT NULL,
  symbol INTEGER NOT NULL REFERENCES symbols(id) ON DELETE CASCADE,
  bid_px NUMERIC,
  bid_sz NUMERIC,
  ask_px NUMERIC,
  ask_sz NUMERIC,
  PRIMARY KEY (time, id)
) WITH (
  tsdb.hypertable,
  tsdb.partition_column='time',
  timescaledb.enable_columnstore=false
);
