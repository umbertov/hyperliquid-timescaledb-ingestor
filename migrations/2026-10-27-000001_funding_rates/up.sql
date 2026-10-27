CREATE TABLE IF NOT EXISTS funding_rates (
  id BIGSERIAL,
  time TIMESTAMPTZ NOT NULL,
  symbol INTEGER NOT NULL REFERENCES symbols(id) ON DELETE CASCADE,
  funding_rate NUMERIC NOT NULL,
  premium NUMERIC NOT NULL,
  PRIMARY KEY (time, id)
) WITH (
  tsdb.hypertable,
  tsdb.partition_column='time',
  timescaledb.enable_columnstore=false
);

CREATE UNIQUE INDEX IF NOT EXISTS funding_rates_dedup_uniq
  ON funding_rates (time, symbol);
