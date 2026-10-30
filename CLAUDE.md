# Project guidance

Read `README.md` before you change the project. Maintain `README.md` and `CLAUDE.md` after changes.

## Data model

- Store exchange market names in `symbols`.
- Store trades, order-book messages, and bbo (best bid/offer) messages in TimescaleDB hypertables.
- Store bid and ask levels as JSONB arrays.
- Store bbo bid and ask price and size as nullable `NUMERIC` columns, since either side of a bbo update can be `null`.
- Use exchange trade IDs and timestamps to deduplicate trade events.
- Declare trade prices and sizes as `NUMERIC`.
- Keep hypertable options minimal and disable columnstore storage.
- Do not add retention policies or read optimization indexes without a measured need.
- Use `@index` for spot symbols, except `PURR/USDC`.
- Export trades and order-book messages as Parquet in bounded batches.
- Keep exported prices and sizes as decimal text to preserve exact values.
- Fail if an export output file already exists.
- Store settled funding rates in the `funding_rates` hypertable, one row per symbol and settlement time. Store `fundingRate` and `premium` as `NUMERIC`.
- Deduplicate funding rates on (time, symbol). Insert them with `ON CONFLICT DO NOTHING`.
- Never write a predicted funding rate into `funding_rates`.

## Funding collection

- Poll `fundingHistory` one request at a time, with at least one second between requests. The info endpoint of the REST (HTTP request) API allows 1,200 weight per minute per IP.
- Start each market at one millisecond after its newest stored settlement, or at `--funding-backfill-start` when none is stored.
- Require `--funding-poll-interval-secs` and `--funding-backfill-start` for live collection. Do not add defaults.
- Fail on a response row for another coin, a row before the requested start, or a decimal field that is not an exact number.

## Builder-dex collection

- Require `--perp-dexes` for live collection. It takes a comma-separated list of HIP-3 builder perp dexes, such as `xyz`. An empty value selects none. Do not add a default.
- `hyperliquid_rust_sdk` 0.6.0 sends `meta` without `dex`. Load `perpDexs` and `meta` with `dex` over HTTP in `src/dexes.rs`.
- Fail at startup if Hyperliquid does not list a requested dex, or if a market name lacks the `<dex>:` prefix.
- Store builder-dex market names as Hyperliquid names them, such as `xyz:NVDA`. Use the same name for `trades`, `l2Book`, `bbo` and `fundingHistory`.
- Skip delisted builder-dex markets.
- Do not pair a builder-dex perp with a spot market by name. No info API field links them, and several spot tokens share one ticker. Collect a perp without a mapping entry alone and log that no spot counterpart is verified.
- Take builder-dex spot counterparts only from `config/xyz-spot-map.toml`, passed with `--xyz-spot-map`. Keep its header selection rule and snapshot date. Do not add a default path.
- Pick each spot pair by hand from a live snapshot, by the highest 24 h volume among named candidates. Never regenerate the mapping at startup.
- Require `--xyz-spot-map` when `--perp-dexes` includes `xyz`. Reject it without `xyz`.
- Check each mapped perp against the live `xyz` meta and each mapped spot pair against `spotMeta`. Fail at startup on a mismatch, including a spot base token that differs from `spot_token`.
- Subscribe mapped spot pairs to trades, `l2Book` and, with `--collect-bbo`, bbo. Store them with their `@index` names.
- Store the mapping in `perp_spot_pairs` with source `mapping-file`. Make the table match the file at each start.
- Add builder-dex perps and mapped spot pairs after the `--paired-perp-spot` selection. Count them in the subscription limit.

## bbo collection

- Collect the `bbo` channel only when `--collect-bbo` is set. The default is off, so the flag changes no existing deployment.
- The Hyperliquid Rust SDK (`hyperliquid_rust_sdk` 0.6.0) has a closed `Subscription` enum with no `bbo` variant and no raw-message passthrough. Collect `bbo` over a separate raw WebSocket connection in `src/bbo.rs`, built on `tokio-tungstenite` (already in the dependency tree through the SDK).
- Subscribe with `{"method":"subscribe","subscription":{"type":"bbo","coin":<coin>}}` for the same market list the `l2Book`/`trades` connection uses.
- Send `{"method":"ping"}` before the connection's 60-second idle timeout and expect `{"channel":"pong"}` back.
- Reconnect and resubscribe on every disconnect. Log each reconnect. Treat 60 seconds without any message as a disconnect. Wait 1 second before the first reconnect, double the wait up to 60 seconds, and reset it after a connection that lasted 60 seconds. Treat a message that does not match the documented `bbo` shape as a fatal error: log the raw message and stop, never skip it silently.
- Count each enabled `bbo` subscription against the same per-IP 1,000-subscription limit as `l2Book` and `trades`, even though it runs on its own connection. Fail at startup if the total would exceed that limit.

## Migrations

- Make each `up.sql` safe to run twice.
- Apply migrations in timestamp order.
- Inspect the schema before you repair migration bookkeeping.

## Development

- Copy `.env.example` to `.env` before deployment.
- Apply migrations before you start the ingestor.
- Set `XYZ_SPOT_MAP_FILE` in `.env`. Compose sets the container variable `XYZ_SPOT_MAP` only when `PERP_DEXES` is non-empty.
- Use `nix run .#compose -- down` to stop the Compose service.
- Build the Compose ingestor image from `Dockerfile` with `docker compose up --build`.
- Use host networking to connect to a database published on `127.0.0.1:5433` on the Docker host.
- Keep `.env` local. Do not commit it.
- Do not log database connection strings.
- Use fixtures for automated tests that need market data.
- Do not call the live API repeatedly from a test suite.
- Keep WebSocket subscriptions below the 1,000 subscription limit per IP.
- With `--paired-perp-spot`, select only symbols with both spot and perpetual markets.
- Use `--paired-perp-spot` to enable paired market selection.

## CI

- Keep formatting, Clippy, workspace tests, and the Docker image build in CI.
- Limit the Nix package source to Rust build inputs.
- Build the CI image with `.#docker` and the Nix cache. Do not use `Dockerfile` in CI.
- Publish checked images to GitHub Container Registry from `main` and version tags.
