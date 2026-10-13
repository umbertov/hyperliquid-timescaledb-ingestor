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
