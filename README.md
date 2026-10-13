# hyperliquid-timescaledb-ingestor

This Rust service stores Hyperliquid market data in TimescaleDB. It syncs spot and perpetual markets, then stores live trades and level-two order-book snapshots.

## Setup

Copy the sample settings file and edit `DATABASE_URL` if the database address differs:

```sh
cp .env.example .env
```

Apply each `migrations/*/up.sql` file in timestamp order. Start paired collection:

```sh
nix run .#up
```

The migrations create time-partitioned hypertables and disable columnstore storage. The trade unique index enforces event deduplication. They add no retention policy or read optimization index.

The ingestor connects to the database in `DATABASE_URL`. The database must run PostgreSQL with TimescaleDB. Docker must run for the Compose deployment.

Compose builds the ingestor image from `Dockerfile`. The ingestor uses host networking to reach a database that Docker publishes on the host loopback address. The sample connects to `127.0.0.1:5433`.

Run `nix run .#compose -- logs -f ingestor` to read service logs. Run `nix run .#compose -- down` to stop the service.
Set `INGESTOR_IMAGE` to set the local image name. Compose builds that image when it starts the service.

## Data

The `symbols` table stores spot and perpetual market names. The `trades` table stores public trade events. The `orderbook_messages` table stores full bid and ask levels as JSONB arrays. The `bbo_messages` table stores best bid and offer updates.
Spot names use `@index` values. The `PURR/USDC` market uses that pair name.

The ingestor uses Hyperliquid's public Rust SDK for market metadata and WebSocket subscriptions. Direct runs collect all markets by default. Hyperliquid limits each IP to 1,000 WebSocket subscriptions. Each market uses two subscriptions. Startup fails if all markets exceed that limit.

Pass `--paired-perp-spot` to collect only symbols with both spot and perpetual markets. The ingestor subscribes to trades and order books for both markets. It recognizes known spot token name remappings, such as `UBTC` to `BTC`.

Run paired collection directly from the project root:

```sh
nix run .#default -- --paired-perp-spot
```

Compose passes `--paired-perp-spot` to the ingestor by default. Run `nix run .#compose -- down` to stop the service.

### bbo collection

Pass `--collect-bbo` to also collect the `bbo` (best bid/offer) channel. This flag is off by default, so a deployment that does not pass it keeps today's behavior.

The `l2Book` channel updates roughly once every 5.4 seconds per market. The `bbo` channel updates far more often on a liquid market. The Hyperliquid Rust SDK has no `bbo` subscription, so the ingestor opens a second WebSocket connection and speaks the Hyperliquid `bbo` protocol directly. This connection reconnects and resubscribes on every disconnect, and logs each reconnect. A connection that sends nothing for 60 seconds, not even a pong to the 30-second ping, counts as dead and reconnects. The wait before a reconnect starts at 1 second and doubles up to 60 seconds. It goes back to 1 second after a connection that lasted at least 60 seconds. A message that does not match the documented `bbo` shape is a fatal error, not a skipped message.

Each enabled `bbo` subscription counts toward the same 1,000-subscription-per-IP limit as `l2Book` and `trades`, even though it uses a separate connection. Startup fails if `--collect-bbo` would push the total subscription count over that limit.

```sh
nix run .#default -- --paired-perp-spot --collect-bbo
```

## Export data

The `export` command writes Parquet files for stored trades and order-book messages. It exports both datasets by default. It writes batches of 8,192 rows.

Run an export from the project root:

```sh
nix run .#default -- export --output-dir ./export
nix run .#default -- export --output-dir ./export --dataset trades --symbol BTC --symbol @1
```

Set `--dataset` to `both`, `trades`, or `orderbooks`. Repeat `--symbol` to select market names. An empty symbol list selects all markets. Price and size fields use decimal text to preserve exact values. The command fails if an output file already exists.

The export directory contains `trades.parquet` and `orderbook_messages.parquet`. The command creates only the files for the selected dataset.

The Dockerfile builds the Nix default package and copies its binary into a small runtime image. CI builds the flake Docker image with the Nix cache. CI does not use `Dockerfile`.

CI checks formatting, Clippy, and workspace tests before it builds the image. Pushes to `main` publish `latest` and a commit tag to GitHub Container Registry. Version tags publish the matching version tag.

## Development

Run commands from the project root. Use `nix develop` for the Rust toolchain and database tools. Keep `.env` local and do not commit it.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```
