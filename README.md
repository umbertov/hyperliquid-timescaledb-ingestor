# hyperliquid-timescaledb-ingestor

This Rust service stores Hyperliquid market data in TimescaleDB. It syncs spot and perpetual markets, then stores live trades and level-two order-book snapshots.

## Setup

Copy the sample settings file and edit `DATABASE_URL` if the database address differs:

```sh
cp .env.example .env
```

Apply each `migrations/*/up.sql` file in timestamp order. Start the ingestor:

```sh
nix run .#up
```

The migrations create time-partitioned hypertables and disable columnstore storage. The trade unique index enforces event deduplication. They add no retention policy or read optimization index.

The ingestor connects to the database in `DATABASE_URL`. The database must run PostgreSQL with TimescaleDB. Docker must run for the Compose deployment.

Compose builds the ingestor image from `Dockerfile`. The ingestor uses host networking to reach a database that Docker publishes on the host loopback address. The sample connects to `127.0.0.1:5433`.

Run `nix run .#compose -- logs -f ingestor` to read service logs. Run `nix run .#compose -- down` to stop the service.
Set `INGESTOR_IMAGE` to set the local image name. Compose builds that image when it starts the service.

## Data

The `symbols` table stores spot and perpetual market names. The `trades` table stores public trade events. The `orderbook_messages` table stores full bid and ask levels as JSONB arrays.
Spot names use `@index` values. The `PURR/USDC` market uses that pair name.

The ingestor uses Hyperliquid's public Rust SDK for market metadata and WebSocket subscriptions. It selects symbols that have both spot and perpetual markets. It subscribes to trades and order books for both markets. Startup fails if those pairs exceed 500 markets per IP. Private account data is out of scope.

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
