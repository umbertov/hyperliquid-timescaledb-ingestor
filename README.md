# hyperliquid-timescaledb-collector

This Rust service stores Hyperliquid market data in TimescaleDB. It syncs perpetual markets, then stores live trades and level-two order-book snapshots.

## Setup

Copy the sample settings file and edit `DATABASE_URL` if the database address differs:

```sh
cp .env.example .env
```

Apply each `migrations/*/up.sql` file in timestamp order. Start the collector:

```sh
nix run .#up
```

The collector connects to the database in `DATABASE_URL`. The database must run PostgreSQL with TimescaleDB. Docker must run for the Compose deployment.

Run `nix run .#compose -- logs -f collector` to read service logs. Run `nix run .#compose -- down` to stop the service.
Set `COLLECTOR_IMAGE` to select a container image. The default uses the image that `nix run .#up` builds.

## Data

The `symbols` table stores perpetual market names. The `trades` table stores public trade events. The `orderbook_messages` table stores full bid and ask levels as JSONB arrays.

The collector uses Hyperliquid's public Rust SDK for market metadata and WebSocket subscriptions. Private account data is out of scope.

CI checks formatting, Clippy, and workspace tests before it builds the image. Pushes to `main` publish `latest` and a commit tag to GitHub Container Registry. Version tags publish the matching version tag.

## Development

Run commands from the project root. Use `nix develop` for the Rust toolchain and database tools. Keep `.env` local and do not commit it.

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
```
