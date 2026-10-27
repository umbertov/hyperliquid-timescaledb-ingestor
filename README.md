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

The `symbols` table stores spot and perpetual market names. The `trades` table stores public trade events. The `orderbook_messages` table stores full bid and ask levels as JSONB arrays. The `bbo_messages` table stores best bid and offer updates. The `perp_spot_pairs` table stores the builder-dex perp to spot pairs from the mapping file.
Spot names use `@index` values. The `PURR/USDC` market uses that pair name.

The ingestor uses Hyperliquid's public Rust SDK for market metadata and WebSocket subscriptions. Direct runs collect all markets by default. Hyperliquid limits each IP to 1,000 WebSocket subscriptions. Each market uses two subscriptions. Startup fails if all markets exceed that limit.

Pass `--paired-perp-spot` to collect only symbols with both spot and perpetual markets. The ingestor subscribes to trades and order books for both markets. It recognizes known spot token name remappings, such as `UBTC` to `BTC`.

Run paired collection directly from the project root:

```sh
nix run .#default -- --paired-perp-spot --perp-dexes xyz --xyz-spot-map config/xyz-spot-map.toml --funding-poll-interval-secs 3600 --funding-backfill-start 2026-10-01T00:00:00Z
```

Compose passes `--paired-perp-spot` to the ingestor by default. Run `nix run .#compose -- down` to stop the service.

### funding collection

The ingestor polls the `fundingHistory` info request for each collected perpetual market. The `funding_rates` table stores the settled funding rate, its settlement time, and the premium. Each market and settlement time has one row.

The poller starts one millisecond after the newest stored settlement. A market with no stored settlement starts at the backfill start. The poller pages forward until the API returns no rows. It sends one request per second, which stays within the info endpoint weight limit. Duplicate rows do nothing on insert.

Set both options. The ingestor fails at startup if either option is missing:

- `--funding-poll-interval-secs` sets the seconds to wait after each full pass. It must be at least 1.
- `--funding-backfill-start` sets an RFC 3339 (internet date and time format) time, such as `2026-10-01T00:00:00Z`.

Compose reads both values from `FUNDING_POLL_INTERVAL_SECS` and `FUNDING_BACKFILL_START`. Set them in `.env` before you start the service.

```sh
nix run .#default -- --paired-perp-spot --perp-dexes xyz --xyz-spot-map config/xyz-spot-map.toml --funding-poll-interval-secs 3600 --funding-backfill-start 2026-10-01T00:00:00Z
```

The poller collects only the perpetual markets that the run selects. It does not collect predicted funding rates.

### Builder-dex collection

HIP-3 builder dexes deploy their own perpetual markets. For example, the `xyz` dex lists tokenized stocks such as `xyz:NVDA`. The `--perp-dexes` option selects the builder dexes to collect. It is required for live collection. Pass a comma-separated list, such as `--perp-dexes xyz`. Pass an empty value, `--perp-dexes ''`, to collect no builder dex.

The ingestor loads the dex list with the `perpDexs` info request and each dex's markets with `meta` and `dex`. Startup fails if Hyperliquid does not list a requested dex. The ingestor collects trades, order books, funding rates and, with `--collect-bbo`, bbo updates for every active market of each dex. It skips delisted markets. The `symbols` table stores the name as Hyperliquid gives it, such as `xyz:NVDA`.

The info API has no field that links a builder-dex perp to a spot token. Several spot tokens share one ticker, such as `NVDA`, `NVDAX` and `EQNVDA`. The ingestor does not match names. It collects a builder-dex perp alone unless the mapping file lists it. The ingestor logs the unmapped perps at startup for each dex.

Builder-dex perps count toward the 1,000-subscription limit. With `--paired-perp-spot`, the ingestor adds them after the paired selection. Startup fails if the total exceeds the limit.

Compose reads the value from `PERP_DEXES`. Compose fails if `PERP_DEXES` is not set. Compose always passes `XYZ_SPOT_MAP`, so `PERP_DEXES` must include `xyz`. An empty `PERP_DEXES=` fails startup.

### xyz spot mapping

Pass `--xyz-spot-map` with the path to a TOML file. The file maps `xyz` perps to spot pairs. The option is required when `--perp-dexes` includes `xyz`. Startup fails when the option is set without `xyz`. The option has no default path.

The sample file is `config/xyz-spot-map.toml`. Its header states the selection criterion. Each `[[pairs]]` entry has four keys: `perp`, `spot`, `spot_token` and `rationale`. Unknown keys fail startup.

At startup, the ingestor checks each entry against the live API. The perp must be an active `xyz` market. The spot pair must exist in `spotMeta`, and its base token must equal `spot_token`. A mismatch fails startup.

The ingestor subscribes to trades and order books for each mapped spot pair. With `--collect-bbo`, it also subscribes to bbo. Mapped spot pairs count toward the subscription limit. The ingestor stores each pair in the `perp_spot_pairs` table with the source `mapping-file`. At each start, the table matches the file. Rows for perps that the file no longer lists are deleted.

Compose reads the path from `XYZ_SPOT_MAP`. It mounts the file read-only at `/etc/hyperliquid/xyz-spot-map.toml`. Compose fails if `XYZ_SPOT_MAP` is not set.

### bbo collection

Pass `--collect-bbo` to also collect the `bbo` (best bid/offer) channel. This flag is off by default, so a deployment that does not pass it keeps today's behavior.

The `l2Book` channel updates roughly once every 5.4 seconds per market. The `bbo` channel updates far more often on a liquid market. The Hyperliquid Rust SDK has no `bbo` subscription, so the ingestor opens a second WebSocket connection and speaks the Hyperliquid `bbo` protocol directly. This connection reconnects and resubscribes on every disconnect, and logs each reconnect. A connection that sends nothing for 60 seconds, not even a pong to the 30-second ping, counts as dead and reconnects. The wait before a reconnect starts at 1 second and doubles up to 60 seconds. It goes back to 1 second after a connection that lasted at least 60 seconds. A message that does not match the documented `bbo` shape is a fatal error, not a skipped message.

Each enabled `bbo` subscription counts toward the same 1,000-subscription-per-IP limit as `l2Book` and `trades`, even though it uses a separate connection. Startup fails if `--collect-bbo` would push the total subscription count over that limit.

```sh
nix run .#default -- --paired-perp-spot --collect-bbo --perp-dexes xyz --xyz-spot-map config/xyz-spot-map.toml --funding-poll-interval-secs 3600 --funding-backfill-start 2026-10-01T00:00:00Z
```

## Deploy

Do these steps in this order. The ingestor needs the `funding_rates` and `perp_spot_pairs` tables before it starts.

1. Stop the running service with `nix run .#compose -- down`.
2. Apply the migrations `migrations/2026-10-27-000001_funding_rates/up.sql` and `migrations/2026-10-27-000002_perp_spot_pairs/up.sql`. Apply any earlier migration that the database does not have first. With the diesel CLI, run `diesel migration run`.
3. Add these values to `.env`:
   - `FUNDING_POLL_INTERVAL_SECS`, the seconds between polling passes, for example `3600`. Funding settles once each hour.
   - `FUNDING_BACKFILL_START`, the first settlement time to load for a market with no stored funding.
   - `PERP_DEXES`, for example `xyz`. The list must include `xyz`.
   - `XYZ_SPOT_MAP`, the path to the mapping file, for example `./config/xyz-spot-map.toml`.
4. Start the service with `nix run .#up`.
5. Read the logs. Make sure that the subscription budget line appears. Make sure that `synced builder-dex perp to spot pairs from the mapping file` appears with the expected `pair_count`. Make sure that `polled funding history` appears after the backfill.

Choose `FUNDING_BACKFILL_START` deliberately. The value applies only to markets with no stored funding. After the first pass, each market continues from its newest stored settlement, so a later change has no effect on those markets. An early start loads more history but makes the first pass longer. The info API returns at most 500 elements for each time-range request, and the poller sends one request per second. Funding settles each hour, so 30 days of history is 720 rows, or two requests for each market. With about 170 perps, a start 30 days back takes about six minutes. The funding poller runs beside the WebSocket collection, so trades and order books arrive during the backfill.

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
