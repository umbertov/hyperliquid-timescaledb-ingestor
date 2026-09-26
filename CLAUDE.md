# Project guidance

Read `README.md` before you change the project. Maintain `README.md` and `CLAUDE.md` after changes.

## Data model

- Store exchange market names in `symbols`.
- Store trades and order-book messages in TimescaleDB hypertables.
- Store bid and ask levels as JSONB arrays.
- Use exchange trade IDs and timestamps to deduplicate trade events.

## Migrations

- Make each `up.sql` safe to run twice.
- Apply migrations in timestamp order.
- Inspect the schema before you repair migration bookkeeping.

## Development

- Copy `.env.example` to `.env` before deployment.
- Apply migrations before you start the collector.
- Use `nix run .#compose -- down` to stop the Compose service.
- Keep `.env` local. Do not commit it.
- Do not log database connection strings.
- Use fixtures for automated tests that need market data.
- Do not call the live API repeatedly from a test suite.

## CI

- Keep formatting, Clippy, workspace tests, and the Docker image build in CI.
- Limit the Nix package source to Rust build inputs.
