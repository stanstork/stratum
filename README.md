# Stratum

[![CI](https://github.com/stanstork/stratum/actions/workflows/ci.yml/badge.svg)](https://github.com/stanstork/stratum/actions/workflows/ci.yml)
[![License: AGPL v3](https://img.shields.io/badge/license-AGPL%20v3-blue.svg)](LICENSE)
![Status: early development](https://img.shields.io/badge/status-early%20development-orange)

Stratum is a declarative data pipeline engine written in Rust. It migrates data and schema between databases safely, with crash recovery, parallel execution, rich transformation capabilities, and cryptographic post-migration verification.

```smql
connection "source" {
  driver = "mysql"
  url    = env("MYSQL_URL")
}

connection "dest" {
  driver = "postgres"
  url    = env("POSTGRES_URL")
}

pipeline "customers" {
  from { connection = connection.source table = "customers" }
  to   { connection = connection.dest   table = "customers" mode = "replace" }

  where "active" {
    customers.deleted_at is null
  }

  select {
    id    = customers.id
    name  = customers.name
    email = lower(trim(customers.email))
  }
}
```

## Why Stratum?

Most database migrations are either hand-written scripts or heavyweight ETL/CDC
platforms. Stratum sits in between - one declarative tool that:

- **Reads like config, not code.** A single SMQL file describes the whole
  migration: source, destination, filters, transforms, schema, and dependencies.
- **Is safe to re-run.** Crash-safe checkpoints mean an interrupted migration
  resumes exactly where it stopped - no half-applied state, no re-processed rows.
- **Proves it worked.** Cryptographic (Merkle-tree) verification re-reads the
  destination and proves it matches what was written, down to the offending row.
- **Migrates schema, not just rows.** Tables, indexes, foreign keys, ENUMs, and
  sequences - with FK-aware ordering and dependency-graph discovery.
- **Extends without forking.** Transforms, filters, sources, and sinks can be
  sandboxed WASM/JS plugins.

If a `pg_dump | psql` one-liner covers your case, use that. Stratum is for
migrations that need transformation, cross-engine type mapping, dependency
ordering, resumability, or verification.

## Features

- **Declarative pipelines** - SMQL v2.1 with SQL-inspired syntax
- **Schema migration** - CREATE TABLE, indexes, foreign keys, ENUMs, sequences
- **DAG execution** - `after = [pipeline.x]` dependencies, parallel levels
- **Crash recovery** - sled-backed checkpoints, automatic resume
- **Transformations** - field mapping, computed columns, `when` expressions, functions
- **Data quality** - `validate` blocks with per-row `assert` / `warn` rules
- **Fault tolerance** - circuit breaker, configurable retry, Dead Letter Queue
- **Graph references** - auto-discover and migrate FK-dependent tables
- **Pagination strategies** - primary key, numeric, timestamp cursor
- **Lifecycle hooks** - `before` / `after` SQL blocks per pipeline
- **WASM plugins** - sandboxed transform / filter / source / sink plugins in native Rust or JavaScript
- **Cryptographic verification** - Merkle tree receipts prove destination matches what was written

## Supported Connectors

| Role | Connector |
|------|-----------|
| Source | MySQL, CSV |
| Destination | PostgreSQL (COPY fast-path) |

### Secure connections (TLS/SSL)

Most managed databases (AWS RDS, GCP Cloud SQL, Azure, Neon, Supabase,
PlanetScale, Aiven, Heroku) require TLS. Both SQL drivers negotiate it from
the connection URL - no extra config. Each driver uses its ecosystem's native
parameter names; the per-driver tables below show exactly what each mode
encrypts and verifies. To **authenticate** the server (not just encrypt), use a
verifying mode (`verify-full` / `verify_ca`), with a CA bundle for private CAs.

**PostgreSQL** - libpq-style `sslmode` (plus optional `sslrootcert`):

| `sslmode`     | Transport                  | Cert chain | Hostname |
|---------------|----------------------------|------------|----------|
| `disable`     | plaintext                  | –          | –        |
| `prefer` *(default)* | TLS, falls back to plaintext | not checked | not checked |
| `require`     | TLS                        | not checked| not checked |
| `verify-ca`   | TLS                        | verified   | not checked |
| `verify-full` | TLS                        | verified   | verified |

These follow libpq: `require` encrypts but does not authenticate the server;
`verify-ca` / `verify-full` verify the certificate.

```smql
connection "dest" {
  driver = "postgres"
  url    = env("POSTGRES_URL")  # e.g. postgres://user:pass@db.example.com:5432/app?sslmode=verify-full
}
```

For a private CA (e.g. RDS/Cloud SQL/Supabase), point at the CA bundle so the
chain can be verified: `?sslmode=verify-full&sslrootcert=/path/to/ca.pem`.

**MySQL** - `require_ssl` plus optional `verify_ca` / `verify_identity`, and
`ssl_ca` for a private CA bundle (implies TLS):

| URL parameters                              | Transport | Cert chain | Hostname |
|---------------------------------------------|-----------|------------|----------|
| *(none)*                                    | plaintext | –          | –        |
| `require_ssl=true`                          | TLS       | verified   | verified |
| `require_ssl=true&verify_ca=false`          | TLS       | not checked| not checked |
| `require_ssl=true&verify_identity=false`    | TLS       | verified   | not checked |
| `ssl_ca=/path/to/ca.pem`                    | TLS       | verified against CA | verified |

```smql
connection "source" {
  driver = "mysql"
  url    = env("MYSQL_URL")  # e.g. mysql://user:pass@db.example.com:3306/app?require_ssl=true
}
```

For a private CA (e.g. RDS/Cloud SQL), supply the bundle and skip hostname
verification if the certificate's CN doesn't match the host:
`?ssl_ca=/path/to/ca.pem&verify_identity=false`.

## Project Status

Stratum is **pre-1.0 and under active development**. The engine runs real
migrations today - data + schema, with verification, crash-safe resume, and
plugins - but the SMQL language and internal APIs still change between commits.
Treat it as promising-but-young: well suited to evaluation and non-critical
workloads, not yet battle-tested for unattended production use.

**Current limitations:**

- **Destinations:** PostgreSQL only (with COPY fast-path). MySQL and CSV are
  supported as **sources** only.
- **Snapshot/batch migration only** - change-data-capture (CDC) is planned but
  not implemented.
- **Single-node:** execution and state (sled) are local to one machine; there is
  no distributed/coordinated mode.
- **Plugin host functions** (outbound HTTP, key-value, metrics) are
  capability-gated and currently stubbed - disabled by default.
- **No published binaries or crates yet** - build from source (below).

These are known and tracked in the issue tracker.

## Install

**From source (requires Rust 1.88 or newer):**

```bash
git clone https://github.com/stanstork/stratum.git
cd stratum
cargo build --release
# binary at ./target/release/stratum
```

## Quick Start

Spin up throwaway databases - MySQL seeded with the
[Sakila](https://dev.mysql.com/doc/sakila/en/) sample database, plus an empty
PostgreSQL - and run an example migration:

```bash
# 1. Start source + destination databases (credentials match .env.example)
docker compose up -d

# 2. Point Stratum at them
cp .env.example .env

# 3. Build, preview, then execute an example migration
cargo build --release
./target/release/stratum plan  -c examples/configs/schema.smql -e .env   # dry run, no writes
./target/release/stratum apply -c examples/configs/schema.smql -e .env   # execute

# Tear everything down (and delete the data)
docker compose down -v
```

## Usage

```bash
# Analyze migration plan (dry run, no changes)
stratum plan -c migration.smql

# Plan with sample data preview
stratum plan -c migration.smql --sample --sample-size 10

# Execute migration
stratum apply -c migration.smql

# Execute with live TUI progress
stratum apply -c migration.smql --tui

# Execute with colored output
stratum apply -c migration.smql --pretty

# Execute and store Merkle integrity receipt
stratum apply -c migration.smql --integrity

# Execute and store per-row hashes for row-level mismatch reporting
stratum apply -c migration.smql --full-integrity

# Verify destination matches stored receipt
stratum verify -c migration.smql

# Verify and write report to file
stratum verify -c migration.smql --output report.txt

# Test database connectivity
stratum ping --url mysql://user:pass@localhost:3306/db

# Inspect or control a run
stratum status -c migration.smql   # show run status
stratum pause  -c migration.smql   # request a graceful pause
stratum resume -c migration.smql   # resume a paused run
stratum reset  -c migration.smql   # clear all state for a migration

# Plugin tooling (compile / inspect / validate / test WASM & JS plugins)
stratum plugin --help
```

**Global flags:**

| Flag | Description |
|------|-------------|
| `-e, --env-file <FILE>` | Load environment variables from file |
| `-v / -vv` | Increase log verbosity |
| `-q, --quiet` | Suppress non-essential output |
| `--log-level <LEVEL>` | `error` \| `warn` \| `info` \| `debug` \| `trace` |
| `--log-file <FILE>` | Write logs to file |
| `--no-color` | Disable colored output |

**Environment variables:**

| Variable | Description |
|----------|-------------|
| `STRATUM_CONFIG` | Path to config file (overrides auto-discovery) |
| `STRATUM_LOG_LEVEL` | Default log level |
| `RUST_LOG` | Standard Rust log filter |

## Quick Examples

**Multi-pipeline DAG with dependencies:**
```smql
pipeline "dim_products" {
  from { connection = connection.src table = "products" }
  to   { connection = connection.dst table = "dim_products", mode = "replace" }
}

pipeline "fact_orders" {
  after = [pipeline.dim_products]  // runs after dim_products completes

  from { connection = connection.src table = "orders" }
  to   { connection = connection.dst table = "fact_orders", mode = "append" }

  with {
    products from dim_products where products.id == orders.product_id
  }

  select {
    order_id     = orders.id
    product_name = products.name
    total        = orders.total * define.tax_rate
    tier = when {
      orders.total > 10000 then "enterprise"
      orders.total > 1000  then "business"
      else "standard"
    }
  }

  paginate {
    using      = "timestamp"
    column     = orders.updated_at
    tiebreaker = orders.id
  }
}
```

**Schema migration with FK graph:**
```smql
pipeline "migrate_orders_full" {
  from {
    connection = connection.mysql_prod
    table      = "orders"

    with references {
      data  = cascade   // copy schema + referenced rows
      depth = 3
      exclude = ["audit_*"]
    }
  }

  to {
    connection = connection.postgres_warehouse
    mode       = "replace"
    map {
      orders = "fact_orders"
      users  = "dim_users"
    }
  }
}
```

**Data validation and error handling:**
```smql
validate {
  assert "positive_total" {
    check   = orders.total >= 0
    message = "Order total cannot be negative"
    action  = skip
  }
  warn "missing_email" {
    check   = users.email is not null
    message = "User email is missing"
  }
}

on_error {
  retry       { max_attempts = 3, backoff = "5s" }
  failed_rows { table = "orders_errors" }
}
```

**WASM plugins (transform + filter):**
```smql
// Declare plugins once - a .js is compiled to WASM (QuickJS) on first use;
// a prebuilt .wasm (e.g. native Rust) is loaded as-is.
plugin "to_upper"    { path = "plugins/upper.js" }
plugin "is_positive" { path = "plugins/positive.wasm" }

pipeline "customers" {
  from { connection = connection.src table = "customers" }
  to   { connection = connection.dst table = "customers" }

  select {
    id        = customers.id
    loud_name = plugin.to_upper({ name: customers.name })   // transform plugin
  }

  validate {
    rule "positive_balance" {
      filter  = plugin.is_positive({ value: customers.balance })   // filter plugin
      on_fail = skip
    }
  }
}
```

Plugins can also act as a pipeline's **source** or **sink** via a
`connection { driver = "wasm" plugin = "..." }`. See
[docs/plugins/](docs/plugins/README.md) for authoring in
[Rust](docs/plugins/rust.md) or [JavaScript](docs/plugins/javascript.md),
capabilities, and resource limits. Runnable examples: [`examples/plugins/`](examples/plugins/).

**Cryptographic verification:**
```bash
# 1. Migrate with integrity receipts
stratum apply -c migration.smql --integrity

# 2. Later, prove destination matches what was written
stratum verify -c migration.smql

# ✓ migrate_customers/customers - match (14 batches, 13,842 rows, 312ms)
# ✓ migrate_orders/orders       - match (128 batches, 127,491 rows, 2,841ms)

# With --full-integrity, mismatches are pinpointed to the exact row:
# ✗ migrate_orders/orders - MISMATCH (1 divergent batches, 2,841ms)
#   batch 4 (rows 3000-4000): expected a3f1b2c4... actual 9d8c7b6a...
#     row 3412: expected a3f1... actual 9d8c...
```

Verification re-reads the destination and compares Merkle tree roots - it detects modified, deleted, and inserted rows, not just count differences. See [docs/verification.md](docs/verification.md) for the full design.

## State & Resume

Stratum stores pipeline state in `~/.stratum/state/` (sled embedded KV). If a migration is interrupted, re-running the same command resumes from the last checkpoint - no rows are re-processed. Integrity receipts are stored in the same directory under `receipt:{pipeline}:{table}` keys.

## Documentation

| Document | Description |
|----------|-------------|
| [docs/smql-reference.md](docs/smql-reference.md) | Full SMQL v2.1 language reference |
| [docs/architecture.md](docs/architecture.md) | Crate map, design decisions, data flow |
| [docs/plugins/](docs/plugins/README.md) | WASM plugins - roles, native Rust & JS (QuickJS) runtimes, authoring, CLI |
| [docs/verification.md](docs/verification.md) | Cryptographic verification design and implementation |
| [examples/configs/](examples/configs/) | Runnable SMQL examples - schema mapping, DAG dependencies, validation, DLQ, and [`when.smql`](examples/configs/when.smql) (conditional values & computed-column chains) |

## Development

```bash
# Run all tests
cargo test

# Integration tests (requires MySQL + PostgreSQL)
cargo test -p engine-tests -- --test-threads=1

# Lint
cargo clippy --all-targets

# Format
cargo fmt
```

Test fixtures and example configs are in [`examples/configs/`](examples/configs/).

## Roadmap

Rough direction (not commitments):

- Additional destinations (MySQL sink, more connectors)
- Change-data-capture for incremental sync
- Published binaries and crates
- Plugin host capabilities (HTTP, key-value) beyond the current stubs

See the [issue tracker](https://github.com/stanstork/stratum/issues) for what's
actively in progress.

## License

Stratum is licensed under the **GNU Affero General Public License v3.0 or later**
(`AGPL-3.0-or-later`). See [LICENSE](LICENSE) for the full text.

```
Copyright (C) 2026 Stratum contributors

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU Affero General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option) any
later version. This program is distributed WITHOUT ANY WARRANTY; see the GNU
Affero General Public License for more details.
```
