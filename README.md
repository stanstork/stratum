<p align="center">
  <picture>
    <source
      media="(prefers-color-scheme: dark)"
      srcset="docs/assets/paganel-mark-dark.svg">
    <img
      src="docs/assets/paganel-mark-light.svg"
      alt="Paganel logo"
      width="128">
  </picture>
</p>

<h1 align="center">Paganel</h1>

<p align="center">
  <strong>Data migration that proves it worked.</strong>
</p>

<p align="center">
  <a href="https://github.com/stanstork/stratum/actions/workflows/ci.yml"><img src="https://github.com/stanstork/stratum/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-AGPL%20v3-blue.svg" alt="AGPL v3"></a>
  <img src="https://img.shields.io/badge/status-early%20development-orange" alt="Early development">
</p>

<p align="center">
  <img src="assets/demo.gif" alt="Migrate Sakila with integrity receipts, verify (all tables match), hand-edit one destination row, verify again — Paganel names the exact divergent row." width="820">
</p>

<!-- demo.gif is rendered from assets/demo.tape; regenerate with `vhs assets/demo.tape` (needs the quickstart DBs up and a release build). -->

Paganel is a data migration engine written in Rust. It moves data and schema between systems with crash recovery, parallel execution, and in-flight transforms, then cryptographically verifies that the destination matches what was written, down to the row. Today that means MySQL, PostgreSQL, and CSV; sources and sinks can also be sandboxed WASM plugins, so anything with data can stand at either end of a pipeline.

On a 100M-row MySQL->PostgreSQL copy with the databases on separate hosts (over a real network), it sustains ~390K rows/s on a single lane and ~940K rows/s with four parallel lanes ([benchmarks](docs/benchmarks.md)).

```ppl
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

## Why Paganel?

Most database migrations are either hand-written scripts or heavyweight ETL/CDC
platforms. Paganel sits in between - one declarative tool that:

- **Reads like config.** A single PPL file describes the whole
  migration: source, destination, filters, transforms, schema, and dependencies.
- **Is safe to re-run.** Crash-safe checkpoints mean an interrupted migration
  resumes exactly where it stopped - no half-applied state, no re-processed rows.
- **Checks it worked.** Cryptographic (Merkle-tree) verification re-reads the
  destination and detects any difference from what was written, down to the
  offending row.
- **Migrates schema too.** Tables, indexes, foreign keys, ENUMs, and
  sequences - with FK-aware ordering and dependency-graph discovery.
- **Extends without forking.** Transforms, filters, sources, and sinks can be
  sandboxed WASM/JS plugins.

If a `pg_dump | psql` one-liner covers your case, use that. Paganel is for
migrations that need transformation, cross-engine type mapping, dependency
ordering, resumability, or verification.

## Features

- **Declarative pipelines** - PPL with SQL-inspired syntax
- **Schema migration** - CREATE TABLE, indexes, foreign keys, ENUMs, sequences
- **DAG execution** - `after = [pipeline.x]` dependencies, parallel levels
- **Crash recovery** - sled-backed checkpoints, automatic resume
- **Transformations** - field mapping, computed columns, `when` expressions, functions
- **Data quality** - `validate` blocks with per-row `assert` / `warn` rules
- **Fault tolerance** - circuit breaker, configurable retry, Dead Letter Queue
- **Graph references** - auto-discover and migrate FK-dependent tables
- **Multi-table pipelines** - `tables = [...]` fans one block out into a full copy per table
- **Parallel lanes** - `lanes = N` splits a large single-table copy into N primary-key ranges; graph migrations run their tables concurrently
- **Pagination strategies** - primary key, numeric, timestamp cursor
- **Lifecycle hooks** - `before` / `after` SQL blocks per pipeline
- **WASM plugins** - sandboxed transform / filter / source / sink plugins in native Rust or JavaScript
- **Cryptographic verification** - Merkle tree receipts detect any difference between the destination and what was written

## Supported Connectors

| Role | Connector |
|------|-----------|
| Source | MySQL, PostgreSQL, CSV |
| Destination | PostgreSQL (COPY fast-path), MySQL (LOAD DATA fast-path) |

### Secure connections (TLS/SSL)

Most managed databases (AWS RDS, GCP Cloud SQL, Azure, Neon, Supabase,
PlanetScale, Aiven, Heroku) require TLS. Both SQL drivers negotiate it from
the connection URL - no extra config. Each driver uses its ecosystem's native
parameter names; the per-driver tables below show exactly what each mode
encrypts and verifies. To authenticate the server as well as encrypt the link, use a
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

```ppl
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

```ppl
connection "source" {
  driver = "mysql"
  url    = env("MYSQL_URL")  # e.g. mysql://user:pass@db.example.com:3306/app?require_ssl=true
}
```

For a private CA (e.g. RDS/Cloud SQL), supply the bundle and skip hostname
verification if the certificate's CN doesn't match the host:
`?ssl_ca=/path/to/ca.pem&verify_identity=false`.

## Project Status

Paganel is pre-1.0. The engine runs real migrations today - data + schema,
with verification, crash-safe resume, and plugins - but the PPL language and
internal APIs still change between commits. Use it for evaluation and
non-critical workloads; don't leave it unattended in production yet.

**Current limitations:**

- **Destinations:** PostgreSQL (COPY fast-path) and MySQL (LOAD DATA fast-path).
  CSV is supported as a source only.
- **Snapshot/batch migration only** - change-data-capture (CDC) is planned but
  not implemented.
- **Single-node:** execution and state (sled) are local to one machine; there is
  no distributed/coordinated mode.
- **Plugin host functions** (outbound HTTP, key-value, metrics) are
  capability-gated and off by default. Outbound HTTP is guarded (link-local/
  cloud-metadata blocked, per-request timeout, response-size cap, optional host
  allowlist); the key-value store is instance-scoped scratch, not persisted.
- **No published binaries or crates yet** - build from source (below).

## Install

**From source (requires Rust 1.88 or newer):**

```bash
git clone https://github.com/stanstork/stratum.git
cd paganel
cargo build --release
# binary at ./target/release/pag
```

## Quick Start

Spin up throwaway databases - MySQL seeded with the
[Sakila](https://dev.mysql.com/doc/sakila/en/) sample database, plus an empty
PostgreSQL - and run an example migration:

```bash
# 1. Start source + destination databases (credentials match .env.example)
docker compose up -d

# 2. Point Paganel at them
cp .env.example .env

# 3. Build, preview, then execute an example migration
cargo build --release
./target/release/pag plan  -c examples/configs/schema.ppl -e .env   # dry run, no writes
./target/release/pag apply -c examples/configs/schema.ppl -e .env   # execute

# Tear everything down (and delete the data)
docker compose down -v
```

> **Ports already in use?** The containers publish on **15432** (PostgreSQL) and
> **13306** (MySQL) by default (not 5432/3306), so `docker compose up -d` works
> even if you already run those databases locally. To pick different host ports,
> set `POSTGRES_PORT` / `MYSQL_PORT` (in `.env` or your shell) and update the
> matching port in the URLs in `.env`.

## Usage

```bash
# Analyze migration plan (dry run, no changes) - prints a human summary
pag plan -c migration.ppl

# Full machine-readable report (for CI / tooling)
pag plan -c migration.ppl --json

# Preview transformed sample rows in the summary
pag plan -c migration.ppl --sample --sample-size 10

# Print the exact CREATE / ALTER DDL the migration would run
pag plan -c migration.ppl --ddl

# Execute migration
pag apply -c migration.ppl

# Execute with live TUI progress
pag apply -c migration.ppl --tui

# Execute with colored output
pag apply -c migration.ppl --pretty

# Execute and commit a keyed Merkle integrity receipt
pag apply -c migration.ppl --integrity

# Verify destination matches stored receipt
pag verify -c migration.ppl

# Verify and write report to file
pag verify -c migration.ppl --output report.txt

# Test database connectivity
pag ping --url mysql://user:pass@localhost:3306/db

# Inspect or control a run
pag status -c migration.ppl   # show run status
pag pause  -c migration.ppl   # request a graceful pause
pag resume -c migration.ppl   # resume a paused run
pag reset  -c migration.ppl   # clear all state for a migration

# Plugin tooling (compile / inspect / validate / test WASM & JS plugins)
pag plugin --help
```

> **`plan` summary vs `--json`.** The default `plan` output is a compact human
> summary. `--json` emits the complete report it's built from - every column
> with its type and indexes, the full row-count objects (`value` / `is_estimated`
> / `confidence`), all mappings and joins, per-pipeline diagnostics *including* the
> routine `info` notes the summary collapses, execution stages, and full resource
> estimations - plus run metadata (`plan_id`, `config_hash`, resolved `defines`).
> Use the summary to read, `--json` to gate CI or feed tooling. `stdout` carries
> only the report, so `pag plan --json > plan.json` is always valid JSON. See
> [docs/plan.md](docs/plan.md#the---json-report) for the shape, and
> [docs/schema-plan.json](docs/schema-plan.json) for a complete example report.

<p align="center">
  <img src="assets/plan.png" alt="pag plan output: a two-stage execution DAG (migrate_payment waits for migrate_customer), per-pipeline column renames, excluded columns, joins, primary keys, and duration/memory/transfer estimates — ending in 'Ready to apply'." width="820">
</p>

<!-- plan.png is real `pag plan` output; regenerate with `./assets/plan.sh` (needs the quickstart DBs up, a release build, and tmux/freeze/ImageMagick). -->

> **`apply` / `verify` output modes.** `apply` runs in default log mode, `--pretty`
> (colored, per-event lines), or `--tui` (a live dashboard with pause/cancel
> controls). `verify` prints stable `✓`/`✗`/`?` result lines by default and adds
> headers, progress phases, and a summary under `--pretty`. See
> [docs/output-modes.md](docs/output-modes.md) for annotated examples of each and
> the TUI keyboard reference.

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
| `PAGANEL_CONFIG` | Path to config file (overrides auto-discovery) |
| `PAGANEL_LOG_LEVEL` | Default log level |
| `RUST_LOG` | Standard Rust log filter |

## Quick Examples

**Multi-pipeline DAG with dependencies:**
```ppl
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
    strategy   = "timestamp"
    cursor     = "orders.updated_at"
    tiebreaker = "orders.id"
  }
}
```

**Schema migration with FK graph:**
```ppl
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

**Multi-table pipeline (one block, many tables):**
```ppl
// `tables` fans out into one full-copy pipeline per table - connections and
// settings declared once. Tables run under the `execution` strategy (parallel
// here). Optional per-table tweaks: `select "T"` projects/renames columns and
// `map` renames the destination table; unnamed tables copy verbatim.
pipeline "warehouse" {
  from {
    connection = connection.src
    tables = ["actor", "category", "customer"]
  }
  to {
    connection = connection.dst
    map { customer = "dim_customer" }
  }
  select "customer" {
    id          = customer.customer_id
    given_name  = customer.first_name
    family_name = customer.last_name
  }
  settings { create_missing_tables = true }
}
```

**Data validation and error handling:**
```ppl
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
```ppl
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
    assert "positive_balance" {
      check  = plugin.is_positive({ value: customers.balance })   // filter plugin
      action = skip
    }
  }
}
```

Plugins can also act as a pipeline's source or sink via a
`connection { driver = "wasm" plugin = "..." }`. See
[docs/plugins/](docs/plugins/README.md) for authoring in
[Rust](docs/plugins/rust.md) or [JavaScript](docs/plugins/javascript.md),
capabilities, and resource limits. Runnable examples: [`examples/plugins/`](examples/plugins/).

**Cryptographic verification:**
```bash
# 1. Migrate with integrity receipts
pag apply -c migration.ppl --integrity

# 2. Later, check the destination against what was written
pag verify -c migration.ppl

# ✓ migrate_customers/customers - match (13,842 rows, root a3f1b2c49d8c7b6a, 312ms)
# ✓ migrate_orders/orders       - match (127,491 rows, root 5e2d8a1c04b93f77, 2,841ms)

# Mismatches are pinpointed to the exact row, by primary key:
# ✗ migrate_orders/orders - MISMATCH (0 missing, 1 changed, 0 extra; 127,491 rows expected, 127,491 found; 2,841ms)
#   expected root 5e2d8a1c04b93f77
#   actual   root 9d8c7b6a1f2e3d4c
#   order_id=3412 - changed: expected a3f1b2c49d8c7b6a actual 9d8c7b6a1f2e3d4c
```

Every row hash is keyed by its primary key, so verification is independent of batch size, lane count, and read order - it detects modified, deleted, and inserted rows by key.

Integrity costs ~0.3-0.5 µs per row, and the hashes stream to disk rather than memory: about 51 bytes per row, so a 10M-row table with an integer key leaves ~510 MB under `~/.paganel/state/` until that pipeline runs again. See [docs/verification.md](docs/verification.md) for the full design and [benchmarks](docs/benchmarks.md#the-cost-of---integrity) for the measured overhead.

## State & Resume

Paganel stores pipeline state in `~/.paganel/state/` (sled embedded KV). If a migration is interrupted, re-running the same command resumes from the last checkpoint - no rows are re-processed. Integrity receipts are stored in the same directory under `receipt:{pipeline}:{table}` keys.

`apply` also records the throughput it achieves into a separate calibration cache (`~/.paganel/calibration`) so `pag plan` can estimate duration from this machine's measured rates rather than a generic default; until then it shows a conservative, clearly-labelled rough estimate. See [docs/plan.md](docs/plan.md#duration-estimates).

## Documentation

| Document | Description |
|----------|-------------|
| [docs/plan.md](docs/plan.md) | Reading `pag plan` - the summary, flags, sampling, and the magnitude bar |
| [docs/ppl-reference.md](docs/ppl-reference.md) | Full PPL language reference |
| [docs/why-ppl.md](docs/why-ppl.md) | Why a purpose-built DSL (PPL) instead of YAML/JSON/SQL - design rationale and trade-offs |
| [docs/architecture.md](docs/architecture.md) | Crate map, design decisions, data flow |
| [docs/plugins/](docs/plugins/README.md) | WASM plugins - roles, native Rust & JS (QuickJS) runtimes, authoring, CLI |
| [docs/verification.md](docs/verification.md) | Cryptographic verification design and implementation |
| [docs/benchmarks.md](docs/benchmarks.md) | Reproducible Paganel benchmark (optional pgloader comparison) - methodology, results, `./benchmarks/run.sh` |
| [examples/configs/](examples/configs/) | Runnable PPL examples - schema mapping, DAG dependencies, validation, DLQ, and [`when.ppl`](examples/configs/when.ppl) (conditional values & computed-column chains) |

## Development

```bash
# Run all tests
cargo test

# Integration tests (requires MySQL + PostgreSQL)
docker compose up -d # seeds the exact test databases
cargo test -p engine-tests -- --test-threads=1

# Lint
cargo clippy --all-targets

# Format
cargo fmt
```

The integration fixtures default to the `docker compose` ports (15432/13306), so
the two commands above are all you need. To run against databases on other ports,
set `POSTGRES_PORT` / `MYSQL_PORT` (e.g. `POSTGRES_PORT=5432 MYSQL_PORT=3306
cargo test -p engine-tests -- --test-threads=1`).

Test fixtures and example configs are in [`examples/configs/`](examples/configs/).

## Roadmap

Rough direction (not commitments):

- Additional connectors and destinations
- Change-data-capture for incremental sync
- Multiple-table union sources (`from` reading several tables)
- Configurable connection pooling (pool size, timeouts)
- Published binaries and crates
- Persistent (cross-run) plugin key-value store - today's store is instance-scoped scratch

See the [issue tracker](https://github.com/stanstork/stratum/issues) for what's
actively in progress.

## License

Paganel is licensed under the **GNU Affero General Public License v3.0 or later**
(`AGPL-3.0-or-later`). See [LICENSE](LICENSE) for the full text.

```
Copyright (C) 2026 Paganel contributors

This program is free software: you can redistribute it and/or modify it under
the terms of the GNU Affero General Public License as published by the Free
Software Foundation, either version 3 of the License, or (at your option) any
later version. This program is distributed WITHOUT ANY WARRANTY; see the GNU
Affero General Public License for more details.
```
