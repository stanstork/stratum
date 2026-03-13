# Stratum

Stratum is a declarative data pipeline engine written in Rust. It migrates data and schema between databases safely, with crash recovery, parallel execution, and rich transformation capabilities.

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
  from { connection = connection.source, table = "customers" }
  to   { connection = connection.dest,   table = "customers", mode = "replace" }

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

## Features

- **Declarative pipelines** — SMQL v2.1 with SQL-inspired syntax
- **Schema migration** — CREATE TABLE, indexes, foreign keys, ENUMs, sequences
- **DAG execution** — `after = [pipeline.x]` dependencies, parallel levels
- **Crash recovery** — sled-backed checkpoints, automatic resume
- **Transformations** — field mapping, computed columns, `when` expressions, functions
- **Data quality** — `validate` blocks with per-row `assert` / `warn` rules
- **Fault tolerance** — circuit breaker, configurable retry, Dead Letter Queue
- **Graph references** — auto-discover and migrate FK-dependent tables
- **Pagination strategies** — primary key, numeric, timestamp cursor
- **Lifecycle hooks** — `before` / `after` SQL blocks per pipeline

## Supported Connectors

| Role | Connector |
|------|-----------|
| Source | MySQL, PostgreSQL, CSV |
| Destination | PostgreSQL (COPY fast-path) |

## Install

**From source (requires Rust stable):**

```bash
git clone https://github.com/your-org/stratum
cd stratum
cargo build --release
# binary at ./target/release/stratum
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

# Verify migrated row counts match source
stratum verify -c migration.smql

# Test database connectivity
stratum test-conn --url mysql://user:pass@localhost:3306/db
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
  from { connection = connection.src, table = "products" }
  to   { connection = connection.dst, table = "dim_products", mode = "replace" }
}

pipeline "fact_orders" {
  after = [pipeline.dim_products]  // runs after dim_products completes

  from { connection = connection.src, table = "orders" }
  to   { connection = connection.dst, table = "fact_orders", mode = "append" }

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

## State & Resume

Stratum stores pipeline state in `~/.stratum/state/` (sled embedded KV). If a migration is interrupted, re-running the same command resumes from the last checkpoint — no rows are re-processed.

## Documentation

| Document | Description |
|----------|-------------|
| [docs/smql-reference.md](docs/smql-reference.md) | Full SMQL v2.1 language reference |
| [docs/architecture.md](docs/architecture.md) | Crate map, design decisions, data flow |

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
