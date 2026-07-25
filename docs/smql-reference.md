# SMQL Reference (v2.1)

SMQL (Stratum Migration Query Language) is a declarative, SQL-inspired language for defining data pipelines and migrations. It is data-first: every keyword maps directly to a data concept rather than an infrastructure concept.

## Table of Contents

- [Core Principles](#core-principles)
- [Top-Level Blocks](#top-level-blocks)
  - [connection](#connection)
  - [define](#define)
  - [execution](#execution)
  - [plugin](#plugin)
  - [pipeline](#pipeline)
- [Pipeline Blocks](#pipeline-blocks)
  - [from](#from)
  - [to](#to)
  - [where](#where)
  - [with](#with-joins)
  - [select](#select)
  - [validate](#validate)
  - [on_error](#on_error)
  - [paginate](#paginate)
  - [before / after hooks](#before--after-hooks)
  - [settings](#settings)
- [Expressions](#expressions)
- [Graph References](#graph-references)
- [Complete Example](#complete-example)

---

## Core Principles

1. **Named pipelines** - not "migrations" or "resources"
2. **Data-first language** - tables, columns, rows
3. **SQL-inspired where it makes sense** - `where`, `with`, `select`
4. **Declarative but opinionated** - clear intent over flexibility
5. **Clear data flow** - `from -> to` is always explicit

---

## Top-Level Blocks

### connection

Defines a named data source or destination. Referenced inside pipelines via `connection.<name>`.

```smql
connection "mysql_prod" {
  driver = "mysql"
  url    = env("SOURCE_DB_URL")  // required
}

connection "warehouse_pg" {
  driver = "postgres"
  url    = env("DEST_DB")
  schema = "analytics"   // optional (Postgres); defaults to "public"
}

connection "customers_csv" {
  driver      = "csv"
  url         = "data/customers.csv"  // required: path to the file
  delimiter   = ","                   // optional; default ",". Use "\t" for TSV
  has_headers = true                  // optional; default true
  pk_column   = "id"                  // optional; marks the primary key of the created table
}
```

**Supported drivers:** `"mysql"`, `"postgres"`, `"csv"` (source only)

**`schema`** (Postgres only, optional): scopes the connection to a schema.
Unqualified reads, writes, and created tables target it (via `search_path`), and
metadata introspection is scoped to it. Defaults to `public`. The schema must
already exist. For MySQL, the schema is the database in the connection URL.

**CSV connections** point `url` at a file path and are supported as a **source**
only. Parsing options live on the connection (not the pipeline `settings` block):
`delimiter` (default `,`; the escapes `\t`, `\n`, `\r` are recognized, so use
`"\t"` for TSV), `has_headers` (default `true`), and `pk_column` (optional; marks
that column as the primary key of the inferred destination table). Column types
are inferred by sampling the file.

> **Connection pooling is not yet configurable.** Pool sizing and connection
> timeouts are planned; for now the driver defaults are used. Track it in the
> roadmap.

---

### define

Declares named constants that can be referenced throughout pipelines as `define.<name>`.

```smql
define {
  tax_rate      = 1.4
  cutoff_date   = "2024-01-01"
  active_status = "active"
}
```

Use `define.<name>` anywhere an expression is valid:

```smql
where "recent" {
  orders.created_at >= define.cutoff_date
}

select {
  order_tax = orders.total * define.tax_rate
}
```

---

### execution

Optional top-level block (singleton, no name) that controls how the pipeline
DAG runs. Independent pipelines at the same dependency level can run
concurrently. Defaults to sequential, fail-fast.

```smql
execution {
  strategy        = "parallel"   // "sequential" (default) | "parallel"
  max_concurrency = 4            // required for "parallel"; 1-100
  on_failure      = "continue"   // "fail_fast" (default) | "continue"
}
```

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `strategy` | string | `"sequential"` | `"sequential"` runs pipelines one at a time; `"parallel"` runs independent pipelines (same DAG level) concurrently |
| `max_concurrency` | integer (1-100) | – | Max pipelines running at once. **Required** when `strategy = "parallel"` |
| `on_failure` | string | `"fail_fast"` | `"fail_fast"` stops the whole run on the first failure; `"continue"` skips a failed pipeline's dependents but keeps running independent ones |

> `pipeline_timeout` / `total_timeout` (duration strings such as `"30s"`) are
> accepted by the parser but not yet enforced.

---

### plugin

Declares a WASM/JS plugin (one per name) usable as a transform, filter, source,
or sink. A `.js` file is compiled to WASM on first use; a prebuilt `.wasm` is
loaded as-is. Capabilities are denied by default (only logging is on).

```smql
plugin "normalize" {
  path = "plugins/normalize.js"   // required

  // Optional capabilities / limits:
  allow_http         = false
  allow_log          = true
  memory_limit_bytes = 134217728   // 128 MB
  fuel_limit         = 100000000
  timeout_ms         = 30000
}
```

Use it in `select` (`col = plugin.name({ field: source.col })`) or as a
`validate` check (see [validate](#validate)). Full authoring guide, roles,
capabilities, and resource limits: [docs/plugins/](plugins/README.md).

---

### pipeline

The core building block. Each pipeline reads from a source, optionally transforms data, and writes to a destination.

```smql
pipeline "pipeline_name" {
  description = "Human-readable description"

  after = [pipeline.other_pipeline]  // DAG dependency

  from { ... }
  to   { ... }

  where "filter_name" { ... }
  with  { ... }
  select { ... }
  validate { ... }
  on_error { ... }
  paginate { ... }
  before { ... }
  after  { ... }
  settings { ... }
}
```

The `after` field declares dependencies, creating a DAG. All listed pipelines must complete before this one starts. Pipelines without dependencies run in parallel.

---

## Pipeline Blocks

### from

Defines the data source.

**Single table:**
```smql
from {
  connection = connection.mysql_prod
  table      = "orders"
}
```

**Multiple-table union (planned - not yet implemented).** A `from` block
currently reads a single `table`. The intended syntax, for reference only:

```text
// PLANNED - not yet supported
from {
  connection = connection.mysql_prod
  tables     = ["orders_2023", "orders_2024"]   // implicit union
}

// PLANNED - per-table filters
from {
  connection = connection.mysql_prod
  union {
    table "orders_2023" where year == 2023
    table "orders_2024" where year == 2024
  }
}
```

**With graph references** (see [Graph References](#graph-references)):
```smql
from {
  connection = connection.mysql_prod
  table      = "orders"

  with references {
    data    = cascade
    depth   = 3
    exclude = ["audit_logs", "temp_*"]
  }
}
```

---

### to

Defines the destination.

```smql
to {
  connection = connection.warehouse_pg
  table      = "fact_orders"
  mode       = "append"
}
```

**mode values** - the destination's pre-load state (truncate vs. keep):

| Mode | Behavior |
|------|----------|
| `"append"` *(default)* | Load rows, keeping any existing ones |
| `"replace"` | Truncate the destination table, then load |

> **Row-level write strategy is a separate, currently-automatic axis.** How each
> row is written is orthogonal to truncate-vs-append: when the destination
> supports bulk COPY and the table has a primary key, rows are **upserted** (COPY
> into a staging table, then MERGE on the primary key); otherwise they are plain
> **inserted**. This is automatic and not yet configurable - choosing the strategy
> and conflict keys explicitly (a planned `on_conflict` setting) isn't available,
> and passing `mode = "upsert"` / `"merge"` is a build error.

**With table renaming for graph pipelines:**
```smql
to {
  connection = connection.warehouse_pg
  mode       = "replace"

  map {
    orders   = "fact_orders"
    users    = "dim_users"
    products = "dim_products"
  }
}
```

---

### where

Row-level filter with an optional name. The name is a self-documenting label
(it appears in plan output and diagnostics); it isn't referenced elsewhere.

```smql
where "active_only" {
  customers.status == define.active_status
}
```

**Multiple conditions** (implicit AND):
```smql
where "valid_orders" {
  orders.status == define.active_status
  orders.total > 100
  orders.created_at >= define.cutoff_date
}
```

**Operators:** `==`, `!=`, `>`, `<`, `>=`, `<=`, `is null`, `is not null`

---

### with (Joins)

Compact multi-join syntax. Each line declares: `alias from table where join_condition`.

```smql
with {
  order_items from order_items where order_items.order_id == orders.id
  users       from users       where users.id == orders.user_id
  products    from products     where products.id == order_items.product_id
  regions     from regions      where regions.id == orders.region_id
}
```

Every table used in a join condition must itself be joined (here `order_items`
is joined before `products` references it). All joined tables become available in
`where`, `select`, and `validate` blocks.

---

### select

Field mapping block. Syntax is `destination_col = expression`.

> **`select` *adds* columns; by default it doesn't restrict them.** With the
> default `copy_columns = "all"`, the destination gets **every source column plus**
> the ones you define here - so a `select` that lists a few columns still emits all
> the others too. To output *only* the columns in `select`, set
> `copy_columns = "map_only"` in [settings](#settings).

**Simple column copy:**
```smql
select {
  order_id   = orders.id
  user_id    = orders.user_id
}
```

**Rename:**
```smql
select {
  customer_id = orders.user_id  // renamed
  order_total = orders.total
}
```

**Arithmetic:**
```smql
select {
  order_tax     = orders.total * define.tax_rate
  net_revenue   = orders.total - orders.discount
}
```

**Functions:**
```smql
select {
  customer_email = lower(trim(users.email))
  order_date     = date(orders.created_at)
  order_year     = year(orders.created_at)
  order_month    = month(orders.created_at)
  order_quarter  = quarter(orders.created_at)
  synced_at      = now()
}
```

**`when` expression (conditional / pattern matching):**
```smql
select {
  revenue_tier = when {
    orders.total > 10000  then "enterprise"
    orders.total > 1000   then "business"
    orders.total > 100    then "standard"
    else "small"
  }

  status_label = when {
    orders.status == "pending"   then "Pending"
    orders.status == "shipped"   then "Shipped"
    orders.status == "delivered" then "Delivered"
    else orders.status
  }
}
```

**Named select for graph-referenced tables** (see [Graph References](#graph-references)):
```smql
// Primary table (unnamed)
select {
  order_id   = orders.id
  order_total = orders.total
}

// Named select for a referenced table
select "users" {
  user_id    = users.id
  user_name  = users.name
  user_email = lower(trim(users.email))
}
```

---

### validate

Data quality checks run per row before writing. Two check kinds:

- `assert` - on failure: `skip` the row, `fail` the pipeline, or `warn` and continue
- `warn` - always continues, logs a warning

```smql
validate {
  assert "positive_total" {
    check   = orders.total >= 0
    message = "Order total cannot be negative"
    action  = skip  // skip | fail | warn
  }

  assert "valid_email" {
    check   = customer_email is not null
    message = "Email is required"
    action  = skip
  }

  warn "high_discount" {
    check   = orders.discount <= orders.total * 0.8
    message = "Discount exceeds 80% of total"
  }

  warn "missing_customer" {
    check   = customers.customer_key is not null
    message = "Customer not found in dimension"
  }
}
```

**action values:**

| Action | Behavior |
|--------|----------|
| `skip` | Drop the row and continue - the row is **not** written anywhere (no DLQ) |
| `fail` | Send the row to the dead-letter queue (if configured), then abort the pipeline |
| `warn` | Log a warning and write the row to the destination |

> **DLQ routing:** only `fail` rows reach the dead-letter queue configured in
> [`on_error.failed_rows`](#on_error). A `fail` row is written to the DLQ *and*
> stops the pipeline; `skip` silently drops the row without persisting it. (The
> DLQ also captures rows that hit transformation/data errors, which do not stop
> the pipeline.)

**Plugin checks** - a `check` can be a filter plugin call instead of an
expression (see [plugin](#plugin)). The plugin returns a boolean verdict per
row, and `action` decides what happens when it rejects:

```smql
validate {
  assert "positive_balance" {
    check  = plugin.is_positive({ value: customers.balance })
    action = skip   // skip | fail | warn
  }
}
```

Expression checks are compiled and run by the expression engine; plugin checks
are dispatched to the WASM/JS runtime by the same per-row validator. A plugin
call is only allowed as the *entire* `check` - it cannot be embedded inside a
larger expression. When a plugin rejects a row, the reason it returns is used as
the failure message, so `message` is typically omitted for plugin checks.

---

### on_error

Configures retry behavior and dead-letter routing for rows that fail
validation or transformation.

```smql
on_error {
  // Retry transient read/write failures before giving up.
  retry {
    max_attempts = 3
    delay_ms     = 500   // base delay; grows exponentially per attempt
  }

  // Route failed rows to a dead-letter table instead of aborting.
  failed_rows {
    table {
      connection = connection.dst
      table      = "payment_failures"
      // schema  = "errors"   // optional (Postgres)
    }
  }
}
```

`failed_rows` needs a **nested destination block** - either `table { … }` (as
above) or `file { … }` for a JSON dead-letter file:

```smql
on_error {
  failed_rows {
    file {
      path   = "/tmp/inventory_failures.jsonl"
      format = "json"   // only "json" is supported today; optional
    }
  }
}
```

**Notes:**
- `retry` reads `max_attempts` (default 3) and `delay_ms` (base delay, default
  1000). The per-attempt delay grows exponentially from `delay_ms`; the backoff
  strategy itself is not yet configurable.
- A `failed_rows` block without a nested `table`/`file` destination just counts
  failures without persisting them.

---

### paginate

Optional. Controls how the source table is paginated during a snapshot read.
If omitted, Stratum uses offset-based pagination (the `default` strategy).
Column names are given as **quoted strings**, not column references.

```smql
paginate {
  strategy   = "timestamp"
  cursor     = "updated_at"
  tiebreaker = "id"
  timezone   = "UTC"
}
```

**strategy values:**

#### `"default"` - Offset / Limit (used when `paginate` is omitted)
Plain `LIMIT/OFFSET` paging; no cursor column needed. Simplest, but slower on
large tables because later pages scan past all earlier rows.

```smql
paginate { strategy = "default" }
```

#### `"pk"` - Primary Key
Keyset pagination on a monotonic primary key.

```smql
paginate {
  strategy = "pk"
  cursor   = "id"   // defaults to "id" if omitted
}
```

Generated query:
```sql
WHERE id > :last_cursor ORDER BY id LIMIT :batch_size
```

#### `"numeric"` - Numeric Column
For paginating by any numeric column that isn't the PK. Requires a `tiebreaker`
for stable ordering.

```smql
paginate {
  strategy   = "numeric"
  cursor     = "sequence_num"
  tiebreaker = "id"
}
```

Generated query:
```sql
WHERE (sequence_num > :last_cursor)
   OR (sequence_num = :last_cursor AND id > :last_id)
ORDER BY sequence_num, id LIMIT :batch_size
```

#### `"timestamp"` - Timestamp Column
For incremental / CDC-like loads. Requires a `tiebreaker`.

```smql
paginate {
  strategy   = "timestamp"
  cursor     = "updated_at"
  tiebreaker = "id"
  timezone   = "UTC"
}
```

Generated query:
```sql
WHERE (updated_at > :last_cursor)
   OR (updated_at = :last_cursor AND id > :last_id)
ORDER BY updated_at, id LIMIT :batch_size
```

**Parameters:**

| Key | Required | Description |
|-----|----------|-------------|
| `strategy` | No | `"default"` (offset), `"pk"`, `"numeric"`, or `"timestamp"`. Defaults to `"default"` |
| `cursor` | Conditional | Pagination column name (string). Defaults to `"id"`; set it for `numeric`/`timestamp` |
| `tiebreaker` | Conditional | PK column name (string) for stable ordering; **required** for `numeric` and `timestamp` |
| `timezone` | No | IANA timezone for the `timestamp` strategy (default: `"UTC"`) |

---

### before / after hooks

Raw SQL executed before or after the data migration. Useful for disabling indexes, triggers, or constraints during bulk load.

```smql
before {
  sql = [
    "ALTER TABLE fact_orders DISABLE TRIGGER ALL",
    "DROP INDEX IF EXISTS idx_orders_customer",
    "DROP INDEX IF EXISTS idx_orders_date"
  ]
}

after {
  sql = [
    "CREATE INDEX CONCURRENTLY idx_orders_customer ON fact_orders(customer_id)",
    "CREATE INDEX CONCURRENTLY idx_orders_date ON fact_orders(order_date)",
    "ALTER TABLE fact_orders ENABLE TRIGGER ALL",
    "VACUUM ANALYZE fact_orders"
  ]
}
```

---

### settings

Per-pipeline configuration overrides.

```smql
settings {
  batch_size            = env("batch_size")
  create_missing_tables = true
  copy_columns          = "all"   // "all" | "map_only"
}
```

**Available settings:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `batch_size` | integer | `1000` | Rows per batch |
| `create_missing_tables` | bool | `false` | Create the destination table if it doesn't exist |
| `create_missing_columns` | bool | `false` | Add missing columns to an existing destination table |
| `ignore_constraints` | bool | `false` | Skip creating foreign keys / constraints on the destination |
| `copy_columns` | enum | `"all"` | `"all"` copies every source column; `"map_only"` copies only mapped/`select`ed columns |

---

## Expressions

Expressions are used in `select`, `where`, `validate` and `define`.

### Literals

```smql
"string value"        // string
42                    // integer
3.14                  // float
true / false          // boolean
null                  // null
"2024-01-01"          // date string
```

### Column References

```smql
table.column          // qualified (required when multiple tables in scope)
column                // unqualified (when source is unambiguous)
```

### Arithmetic

```smql
orders.total * 1.4
orders.subtotal + orders.tax
orders.total - orders.discount
inventory.quantity / 100
```

### Comparison Operators

```smql
col == "value"
col != "value"
col > 100
col >= define.cutoff_date
col is null
col is not null
```

### Logical Operators

```smql
condition_a and condition_b
condition_a or  condition_b
```

`&&` and `||` are accepted as aliases for `and` and `or`.

### Functions

| Function | Description | Example |
|----------|-------------|---------|
| `lower(s)` | Lowercase string | `lower(users.email)` |
| `upper(s)` | Uppercase string | `upper(users.code)` |
| `trim(s)` | Strip whitespace | `trim(users.name)` |
| `concat(a, b, ...)` | String concatenation | `concat(users.first, " ", users.last)` |
| `coalesce(a, b, ...)` | First non-null value | `coalesce(users.nick, users.name, "N/A")` |
| `date(ts)` | Date value (drops the time component) | `date(orders.created_at)` |
| `year(ts)` | Year as an integer | `year(orders.created_at)` |
| `month(ts)` | Month (1-12) as an integer | `month(orders.created_at)` |
| `quarter(ts)` | Quarter (1-4) as an integer | `quarter(orders.created_at)` |
| `now()` | Current UTC timestamp | `now()` |
| `env(name, [default])` | Environment variable value | `env("REGION", "us-east-1")` |

`date` returns the date with the time dropped, while `year`, `month`, and
`quarter` return integers. They require a timestamp or date input - a
non-temporal value (e.g. a string) raises an error - and return `null` when the
input is `null`. `coalesce` returns the first non-null argument (or `null` if
all are null) and takes the type of its first argument when a computed column is
auto-created. `now()` returns the current UTC
timestamp. `env("VAR")` requires the variable and fails if it is unset;
`env("VAR", default)` returns the variable parsed to the default's type, or the
default when the variable is unset (see [Environment Variables](#environment-variables)).

### `when` Expression

Multi-branch conditional. Evaluated top-to-bottom, first match wins.

```smql
col = when {
  expr1 then value1
  expr2 then value2
  else  default_value
}
```

Example:
```smql
discount_rate = when {
  orders.total > 0  then orders.discount / orders.total
  else 0.0
}
```

Branches may use any expression - column references, arithmetic, functions,
nested `when`, `is null` checks - **except a direct plugin call** (see below). A
`when` can reference an earlier computed column in the same `select`; computed
columns are evaluated top to bottom, so later ones build on earlier ones:

```smql
select {
  net_total = orders.total - orders.discount    // computed column
  tier      = when {                            // references it
    net_total > 100 then "gold"
    net_total > 50  then "silver"
    else "bronze"
  }
}
```

A plugin call cannot appear *directly inside* a `when` branch, but you can
assign the plugin output to its own column and branch on that column - plugin
transforms run before computed columns, so the value is available:

```smql
select {
  category = plugin.classify({ text: article.body })     // plugin -> own column
  label    = when {                                      // then branch on it
    category == "spam" then "blocked"
    else "ok"
  }
}
```

### Environment Variables

`env(...)` reads a value from the environment at load time. It can be used
anywhere an expression is valid - most commonly a `connection.url`, a `define`,
or a setting - so secrets and per-environment values stay out of the config
file.

```smql
connection "src" {
  driver = "postgres"
  url    = env("DATABASE_URL")            // required
}

settings {
  batch_size = env("BATCH_SIZE", 1000)    // optional, typed default
}
```

**Two forms:**

| Form | Behavior |
|------|----------|
| `env("VAR")` | Required. Returns the variable as a string. Fails at load time if the variable is unset. |
| `env("VAR", default)` | Optional. If the variable is set, its value is parsed to the **type of `default`**; if unset, `default` is used as-is. |

**Type coercion** follows the default's type:

| Default example | Env `"120"` becomes | Notes |
|-----------------|---------------------|-------|
| `env("N", 10)` | `120` (integer) | Non-negative integer default → unsigned |
| `env("R", 1.5)` | `120.0` (float) | Whole-number floats accept integer input |
| `env("FLAG", false)` | - | `"true"`/`"false"` (case-insensitive) → boolean |
| `env("NAME", "x")` | `"120"` (string) | No coercion |

If the variable is set but cannot be parsed to the default's type (e.g.
`env("N", 10)` with `N="abc"`), loading fails with an error.

**Resolution:** values come from the process environment, overlaid by a file
passed with `-e/--env-file` (file values win on conflict).

---

## Graph References

Graph references allow a pipeline to automatically discover and migrate all FK-dependent tables from the source, without declaring each as a separate pipeline. The primary `table` in `from` becomes the entry point for FK graph traversal.

### Single Table vs Graph Pipeline

```smql
// Single table - table in both from and to
from { table = "orders" }
to   { table = "orders_copy" }

// Graph pipeline - table only in from; to uses map for renaming
from {
  table = "orders"
  with references { data = cascade }
}
to {
  mode = "replace"
  map  { orders = "fact_orders" }
}
```

### with references Block

Placed inside `from`. Controls graph traversal behavior.

```smql
from {
  connection = connection.mysql_prod
  table      = "orders"

  with references {
    data    = cascade          // cascade | schema_only (default: schema_only)
    depth   = all              // all | 1, 2, 3... (default: all)
    exclude = ["audit_logs", "temp_*", "*_staging"]
  }
}
```

| Option | Values | Default | Description |
|--------|--------|---------|-------------|
| `data` | `cascade`, `schema_only` | `schema_only` | Whether to copy row data for referenced tables |
| `depth` | `all` or integer | `all` | How many FK levels to follow |
| `exclude` | array of strings/patterns | `[]` | Tables to skip (supports wildcards: `audit_*`, `*_log`, `*log*`, `*`) |

**Schema behavior:**

| Setting | Schema created | Data copied |
|---------|:--------------:|:-----------:|
| `with references {}` | ✓ | ✗ |
| `with references { data = cascade }` | ✓ | ✓ (referenced rows only) |

### Destination Table Renaming

Use `map` in `to` to rename tables at the destination. Unmapped tables keep their original names.

```smql
to {
  connection = connection.warehouse_pg
  mode       = "replace"

  map {
    orders   = "fact_orders"
    users    = "dim_users"
    products = "dim_products"
    regions  = "dim_regions"
  }
}
```

### Field Mappings for Referenced Tables

Use named `select` blocks to define field mappings per referenced table. The unnamed `select` applies to the primary table.

```smql
// Primary table
select {
  order_id   = orders.id
  order_total = orders.total
}

// Referenced tables
select "users" {
  user_id    = users.id
  user_name  = users.name
  user_email = lower(trim(users.email))
}

select "products" {
  product_id   = products.id
  product_name = products.name
  category     = products.category
}
```

### Data Filtering with Cascade

When `data = cascade`, the `where` clause on the primary table propagates: only rows referenced by filtered primary rows are copied, recursively up to `depth` levels.

Example: `where` filters to orders 1, 2, 3 -> only users referenced by those orders are copied -> only regions referenced by those users are copied.

### Complete Graph Example

```smql
pipeline "migrate_orders" {
  description = "Migrate orders with all FK dependencies"

  from {
    connection = connection.mysql_prod
    table      = "orders"

    with references {
      data    = cascade
      depth   = 3
      exclude = ["audit_logs", "temp_*"]
    }
  }

  to {
    connection = connection.postgres_warehouse
    mode       = "replace"

    map {
      orders   = "fact_orders"
      users    = "dim_users"
      products = "dim_products"
      regions  = "dim_regions"
    }
  }

  where "recent_orders" {
    orders.created_at >= define.cutoff_date
  }

  select {
    order_id    = orders.id
    customer_id = orders.user_id
    order_total = orders.total
    order_date  = date(orders.created_at)
  }

  select "users" {
    user_id    = users.id
    user_name  = users.name
    user_email = lower(trim(users.email))
  }

  select "products" {
    product_id   = products.id
    product_name = products.name
    category     = products.category
  }
}
```

---

## Complete Example

An e-commerce warehouse showing every pipeline block together - a star schema
built from explicit dimension/fact pipelines, plus a graph-cascade pipeline
that auto-follows FK references (`with references`, `map`, and named `select`).

```smql
// ================================================================
// Configuration
// ================================================================

define {
  tax_rate      = 1.4
  cutoff_date   = "2024-01-01"
  active_status = "active"
}

// Run independent pipelines (e.g. the dimensions) concurrently.
execution {
  strategy        = "parallel"
  max_concurrency = 3
  on_failure      = "fail_fast"
}

// ================================================================
// Connections
// ================================================================

connection "mysql_prod" {
  driver = "mysql"
  url    = env("source_db")
}

connection "postgres_warehouse" {
  driver = "postgres"
  url    = env("dest_db")
}

// ================================================================
// Dimensions (load first - no dependencies)
// ================================================================

pipeline "dim_customers" {
  description = "Customer dimension"

  from {
    connection = connection.mysql_prod
    table      = "customers"
  }

  to {
    connection = connection.postgres_warehouse
    table      = "dim_customers"
    mode       = "replace"
  }

  where "active_customers" {
    customers.status == define.active_status
  }

  select {
    customer_key    = customers.id
    customer_name   = customers.name
    customer_email  = lower(trim(customers.email))
    customer_segment = customers.segment
    created_at      = customers.created_at
  }

  validate {
    assert "valid_email" {
      check   = customer_email is not null
      message = "Email is required"
      action  = skip
    }
  }

  settings {
    batch_size = env("batch_size")
  }
}

pipeline "dim_products" {
  description = "Product dimension"

  from {
    connection = connection.mysql_prod
    table      = "products"
  }

  to {
    connection = connection.postgres_warehouse
    table      = "dim_products"
    mode       = "replace"
  }

  select {
    product_key  = products.id
    product_name = products.name
    category     = products.category
    price        = products.price
  }
}

pipeline "dim_regions" {
  description = "Region dimension"

  from {
    connection = connection.mysql_prod
    table      = "regions"
  }

  to {
    connection = connection.postgres_warehouse
    table      = "dim_regions"
    mode       = "replace"
  }

  select {
    region_key  = regions.id
    region_name = regions.name
    country     = regions.country
  }
}

// ================================================================
// Facts (load after dimensions)
// ================================================================

pipeline "fact_orders" {
  description = "Orders fact table with denormalized dimensions"

  after = [
    pipeline.dim_customers,
    pipeline.dim_products,
    pipeline.dim_regions
  ]

  from {
    connection = connection.mysql_prod
    table      = "orders"
  }

  to {
    connection = connection.postgres_warehouse
    table      = "fact_orders"
    mode       = "append"
  }

  where "valid_orders" {
    orders.status == define.active_status
    orders.total > 0
    orders.created_at >= define.cutoff_date
  }

  // Join related source tables (all in mysql_prod)
  with {
    order_items from order_items where order_items.order_id == orders.id
    customers   from customers   where customers.id == orders.user_id
    products    from products     where products.id == order_items.product_id
    regions     from regions      where regions.id == orders.region_id
  }

  select {
    // Keys
    order_key     = orders.id
    customer_key  = orders.user_id
    product_key   = order_items.product_id
    region_key    = orders.region_id

    // Customer dimensions
    customer_name    = customers.name
    customer_email   = lower(trim(customers.email))
    customer_segment = customers.segment

    // Product dimensions
    product_name = products.name
    category     = products.category
    list_price   = products.price

    // Region dimensions
    region_name = regions.name
    country     = regions.country

    // Order metrics
    quantity    = order_items.quantity
    subtotal    = orders.subtotal
    tax         = orders.subtotal * define.tax_rate
    total       = orders.total
    discount    = orders.discount
    net_revenue = orders.total - orders.discount

    // Dates
    order_date    = date(orders.created_at)
    order_year    = year(orders.created_at)
    order_month   = month(orders.created_at)
    order_quarter = quarter(orders.created_at)

    // Computed dimensions
    revenue_tier = when {
      orders.total > 10000  then "enterprise"
      orders.total > 1000   then "business"
      orders.total > 100    then "standard"
      else "small"
    }

    discount_rate = when {
      orders.total > 0  then orders.discount / orders.total
      else 0.0
    }

    // Audit
    synced_at = now()
  }

  validate {
    assert "positive_total" {
      check   = orders.total >= 0
      message = "Order total cannot be negative"
      action  = skip
    }

    assert "valid_quantity" {
      check   = order_items.quantity > 0
      message = "Quantity must be positive"
      action  = skip
    }

    warn "high_discount" {
      check   = orders.discount <= orders.total * 0.8
      message = "Discount exceeds 80% of total"
    }

    warn "missing_customer" {
      check   = customers.id is not null
      message = "Customer not found"
    }
  }

  on_error {
    retry {
      max_attempts = 3
      delay_ms     = 500
    }
    failed_rows {
      table {
        connection = connection.postgres_warehouse
        table      = "fact_orders_errors"
      }
    }
  }

  paginate {
    strategy   = "timestamp"
    cursor     = "orders.updated_at"
    tiebreaker = "orders.id"
    timezone   = "UTC"
  }

  before {
    sql = [
      "ALTER TABLE fact_orders DISABLE TRIGGER ALL",
      "DROP INDEX IF EXISTS idx_orders_customer",
      "DROP INDEX IF EXISTS idx_orders_date",
      "DROP INDEX IF EXISTS idx_orders_product"
    ]
  }

  after {
    sql = [
      "CREATE INDEX CONCURRENTLY idx_orders_customer ON fact_orders(customer_key)",
      "CREATE INDEX CONCURRENTLY idx_orders_date ON fact_orders(order_date)",
      "CREATE INDEX CONCURRENTLY idx_orders_product ON fact_orders(product_key)",
      "ALTER TABLE fact_orders ENABLE TRIGGER ALL",
      "VACUUM ANALYZE fact_orders"
    ]
  }

  settings {
    batch_size            = env("batch_size")
    create_missing_tables = true
  }
}

// ================================================================
// Graph cascade - migrate a table and auto-follow its FK references
// (an alternative to declaring each referenced table by hand)
// ================================================================

pipeline "suppliers_graph" {
  description = "Migrate suppliers and everything they reference"

  from {
    connection = connection.mysql_prod
    table      = "suppliers"

    // Discover and migrate FK-dependent tables (contacts, addresses, ...)
    with references {
      data    = cascade          // copy referenced rows too (default: schema_only)
      depth   = all              // follow every FK level
      exclude = ["audit_*"]      // skip audit tables (wildcards allowed)
    }
  }

  to {
    connection = connection.postgres_warehouse
    mode       = "replace"

    // Rename tables at the destination; unmapped tables keep their names.
    map {
      suppliers = "dim_suppliers"
      contacts  = "dim_contacts"
    }
  }

  // Field mapping for the primary (entry) table.
  select {
    supplier_key = suppliers.id
    supplier_name = suppliers.name
  }

  // Named select: field mapping for a referenced (cascaded) table.
  select "contacts" {
    contact_key = contacts.id
    email       = lower(trim(contacts.email))
  }
}
```
