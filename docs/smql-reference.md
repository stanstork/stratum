# SMQL Reference (v2.1)

SMQL (Stratum Migration Query Language) is a declarative, SQL-inspired language for defining data pipelines and migrations. It is data-first: every keyword maps directly to a data concept rather than an infrastructure concept.

## Table of Contents

- [Core Principles](#core-principles)
- [Top-Level Blocks](#top-level-blocks)
  - [connection](#connection)
  - [define](#define)
  - [transform](#transform)
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

1. **Named pipelines** — not "migrations" or "resources"
2. **Data-first language** — tables, columns, rows
3. **SQL-inspired where it makes sense** — `where`, `with`, `select`
4. **Declarative but opinionated** — clear intent over flexibility
5. **Clear data flow** — `from → to` is always explicit

---

## Top-Level Blocks

### connection

Defines a named data source or destination. Referenced inside pipelines via `connection.<name>`.

```smql
connection "mysql_prod" {
  driver = "mysql"
  url    = env("SOURCE_DB_URL")  // required

  pool {
    max_size = env("DB_POOL_SIZE", 20)  // optional, with default
  }
}

connection "warehouse_pg" {
  driver = "postgres"
  url    = env("DEST_DB")

  pool {
    max_size = 50
    timeout  = "60s"
  }
}
```

**Supported drivers:** `"mysql"`, `"postgres"`

**pool options:**

| Key | Type | Description |
|-----|------|-------------|
| `max_size` | integer | Maximum number of pooled connections |
| `timeout` | string | Connection timeout (e.g. `"30s"`, `"60s"`) |

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

### transform

Defines a reusable named transformation. Takes typed input, returns an expression output. Called in `select` blocks via `transform.<name>(arg)`.

```smql
transform "normalize_email" {
  input  = string
  output = lower(trim(input))
}

transform "calculate_tax" {
  input  = number
  output = input * define.tax_rate
}
```

Usage in a pipeline:

```smql
select {
  email = transform.normalize_email(customers.email)
  tax   = transform.calculate_tax(orders.subtotal)
}
```

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

**Multiple tables (implicit union):**
```smql
from {
  connection = connection.mysql_prod
  tables     = ["orders_2023", "orders_2024"]  // Union
}
```

**Explicit union with per-table filters:**
```smql
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

**mode values:**

| Mode | Behavior |
|------|----------|
| `"replace"` | Truncate destination table and reload |
| `"append"` | Insert new rows, keep existing |
| `"upsert"` | Insert or update on conflict |
| `"merge"` | Full merge based on key columns |

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

Named row-level filter. The name makes it reusable and self-documenting.

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

**Operators:** `==`, `!=`, `>`, `<`, `>=`, `<=`, `is null`, `is not null`, `matches "regex"`

---

### with (Joins)

Compact multi-join syntax. Each line declares: `alias from table where join_condition`.

```smql
with {
  users     from users     where users.id == orders.user_id
  products  from products  where products.id == order_items.product_id
  regions   from regions   where regions.id == orders.region_id
}
```

All joined tables become available in `where`, `select`, and `validate` blocks.

---

### select

Field mapping block. Syntax is `destination_col = expression`.

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

**`coalesce` (null fallback):**
```smql
select {
  display_name = coalesce(customers.nickname, customers.name, "Anonymous")
}
```

**Reusable transform:**
```smql
select {
  email = transform.normalize_email(customers.email)
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

Data quality checks run per row before writing. Two rule types:

- `assert` — on failure: `skip` the row, `fail` the pipeline, or `warn` and continue
- `warn` — always continues, logs a warning

```smql
validate {
  assert "positive_total" {
    check   = orders.total >= 0
    message = "Order total cannot be negative"
    action  = skip  // skip | fail | warn
  }

  assert "valid_email" {
    check   = customer_email matches "^[^@]+@[^@]+\.[^@]+$"
    message = "Invalid email format"
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
| `skip` | Drop the row, continue pipeline |
| `fail` | Abort the pipeline with an error |
| `warn` | Log a warning, write the row |

---

### on_error

Configures retry behavior, dead-letter routing, and alerting.

```smql
on_error {
  retry {
    max_attempts = 3
    backoff      = "5s"
  }

  failed_rows {
    table = "failed_orders"
  }

  alert {
    email = "team@example.com"
  }
}
```

**Compact form:**
```smql
on_error {
  retry      { max_attempts = 3, backoff = "5s" }
  failed_rows { table = "errors" }
  alert      { email = "team@example.com" }
}
```

---

### paginate

Controls how the source table is paginated. Required for large tables or incremental loads.

```smql
paginate {
  using      = "timestamp"
  column     = orders.updated_at
  tiebreaker = orders.id
  timezone   = "UTC"
}
```

**using strategies:**

#### `"pk"` — Primary Key (default)
Best for tables with auto-increment IDs.

```smql
paginate {
  using  = "pk"
  column = orders.id  // defaults to id if omitted
}
```

Generated query:
```sql
WHERE id > :last_cursor ORDER BY id LIMIT :batch_size
```

#### `"numeric"` — Numeric Column
For paginating by any numeric column that isn't the PK.

```smql
paginate {
  using      = "numeric"
  column     = events.sequence_num
  tiebreaker = events.id
}
```

Generated query:
```sql
WHERE (sequence_num > :last_cursor)
   OR (sequence_num = :last_cursor AND id > :last_id)
ORDER BY sequence_num, id LIMIT :batch_size
```

#### `"timestamp"` — Timestamp Column
For incremental / CDC-like loads.

```smql
paginate {
  using      = "timestamp"
  column     = orders.updated_at
  tiebreaker = orders.id
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
| `using` | Yes | Strategy: `"pk"`, `"numeric"`, `"timestamp"` |
| `column` | Conditional | Pagination column. Defaults to `id` for `pk` |
| `tiebreaker` | Conditional | PK for stable ordering when cursor is non-unique |
| `timezone` | No | IANA timezone for timestamp strategy (default: `"UTC"`) |

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
  batch_size = env("batch_size")
  workers    = 4
  checkpoint = every_batch
}
```

**Available settings:**

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `batch_size` | integer | `1000` | Rows per batch |
| `workers` | integer | `4` | Parallel worker count |
| `checkpoint` | enum | `every_batch` | When to checkpoint state |
| `create_missing_tables` | bool | `false` | Auto-create destination table if missing |
| `offset_strategy` | string | `"pk"` | Default pagination strategy |

---

## Expressions

Expressions are used in `select`, `where`, `validate` and `define`.

### Literals

```smql
"string value"        // string
42                    // integer
3.14                  // float
true / false          // boolean
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
col matches "^[A-Z]+"   // regex match
```

### Logical Operators

```smql
condition_a and condition_b
condition_a or condition_b
```

### Functions

| Function | Description | Example |
|----------|-------------|---------|
| `lower(s)` | Lowercase string | `lower(users.email)` |
| `upper(s)` | Uppercase string | `upper(users.code)` |
| `trim(s)` | Strip whitespace | `trim(users.name)` |
| `concat(a, b, ...)` | String concatenation | `concat(users.first, " ", users.last)` |
| `coalesce(a, b, ...)` | First non-null value | `coalesce(users.nick, users.name, "N/A")` |
| `date(ts)` | Extract date part | `date(orders.created_at)` |
| `year(ts)` | Extract year | `year(orders.created_at)` |
| `month(ts)` | Extract month | `month(orders.created_at)` |
| `quarter(ts)` | Extract quarter | `quarter(orders.created_at)` |
| `now()` | Current timestamp | `now()` |

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

### Environment Variables

```smql
env("VAR_NAME")           // required — error if missing
env("VAR_NAME", "default") // optional with fallback
```

---

## Graph References

Graph references allow a pipeline to automatically discover and migrate all FK-dependent tables from the source, without declaring each as a separate pipeline. The primary `table` in `from` becomes the entry point for FK graph traversal.

### Single Table vs Graph Pipeline

```smql
// Single table — table in both from and to
from { table = "orders" }
to   { table = "orders_copy" }

// Graph pipeline — table only in from; to uses map for renaming
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

Example: `where` filters to orders 1, 2, 3 → only users referenced by those orders are copied → only regions referenced by those users are copied.

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

E-Commerce pipeline showing all features:

```smql
// ================================================================
// Configuration
// ================================================================

define {
  tax_rate      = 1.4
  cutoff_date   = "2024-01-01"
  active_status = "active"
}

// ================================================================
// Connections
// ================================================================

connection "mysql_prod" {
  driver = "mysql"
  url    = env("source_db")
  pool { max_size = 20 }
}

connection "postgres_warehouse" {
  driver = "postgres"
  url    = env("dest_db")
  pool { max_size = 50 }
}

// ================================================================
// Reusable transforms
// ================================================================

transform "normalize_email" {
  input  = string
  output = lower(trim(input))
}

// ================================================================
// Dimensions (load first — no dependencies)
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
    customer_email  = transform.normalize_email(customers.email)
    customer_segment = customers.segment
    created_at      = customers.created_at
  }

  validate {
    assert "valid_email" {
      check   = customer_email matches "^[^@]+@[^@]+\.[^@]+$"
      message = "Invalid email format"
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
    and orders.total > 0
    and orders.created_at >= define.cutoff_date
  }

  // Join dimension tables
  with {
    customers from dim_customers where customers.customer_key == orders.user_id
    products  from dim_products  where products.product_key  == order_items.product_id
    regions   from dim_regions   where regions.region_key    == orders.region_id
  }

  select {
    // Keys
    order_key     = orders.id
    customer_key  = orders.user_id
    product_key   = order_items.product_id
    region_key    = orders.region_id

    // Customer dimensions
    customer_name    = customers.customer_name
    customer_email   = customers.customer_email
    customer_segment = customers.customer_segment

    // Product dimensions
    product_name = products.product_name
    category     = products.category
    list_price   = products.price

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
      check   = customers.customer_key is not null
      message = "Customer not found in dimension"
    }
  }

  on_error {
    retry {
      max_attempts = 3
      backoff      = "5s"
    }
    failed_rows {
      table = "fact_orders_errors"
    }
  }

  paginate {
    using      = "timestamp"
    column     = orders.updated_at
    tiebreaker = orders.id
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
    batch_size = env.batch_size
    workers    = 8
    checkpoint = every_batch
  }
}
```
