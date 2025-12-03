# SMQL Reference (v2.0)

SMQL (Stratum Migration Query Language) is a declarative language for defining data migrations with powerful inline clauses and rich source definitions.

## Table of Contents

- [Overview](#overview)
- [Top-Level Statements](#top-level-statements)
  - [CONNECTIONS](#connections)
  - [MIGRATE](#migrate)
- [Inline Clauses](#inline-clauses)
  - [SETTINGS](#settings)
  - [FILTER](#filter)
  - [LOAD / MATCH](#load--match)
  - [MAP](#map)
  - [OFFSET](#offset)
- [Complete Example](#complete-example)
- [Best Practices](#best-practices)

---

## Overview

SMQL v2.0 introduces inline clauses and richer source definitions, letting you express per-mapping settings, filters, loads, and maps in a single MIGRATE block.

**Key Features:**
- Inline `[...]` blocks under each `SOURCE → DEST` mapping
- Support for plural sources via `SOURCES(...)`
- Explicit `TABLE` / `API` / `FILE` source and destination types
- Powerful clauses for `FILTER`, `LOAD` (with `MATCH`), and `MAP` inside each mapping
- Top-level `WITH SETTINGS` for global defaults

---

## Top-Level Statements

### CONNECTIONS

Defines your source and destination endpoints.

**Syntax:**
```smql
CONNECTIONS (
    SOURCE(MYSQL, "mysql://user:pass@localhost:3306/db"),
    DESTINATION(POSTGRES, "postgres://user:pass@localhost:5432/db")
);
```

**Supported Database Types:**
- `MYSQL`
- `POSTGRES`

**Connection String Format:**
- Standard database connection URIs
- Must include: protocol, credentials, host, port, database name

**Examples:**

MySQL:
```smql
SOURCE(MYSQL, "mysql://root:secret@localhost:3306/myapp")
```

PostgreSQL:
```smql
DESTINATION(POSTGRES, "postgres://user:pass@db.example.com:5432/warehouse")
```

---

### MIGRATE

The core of your migration plan. Each `SOURCE → DEST` mapping can have its own inline clauses.

**Syntax:**
```smql
MIGRATE (
  SOURCE(TABLE, orders) -> DEST(TABLE, orders_flat) [
    // Inline clauses here
  ],
  
  SOURCES(TABLE, [a, b, c]) -> DEST(TABLE, combined) [
    // Inline clauses here
  ],
  
  SOURCE(API, "https://api.example.com/data") -> DEST(FILE, "/tmp/out.json") [
    // Inline clauses here
  ]
)
WITH SETTINGS (
  CREATE_MISSING_TABLES = TRUE,
  BATCH_SIZE = 1000
);
```

**Source Types:**

| Type | Description | Example |
|------|-------------|---------|
| `SOURCE(TABLE, name)` | Single database table | `SOURCE(TABLE, users)` |
| `SOURCES(TABLE, [list])` | Multiple tables (union) | `SOURCES(TABLE, [users_a, users_b])` |
| `SOURCE(API, uri)` | REST API endpoint | `SOURCE(API, "https://...")` |
| `SOURCE(FILE, path)` | File source | `SOURCE(FILE, "/data/input.csv")` |

**Destination Types:**

| Type | Description | Example |
|------|-------------|---------|
| `DEST(TABLE, name)` | Database table | `DEST(TABLE, customers)` |
| `DEST(FILE, path)` | Output file | `DEST(FILE, "/tmp/export.json")` |

**Global Settings:**

The `WITH SETTINGS` block applies defaults to all mappings unless overridden inline.

```smql
WITH SETTINGS (
  CREATE_MISSING_TABLES = TRUE,
  BATCH_SIZE = 1000,
  WORKERS = 4,
  CHECKPOINT_EVERY = 1
)
```

---

## Inline Clauses

Each mapping can include any subset of these clauses inside `[...]` blocks, in any order.

### SETTINGS

Mapping-specific overrides for behavior and schema management.

**Syntax:**
```smql
SETTINGS(
  INFER_SCHEMA = TRUE,
  IGNORE_CONSTRAINTS = FALSE,
  CREATE_MISSING_COLUMNS = TRUE,
  COPY_COLUMNS = MAP_ONLY
)
```

**Available Settings:**

| Name | Type | Default | Description |
|------|------|---------|-------------|
| `INFER_SCHEMA` | Boolean | `FALSE` | Emit DDL for this mapping's tables & foreign keys |
| `IGNORE_CONSTRAINTS` | Boolean | `FALSE` | Don't validate foreign keys at runtime |
| `CREATE_MISSING_COLUMNS` | Boolean | `FALSE` | Auto-add new columns in target if missing |
| `CREATE_MISSING_TABLES` | Boolean | `FALSE` | Auto-create new target tables if missing |
| `COPY_COLUMNS` | Enum | `ALL` | Which source columns to copy: `ALL`, `MAP_ONLY` |

**Examples:**

Only copy explicitly mapped columns:
```smql
SETTINGS(
  COPY_COLUMNS = MAP_ONLY
)
```

Auto-create missing schema:
```smql
SETTINGS(
  INFER_SCHEMA = TRUE
)
```

Disable constraint validation for performance:
```smql
SETTINGS(
  IGNORE_CONSTRAINTS = TRUE
)
```

---

### FILTER

Row-level predicates with support for complex boolean logic.

**Syntax:**
```smql
FILTER(
  AND(
    orders[status] = "active",
    orders[total] > 400,
    users[id] < 4
  )
)
```

**Lookup Syntax:**
- `table[column]` - Reference a column from a table
- Works with source table and any loaded tables (see LOAD)

**Comparison Operators:**
- `=` - Equals
- `!=` - Not equals
- `>` - Greater than
- `<` - Less than
- `>=` - Greater than or equal
- `<=` - Less than or equal

**Logical Functions:**
- `AND(condition1, condition2, ...)` - All conditions must be true
- `OR(condition1, condition2, ...)` - At least one condition must be true
- `NOT(condition)` - Negates a condition

**Examples:**

Simple filter:
```smql
FILTER(
  orders[status] = "active"
)
```

Complex filter with AND:
```smql
FILTER(
  AND(
    orders[status] = "active",
    orders[total] > 1000,
    orders[created_at] >= "2024-01-01"
  )
)
```

OR condition:
```smql
FILTER(
  OR(
    orders[status] = "pending",
    orders[status] = "processing",
    orders[status] = "shipped"
  )
)
```

Nested logic:
```smql
FILTER(
  AND(
    OR(
      orders[status] = "active",
      orders[status] = "pending"
    ),
    NOT(orders[flagged] = TRUE),
    orders[total] > 100
  )
)
```

---

### LOAD / MATCH

Join additional tables for lookup or filtering. This enables denormalization and complex transformations.

**Syntax:**
```smql
LOAD(
  TABLES(users, order_items, products),
  MATCH(
    ON(users[id] -> orders[user_id]),
    ON(order_items[order_id] -> orders[id]),
    ON(products[product_id] -> order_items[id])
  )
)
```

**Components:**

**TABLES(table1, table2, ...)**
- Lists tables to join into this mapping
- Tables must exist in the source database
- All listed tables become available in FILTER and MAP clauses

**MATCH(...)**
- Defines join conditions (foreign key relationships)
- Syntax: `ON(source_table[column] -> target_table[column])`
- Multiple `ON(...)` clauses for multi-table joins

**Join Scope:**
- Joins apply only to this specific mapping (not global)
- Creates a denormalized result set for the destination

**Examples:**

Simple lookup (users):
```smql
LOAD(
  TABLES(users),
  MATCH(
    ON(users[id] -> orders[user_id])
  )
)
```

Multi-table join:
```smql
LOAD(
  TABLES(customers, products, categories),
  MATCH(
    ON(customers[id] -> orders[customer_id]),
    ON(products[id] -> order_items[product_id]),
    ON(categories[id] -> products[category_id])
  )
)
```

Star schema denormalization:
```smql
LOAD(
  TABLES(dim_date, dim_customer, dim_product, dim_region),
  MATCH(
    ON(dim_date[date_key] -> fact_sales[date_key]),
    ON(dim_customer[customer_key] -> fact_sales[customer_key]),
    ON(dim_product[product_key] -> fact_sales[product_key]),
    ON(dim_region[region_key] -> fact_sales[region_key])
  )
)
```

---

### MAP

Project or compute output columns with expressions and functions.

**Syntax:**
```smql
MAP(
  users[name] -> user_name,
  order_items[price] -> order_price,
  order_items[price] * 1.4 -> price_with_tax,
  CONCAT(users[name], products[name]) -> customer_product
)
```

**Components:**
- **Left side:** Any expression (lookup, arithmetic, function call)
- **Right side:** Target column name (destination)
- **Arrow operator:** `->` separates source expression from destination

**Supported Operations:**

**Column Lookups:**
```smql
MAP(
  users[email] -> email,
  orders[total] -> order_total
)
```

**Arithmetic:**
```smql
MAP(
  products[price] * 1.2 -> price_with_markup,
  orders[subtotal] + orders[tax] -> total,
  inventory[quantity] - sales[sold] -> remaining
)
```

**String Functions:**
```smql
MAP(
  CONCAT(users[first_name], " ", users[last_name]) -> full_name,
  UPPER(users[email]) -> email_upper,
  LOWER(products[sku]) -> sku_lower
)
```

---

### OFFSET

Control pagination strategy for reading source data.

**Syntax:**
```smql
OFFSET(
  STRATEGY -> pk | numeric | timestamp,
  CURSOR -> <column_name>,
  TIEBREAKER -> <pk_column_name>,
  TIMEZONE -> <IANA_TZ>
)
```

**Parameters:**

| Parameter | Required | Description |
|-----------|----------|-------------|
| `STRATEGY` | Yes | Pagination strategy: `pk`, `numeric`, `timestamp` |
| `CURSOR` | Conditional | Column to use for pagination (required for numeric/timestamp, optional for pk) |
| `TIEBREAKER` | Conditional | Primary key column for stable sorting (required when cursor may be non-unique) |
| `TIMEZONE` | No | IANA timezone for timestamp strategy (default: `UTC`) |

**Strategies:**

#### 1. Primary Key (pk)

Best for: Most common case, stable pagination with auto-increment IDs

```smql
OFFSET(
  STRATEGY -> pk,
  CURSOR -> id
)
```

Query generated:
```sql
WHERE id > :last_cursor
ORDER BY id
LIMIT :batch_size
```

**Defaults:**
- `CURSOR` defaults to `id` if not specified
- No tiebreaker needed (PKs are unique)

#### 2. Numeric Column (numeric)

Best for: Paginating by numeric columns that aren't primary keys

```smql
OFFSET(
  STRATEGY -> numeric,
  CURSOR -> event_id,
  TIEBREAKER -> id
)
```

Query generated:
```sql
WHERE (event_id > :last_cursor)
   OR (event_id = :last_cursor AND id > :last_id)
ORDER BY event_id, id
LIMIT :batch_size
```

**Requirements:**
- `CURSOR` is required
- `TIEBREAKER` is required (defaults to `id`)
- Tiebreaker ensures stable ordering when cursor values are duplicated

#### 3. Timestamp (timestamp)

Best for: Incremental updates, CDC-like patterns

```smql
OFFSET(
  STRATEGY -> timestamp,
  CURSOR -> updated_at,
  TIEBREAKER -> id,
  TIMEZONE -> America/New_York
)
```

Query generated:
```sql
WHERE (updated_at > :last_cursor)
   OR (updated_at = :last_cursor AND id > :last_id)
ORDER BY updated_at, id
LIMIT :batch_size
```

**Requirements:**
- `CURSOR` is required (timestamp column)
- `TIEBREAKER` is required (defaults to `id`)
- `TIMEZONE` defaults to `UTC`

**Examples:**

Explicit PK column:
```smql
OFFSET(STRATEGY -> pk, CURSOR -> user_id)
```

Numeric with tiebreaker:
```smql
OFFSET(STRATEGY -> numeric, CURSOR -> sequence_num, TIEBREAKER -> id)
```

Timestamp for incremental sync:
```smql
OFFSET(STRATEGY -> timestamp, CURSOR -> updated_at, TIEBREAKER -> id, TIMEZONE -> UTC)
```

---

## Complete Example

Here's a comprehensive migration demonstrating all features:

```smql
CONNECTIONS (
  SOURCE(MYSQL, "mysql://user:pass@localhost:3306/testdb"),
  DESTINATION(POSTGRES, "postgres://user:pass@localhost:5432/testdb")
);

MIGRATE (
  // Complex denormalized mapping with filters and transforms
  SOURCE(TABLE, orders) -> DEST(TABLE, orders_flat) [
    SETTINGS(
      INFER_SCHEMA = TRUE,
      IGNORE_CONSTRAINTS = FALSE,
      CREATE_MISSING_COLUMNS = TRUE,
      COPY_COLUMNS = MAP_ONLY
    ),
    
    FILTER(
      AND(
        orders[status] = "active",
        orders[total] > 400,
        users[id] < 4
      )
    ),
    
    LOAD(
      TABLES(users, order_items, products),
      MATCH(
        ON(users[id] -> orders[user_id]),
        ON(order_items[order_id] -> orders[id]),
        ON(products[product_id] -> order_items[id])
      )
    ),
    
    MAP(
      users[name] -> user_name,
      users[email] -> user_email,
      order_items[price] -> order_price,
      products[name] -> product_name,
      products[price] -> product_price,
      order_items[price] * 1.4 -> order_price_with_tax,
      CONCAT(users[name], products[name]) -> concat_lookup_test
    ),
    
    OFFSET(
      STRATEGY -> timestamp,
      CURSOR -> updated_at,
      TIEBREAKER -> id,
      TIMEZONE -> UTC
    )
  ],

  // Simple filtered copy
  SOURCE(TABLE, invoices) -> DEST(TABLE, statement) [
    SETTINGS(
      INFER_SCHEMA = FALSE,
      COPY_COLUMNS = ALL
    ),
    
    FILTER(
      invoices[date] >= "2024-01-01"
    ),
    
    OFFSET(
      STRATEGY -> pk,
      CURSOR -> id
    )
  ]
)
WITH SETTINGS (
  CREATE_MISSING_TABLES = TRUE,
  BATCH_SIZE = 1000
);
```

---

## Best Practices

### 1. Use MAP_ONLY for Denormalized Tables

When creating wide, denormalized tables:

```smql
SETTINGS(
  COPY_COLUMNS = MAP_ONLY  // Only include explicitly mapped columns
)
```

### 2. Enable Schema Inference for New Destinations

Let Stratum create tables automatically:

```smql
SETTINGS(
  INFER_SCHEMA = TRUE
)
```

### 3. Use Timestamp Strategy for Incremental Updates

For CDC-like patterns:

```smql
OFFSET(
  STRATEGY -> timestamp,
  CURSOR -> updated_at,
  TIEBREAKER -> id
)
```

### 4. Always Use Tiebreakers for Non-Unique Columns

When paginating by non-unique columns:

```smql
OFFSET(
  STRATEGY -> numeric,
  CURSOR -> priority,
  TIEBREAKER -> id  // Ensures stable ordering
)
```

### 5. Filter Early for Performance

Apply filters to reduce data before joins:

```smql
FILTER(
  orders[created_at] >= "2024-01-01"  // Filter before loading related tables
),
LOAD(...)
```

### 6. Use COALESCE for Fallback Values

Handle NULLs gracefully:

```smql
MAP(
  COALESCE(users[nickname], users[first_name], "Unknown") -> display_name
)
```

### 7. Test with Dry Run

Always validate before executing:

```bash
stratum validate migration.smql
```
