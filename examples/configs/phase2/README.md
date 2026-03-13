# Phase 2 Integration Test Configs

Manual test cases for Phase 2 schema migration. All configs use Sakila MySQL as source
and a clean PostgreSQL database as destination.

## Prerequisites

```bash
# Source: Sakila MySQL
mysql -u root -e "CREATE USER 'sakila_user'@'%' IDENTIFIED BY 'qwerty123';"
mysql -u root -e "GRANT ALL ON sakila.* TO 'sakila_user'@'%';"
# Load Sakila: https://dev.mysql.com/doc/sakila/en/sakila-installation.html

# Destination: empty PostgreSQL database
psql -c "CREATE DATABASE testdb;"
```

Override connection URLs:
```bash
export MYSQL_URL="mysql://sakila_user:qwerty123@localhost:3306/sakila"
export POSTGRES_URL="postgres://user:password@localhost:5432/testdb"
```

## Test Matrix

| File | Feature | Mode | Expect |
|------|---------|------|--------|
| `p2-01-schema-only.smql` | with references, schema only | schema_only | All FK-reachable tables created, 0 rows |
| `p2-02-cascade-data.smql` | Cascade data, two-phase fetch | cascade, depth=1 | Related rows scoped to FK values of each primary batch |
| `p2-03-full-chain.smql` | Full FK graph, all depths | cascade, depth=all | All 14+ tables, correct topo order |
| `p2-04-depth-limit.smql` | Depth limiting | schema_only, depth=1 | Only direct FK deps, no transitive |
| `p2-05-exclusion.smql` | Exclude patterns | schema_only | Named tables absent from destination |
| `p2-06-circular-fk.smql` | store ↔ staff circular FK | cascade | Both tables created, FKs added post-data |
| `p2-07-schema-dedup.smql` | Multi-pipeline op dedup | schema_only | Shared tables created once, skip logs visible |
| `p2-08-enum-migration.smql` | MySQL ENUM → PG TYPE | cascade | CREATE TYPE before CREATE TABLE, values preserved |
| `p2-09-generated-columns.smql` | Generated column DDL | cascade | GENERATED ALWAYS AS in DDL, absent from INSERT |
| `p2-10-table-rename.smql` | map block table renaming | cascade | Destination tables use dim_* names, FKs updated |
| `p2-11-full-sakila.smql` | Complete Sakila migration | cascade | All 16 tables, row counts match, FK integrity holds |
| `p2-12-indexes.smql` | Index migration | cascade | Indexes created post-data, FULLTEXT noted |

## Reset destination between tests

```sql
-- Drop all tables in destination before each test
DO $$ DECLARE r RECORD;
BEGIN
  FOR r IN SELECT tablename FROM pg_tables WHERE schemaname = 'public' LOOP
    EXECUTE 'DROP TABLE IF EXISTS ' || r.tablename || ' CASCADE';
  END LOOP;
END $$;

-- Also drop custom types
DO $$ DECLARE r RECORD;
BEGIN
  FOR r IN SELECT typname FROM pg_type
           JOIN pg_namespace ON pg_type.typnamespace = pg_namespace.oid
           WHERE nspname = 'public' AND typtype = 'e' LOOP
    EXECUTE 'DROP TYPE IF EXISTS ' || r.typname || ' CASCADE';
  END LOOP;
END $$;
```

## Test 09 setup (generated columns)

```sql
-- Run on MySQL source before p2-09:
ALTER TABLE film
ADD COLUMN title_length INT
GENERATED ALWAYS AS (CHAR_LENGTH(title)) STORED;

ALTER TABLE film
ADD COLUMN rental_revenue DECIMAL(10,2)
GENERATED ALWAYS AS (rental_rate * rental_duration) STORED;
```
