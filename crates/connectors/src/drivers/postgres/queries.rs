/// Count rows with optional filter clause
/// Placeholders: {table} - escaped table identifier, {filter} - WHERE clause or empty
pub const COUNT: &str = "SELECT COUNT(*) AS count FROM {table} {filter}";

/// Count rows without filter
/// Placeholder: {table} - escaped table identifier
pub const COUNT_NO_FILTER: &str = "SELECT COUNT(*) AS count FROM {table}";

/// Count rows with fast estimate using pg_class statistics
pub const COUNT_ROWS_FAST: &str = include_str!("sql/count_rows_fast.sql");

pub const TABLE_EXISTS_SQL: &str = include_str!("sql/table_exists.sql");

// Lists the *logical* tables of a schema: ordinary tables ('r') and partitioned
// parents ('p'). Individual partitions are an implementation detail - reading the
// parent returns every partition's rows - so `relispartition` children are hidden.
pub const LIST_TABLES_SQL: &str = "\
SELECT c.relname \
FROM pg_class c \
JOIN pg_namespace n ON n.oid = c.relnamespace \
WHERE n.nspname = $1::text \
  AND c.relkind IN ('r', 'p') \
  AND NOT c.relispartition \
ORDER BY c.relname";

pub const TABLE_METADATA_SQL: &str = include_str!("sql/table_metadata.sql");
pub const INDEX_METADATA_SQL: &str = include_str!("sql/index_metadata.sql");
pub const FK_METADATA_SQL: &str = include_str!("sql/fk_metadata.sql");
pub const REFERRING_TABLES_SQL: &str = include_str!("sql/table_referencing.sql");
pub const TABLE_SIZE_SQL: &str = "SELECT pg_total_relation_size($1::text::regclass) AS size_bytes;";
pub const UNIQUE_CONSTRAINT_METADATA_SQL: &str = include_str!("sql/unique_constraint_metadata.sql");
pub const CHECK_CONSTRAINT_METADATA_SQL: &str = include_str!("sql/check_constraint_metadata.sql");

/// Escape a PostgreSQL identifier (table name, column name, etc.) to prevent SQL injection.
/// Wraps the identifier in double quotes and escapes any internal quotes by doubling them.
pub fn escape_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Build a fully qualified table name with proper escaping.
/// Returns "schema"."table" if schema is provided, otherwise just "table".
pub fn qualified_table_name(table: &str, schema: Option<&str>) -> String {
    match schema {
        Some(s) => format!("{}.{}", escape_identifier(s), escape_identifier(table)),
        None => escape_identifier(table),
    }
}
