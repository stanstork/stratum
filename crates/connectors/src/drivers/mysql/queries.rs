/// Count rows with optional filter clause
/// Placeholders: {table} - escaped table identifier, {filter} - WHERE clause or empty
pub const COUNT: &str = "SELECT COUNT(*) AS count FROM {table} {filter}";

/// Count rows without filter
/// Placeholder: {table} - escaped table identifier
pub const COUNT_NO_FILTER: &str = "SELECT COUNT(*) AS count FROM {table}";

/// Fast row count estimate using information_schema (MySQL specific)
pub const COUNT_ROWS_FAST: &str = include_str!("sql/count_rows_fast.sql");

pub const TABLE_EXISTS_SQL: &str = include_str!("sql/table_exists.sql");
pub const LIST_TABLES_SQL: &str = "SHOW TABLES";
pub const TABLE_METADATA_SQL: &str = include_str!("sql/table_metadata.sql");
pub const INDEX_METADATA_SQL: &str = include_str!("sql/index_metadata.sql");
pub const FK_METADATA_SQL: &str = include_str!("sql/fk_metadata.sql");
pub const REFERRING_TABLES_SQL: &str = include_str!("sql/table_referencing.sql");
pub const TABLE_SIZE_SQL: &str = include_str!("sql/table_size.sql");
pub const UNIQUE_CONSTRAINT_METADATA_SQL: &str = include_str!("sql/unique_constraint_metadata.sql");
pub const CHECK_CONSTRAINT_METADATA_SQL: &str = include_str!("sql/check_constraint_metadata.sql");

/// Escape a MySQL identifier (table name, column name, etc.) to prevent SQL injection.
/// Wraps the identifier in backticks and escapes any internal backticks by doubling them.
pub fn escape_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Build a fully qualified table name with proper escaping.
/// Returns `schema`.`table` if schema is provided, otherwise just `table`.
pub fn qualified_table_name(table: &str, schema: Option<&str>) -> String {
    match schema {
        Some(s) => format!("{}.{}", escape_identifier(s), escape_identifier(table)),
        None => escape_identifier(table),
    }
}
