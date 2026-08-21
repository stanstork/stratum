use crate::integrity::algorithm::HashAlgorithm;
use std::{collections::HashMap, sync::OnceLock};

/// Configuration for integrity verification on the write path.
#[derive(Debug, Clone)]
pub struct IntegrityConfig {
    pub algorithm: HashAlgorithm,
    /// table_name -> sorted destination column names.
    pub tables: HashMap<String, Vec<String>>,
    /// table_name -> destination primary-key columns, in table order. A table
    /// missing here (or with an empty list) has no key, and its row hashes act
    /// as their own keys.
    pub key_columns: HashMap<String, Vec<String>>,
    /// Destination column data types (in the destination's own dialect).
    /// Used to apply the same coercions at hash time that the destination applies
    /// on write - e.g., String("a,b") -> Array([String("a"), String("b")]) for an
    /// array/set column - so migration and verify hashes match.
    pub column_types: HashMap<String, HashMap<String, String>>,
}

impl IntegrityConfig {
    pub fn new(algorithm: HashAlgorithm, tables: HashMap<String, Vec<String>>) -> Self {
        let tables = tables
            .into_iter()
            .map(|(table, mut cols)| {
                cols.sort();
                (table, cols)
            })
            .collect();
        Self {
            algorithm,
            tables,
            key_columns: HashMap::new(),
            column_types: HashMap::new(),
        }
    }

    /// Set destination column types: table_name -> column_name -> type_string.
    pub fn with_column_types(
        mut self,
        column_types: HashMap<String, HashMap<String, String>>,
    ) -> Self {
        self.column_types = column_types;
        self
    }

    /// Set the primary-key columns per table.
    pub fn with_key_columns(mut self, key_columns: HashMap<String, Vec<String>>) -> Self {
        self.key_columns = key_columns;
        self
    }

    /// Hashed column order for `table` (empty if the table is not tracked).
    pub fn columns(&self, table: &str) -> &[String] {
        self.tables.get(table).map(|c| c.as_slice()).unwrap_or(&[])
    }

    /// Key columns for `table`; empty means "no primary key".
    pub fn keys(&self, table: &str) -> &[String] {
        self.key_columns
            .get(table)
            .map(|c| c.as_slice())
            .unwrap_or(&[])
    }

    /// Destination column types for `table`; empty means "no coercion needed".
    pub fn types(&self, table: &str) -> &HashMap<String, String> {
        static EMPTY: OnceLock<HashMap<String, String>> = OnceLock::new();
        self.column_types
            .get(table)
            .unwrap_or_else(|| EMPTY.get_or_init(HashMap::new))
    }
}
