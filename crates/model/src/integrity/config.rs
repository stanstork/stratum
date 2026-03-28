use crate::integrity::algorithm::HashAlgorithm;
use std::collections::HashMap;

/// Configuration for integrity verification on the write path.
#[derive(Debug, Clone)]
pub struct IntegrityConfig {
    pub algorithm: HashAlgorithm,
    /// table_name -> sorted destination column names.
    pub tables: HashMap<String, Vec<String>>,
    /// The primary (root) destination table for this pipeline.
    pub primary_table: String,
    /// When true, every individual row hash is stored in the receipt alongside
    /// the batch-level Merkle roots.
    pub store_row_hashes: bool,
    /// Destination column data types.
    /// Used to apply the same coercions at hash time as the COPY writer applies
    /// before writing - e.g., String("a,b") -> Array([String("a"), String("b")])
    /// for TEXT[] columns, so migration and verify hashes match.
    pub column_types: HashMap<String, HashMap<String, String>>,
}

impl IntegrityConfig {
    pub fn new(
        algorithm: HashAlgorithm,
        tables: HashMap<String, Vec<String>>,
        primary_table: impl Into<String>,
    ) -> Self {
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
            primary_table: primary_table.into(),
            column_types: HashMap::new(),
            store_row_hashes: false,
        }
    }

    /// Set destination column types: table_name -> column_name -> pg_type_string.
    pub fn with_column_types(
        mut self,
        column_types: HashMap<String, HashMap<String, String>>,
    ) -> Self {
        self.column_types = column_types;
        self
    }

    pub fn single_table(
        algorithm: HashAlgorithm,
        table_name: impl Into<String>,
        column_order: Vec<String>,
    ) -> Self {
        let name = table_name.into();
        let mut tables = HashMap::new();
        tables.insert(name.clone(), column_order);
        Self::new(algorithm, tables, name)
    }

    pub fn sha256_single(table_name: impl Into<String>, column_order: Vec<String>) -> Self {
        Self::single_table(HashAlgorithm::Sha256, table_name, column_order)
    }

    pub fn with_store_row_hashes(mut self, enabled: bool) -> Self {
        self.store_row_hashes = enabled;
        self
    }
}
