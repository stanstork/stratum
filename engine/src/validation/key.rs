use crate::report::finding::Finding;
use common::{row_data::RowData, value::Value};
use sql_adapter::{adapter::SqlAdapter, error::db::DbError, metadata::table::TableMetadata};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug)]
pub enum KeyCheckPolicy {
    /// No key checks at all.
    None,
    /// Only detect duplicate keys inside the current batch.
    IntraBatchOnly,
    /// Detect duplicates inside the batch AND conflicts in destination.
    /// `batch_size` controls DB lookup granularity.
    IntraBatchAndDestination { batch_size: usize },
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum KeyKind {
    Primary,                 // PRIMARY KEY(...)
    Unique(String /*name*/), // UNIQUE <name>(...)
}

// A collection of ordered values representing a composite key.
type KeyValue = Vec<Value>;

/// A helper struct dedicated to checking for primary and unique key violations.
pub struct KeyChecker {
    // intra-batch seen sets to catch duplicates before hitting DB
    seen: HashMap<(String, KeyKind), HashSet<KeyValue>>,
    // pending tuples to check against destination (batched)
    pending: HashMap<(String, KeyKind), Vec<KeyValue>>,
}

impl KeyChecker {
    pub fn new() -> Self {
        Self {
            seen: HashMap::new(),
            pending: HashMap::new(),
        }
    }

    /// Records all primary and unique keys from a row for later validation.
    pub fn record_row(
        &mut self,
        table: &str,
        meta: &TableMetadata,
        row: &RowData,
        policy: KeyCheckPolicy,
        findings: &mut HashSet<Finding>,
    ) {
        if matches!(policy, KeyCheckPolicy::None) {
            return;
        }

        // Process primary key
        if !meta.primary_keys.is_empty() {
            self.record_key(table, KeyKind::Primary, &meta.primary_keys, row, findings);
        }

        // Process Unique Constraints
        for col in meta.columns() {
            if col.is_unique {
                self.record_key(
                    table,
                    KeyKind::Unique(col.name.clone()),
                    &[col.name.clone()],
                    row,
                    findings,
                );
            }
        }
    }

    pub async fn check_pending(
        &mut self,
        _adapter: &(dyn SqlAdapter + Send + Sync),
        batch_size: usize,
        findings: &mut HashSet<Finding>,
    ) -> Result<(), DbError> {
        for ((table, kind), keys) in self.pending.drain() {
            for _key_batch in keys.chunks(batch_size) {
                let constraint_name = match &kind {
                    KeyKind::Primary => "PRIMARY".to_string(),
                    KeyKind::Unique(name) => name.clone(),
                };

                let existing_keys: Vec<String> = Vec::new(); // TODO: adapter.find_existing_keys(&table, &constraint_name, key_batch).await?;

                for existing_key in existing_keys {
                    findings.insert(Finding::error(
                        "SCHEMA_KEY_VIOLATION_IN_DB",
                        &format!(
                            "Key {:?} for constraint '{}' on table '{}' already exists in the destination.",
                            existing_key, constraint_name, table
                        ),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Records a single key, checks for in-batch duplicates, and queues for DB check.
    fn record_key(
        &mut self,
        table: &str,
        kind: KeyKind,
        cols: &[String],
        row: &RowData,
        findings: &mut HashSet<Finding>,
    ) {
        // Extract a sorted list of values for the key. If any part is NULL, we can't check it.
        let Some(key_value) = self.extract_key_value(row, cols) else {
            return;
        };

        // Intra-batch check: See if we've already processed this exact key in this batch.
        let seen_set = self
            .seen
            .entry((table.to_string(), kind.clone()))
            .or_default();
        if !seen_set.insert(key_value.clone()) {
            findings.insert(self.create_duplicate_finding(table, &kind, cols, &key_value));
        }

        // Add to the pending list for the eventual database check.
        self.pending
            .entry((table.to_string(), kind))
            .or_default()
            .push(key_value);
    }

    /// Helper to build a composite key value from a row for a given set of columns.
    fn extract_key_value(&self, row: &RowData, key_columns: &[String]) -> Option<KeyValue> {
        let mut key_value = Vec::new();
        let row_field_map: HashMap<_, _> = row
            .field_values
            .iter()
            .map(|f| (&f.name, &f.value))
            .collect();

        // Ensure consistent key order by sorting the column names.
        let mut sorted_cols = key_columns.to_vec();
        sorted_cols.sort();

        for col_name in &sorted_cols {
            match row_field_map.get(col_name) {
                Some(Some(val)) => key_value.push(val.clone()),
                // If any part of the key is NULL or the column is missing, the key is not considered unique.
                _ => return None,
            }
        }
        Some(key_value)
    }

    /// Creates a standardized finding for a duplicate key found within the sample batch.
    fn create_duplicate_finding(
        &self,
        table: &str,
        kind: &KeyKind,
        cols: &[String],
        key_value: &KeyValue,
    ) -> Finding {
        let (code, msg) = match kind {
            KeyKind::Primary => (
                "SCHEMA_PK_DUPLICATE_IN_BATCH",
                format!(
                    "Duplicate PRIMARY KEY {:?} found in sample for table '{}' (columns: {:?})",
                    key_value, table, cols
                ),
            ),
            KeyKind::Unique(name) => (
                "SCHEMA_UNIQUE_DUPLICATE_IN_BATCH",
                format!(
                    "Duplicate UNIQUE constraint '{}' key {:?} found in sample for table '{}' (columns: {:?})",
                    name, key_value, table, cols
                ),
            ),
        };
        Finding::error(code, &msg)
    }
}
