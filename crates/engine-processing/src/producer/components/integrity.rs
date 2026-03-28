use crate::error::ProducerError;
use engine_state::MerkleStore;
use model::{
    core::value::{FieldValue, Value},
    integrity::{
        config::IntegrityConfig, hasher::RowHasher, merkle::MerkleTree,
        receipt::VerificationReceipt,
    },
    records::Record,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

pub struct IntegrityState {
    /// One hasher per destination table, keyed by table name.
    hashers: HashMap<String, RowHasher>,
    merkle_store: Arc<dyn MerkleStore>,
    /// Subtree roots per batch in insertion order - primary table only.
    batch_roots: HashMap<String, Vec<[u8; 32]>>,
    /// Row counts per batch - primary table only.
    rows_per_batch: HashMap<String, Vec<u64>>,
    /// Individual row hashes - primary table only, populated when `store_row_hashes`.
    row_hashes: HashMap<String, Vec<[u8; 32]>>,
    /// Accumulated unique row hashes for cascade (non-primary) tables.
    cascade_hashes: HashMap<String, HashSet<[u8; 32]>>,
    config: IntegrityConfig,
}

impl IntegrityState {
    pub fn new(config: IntegrityConfig, merkle_store: Arc<dyn MerkleStore>) -> Self {
        let hashers = config
            .tables
            .iter()
            .map(|(table, cols)| {
                (
                    table.clone(),
                    RowHasher::new(cols.clone(), config.algorithm),
                )
            })
            .collect();
        Self {
            hashers,
            merkle_store,
            batch_roots: HashMap::new(),
            rows_per_batch: HashMap::new(),
            row_hashes: HashMap::new(),
            cascade_hashes: HashMap::new(),
            config,
        }
    }

    /// Hash all rows in `rows`, grouped by destination table.
    /// Primary table rows are batched (insertion order); cascade table rows are
    /// deduplicated and accumulated for a single sorted Merkle root at finalization.
    pub fn hash_batch(&mut self, rows: &[Record]) {
        // Group rows by destination table name.
        let mut groups: HashMap<&str, Vec<&Record>> = HashMap::new();
        for row in rows {
            groups.entry(row.schema.as_str()).or_default().push(row);
        }

        for (table, table_rows) in &groups {
            let key = self.resolve_table_key(table).to_string();
            let is_primary = key == self.config.primary_table;

            if is_primary {
                self.process_primary_table(key, table_rows);
            } else {
                self.process_cascade_table(key, table_rows);
            }
        }
    }

    /// Build per-table Merkle receipts and persist them to the store.
    /// `skipped_rows` is the coordinator-level skip counter for the primary receipt.
    pub async fn save_receipts(
        &self,
        pipeline_name: &str,
        run_id: String,
        skipped_rows: u64,
    ) -> Result<(), ProducerError> {
        // Primary table: one receipt with per-batch roots.
        self.save_primary_tables(pipeline_name, run_id.clone(), skipped_rows)
            .await?;

        // Cascade tables: one receipt per table with a single sorted Merkle root.
        self.save_cascade_tables(pipeline_name, run_id).await?;

        Ok(())
    }

    /// Primary table: batch-based hashing (rows arrive in order
    /// from the offset strategy, so batches align with verify's reads).
    fn process_primary_table(&mut self, key: String, rows: &[&Record]) {
        let empty_map = HashMap::new();
        let col_types = self.config.column_types.get(&key).unwrap_or(&empty_map);
        let hasher = self.hashers.get_mut(&key).unwrap();

        let row_hashes: Vec<[u8; 32]> = rows
            .iter()
            .map(|r| hash_row_coerced(hasher, r, col_types))
            .collect();
        let subtree_root = MerkleTree::root_from_hashes(&row_hashes, self.config.algorithm);
        self.batch_roots
            .entry(key.clone())
            .or_default()
            .push(subtree_root);
        self.rows_per_batch
            .entry(key.clone())
            .or_default()
            .push(rows.len() as u64);
        if self.config.store_row_hashes {
            self.row_hashes
                .entry(key)
                .or_default()
                .extend_from_slice(&row_hashes);
        }
    }

    /// Cascade table: accumulate unique row hashes. The same row may be
    /// referenced by multiple source batches.
    fn process_cascade_table(&mut self, key: String, rows: &[&Record]) {
        let empty_map = HashMap::new();
        let col_types = self.config.column_types.get(&key).unwrap_or(&empty_map);
        let hasher = self.hashers.get_mut(&key).unwrap();

        let set = self.cascade_hashes.entry(key.clone()).or_default();
        for row in rows {
            set.insert(hash_row_coerced(hasher, row, col_types));
        }
    }

    async fn save_primary_tables(
        &self,
        pipeline_name: &str,
        run_id: String,
        skipped_rows: u64,
    ) -> Result<(), ProducerError> {
        for (table_name, batch_roots) in &self.batch_roots {
            let table_root = MerkleTree::root_from_hashes(batch_roots, self.config.algorithm);
            let column_order = self
                .config
                .tables
                .get(table_name)
                .cloned()
                .unwrap_or_default();
            let rows_per_batch = self
                .rows_per_batch
                .get(table_name)
                .cloned()
                .unwrap_or_default();
            let total_rows: u64 = rows_per_batch.iter().sum();
            let stored_row_hashes = if self.config.store_row_hashes {
                self.row_hashes.get(table_name).cloned()
            } else {
                None
            };

            let receipt = VerificationReceipt {
                run_id: run_id.clone(),
                pipeline_name: pipeline_name.to_string(),
                table_name: table_name.clone(),
                table_root,
                batch_roots: batch_roots.clone(),
                column_order,
                total_rows,
                skipped_rows,
                rows_per_batch,
                sorted_hashes: false,
                algorithm: self.config.algorithm,
                created_at: chrono::Utc::now(),
                row_hashes: stored_row_hashes,
            };
            self.merkle_store.save_receipt(&receipt).await?;
        }
        Ok(())
    }

    async fn save_cascade_tables(
        &self,
        pipeline_name: &str,
        run_id: String,
    ) -> Result<(), ProducerError> {
        for (table_name, hash_set) in &self.cascade_hashes {
            let mut sorted_hashes: Vec<[u8; 32]> = hash_set.iter().copied().collect();
            sorted_hashes.sort_unstable();
            let total_rows = sorted_hashes.len() as u64;
            let table_root = MerkleTree::root_from_hashes(&sorted_hashes, self.config.algorithm);
            let column_order = self
                .config
                .tables
                .get(table_name)
                .cloned()
                .unwrap_or_default();

            let receipt = VerificationReceipt {
                run_id: run_id.clone(),
                pipeline_name: pipeline_name.to_string(),
                table_name: table_name.clone(),
                table_root,
                batch_roots: vec![table_root],
                column_order,
                total_rows,
                skipped_rows: 0,
                rows_per_batch: vec![total_rows],
                sorted_hashes: true,
                algorithm: self.config.algorithm,
                created_at: chrono::Utc::now(),
                row_hashes: None,
            };
            self.merkle_store.save_receipt(&receipt).await?;
        }
        Ok(())
    }

    /// Unknown tables fall back to the first registered hasher
    fn resolve_table_key<'a>(&'a self, table: &'a str) -> &'a str {
        if self.hashers.contains_key(table) {
            table
        } else {
            self.hashers
                .keys()
                .next()
                .expect("IntegrityConfig must have at least one table")
                .as_str()
        }
    }
}

/// Hash a row, applying column-type coercions when `col_types` is non-empty.
fn hash_row_coerced(
    hasher: &mut RowHasher,
    row: &Record,
    col_types: &HashMap<String, String>,
) -> [u8; 32] {
    if col_types.is_empty() {
        hasher.hash_row(row)
    } else {
        hasher.hash_row(&coerce_row_for_hash(row, col_types))
    }
}

/// Apply the same coercions to row values at hash time as the COPY writer applies
/// before writing - so that migration hashes match verify hashes.
fn coerce_row_for_hash(row: &Record, col_types: &HashMap<String, String>) -> Record {
    let fields: Vec<FieldValue> = row
        .fields
        .iter()
        .map(|fv| {
            let coerced = fv.value.as_ref().map(|value| {
                let pg_type = col_types.get(&fv.name).map(|s| s.as_str()).unwrap_or("");
                coerce_value_for_hash(value.clone(), pg_type)
            });
            FieldValue {
                name: fv.name.clone(),
                value: coerced,
                data_type: fv.data_type.clone(),
            }
        })
        .collect();
    Record {
        schema: row.schema.clone(),
        fields,
    }
}

/// Coerce a single value to match what the COPY writer would write and PG would store.
/// TODO: make this more robust and configurable - handle more types, handle nested structures, etc.
fn coerce_value_for_hash(value: Value, pg_type: &str) -> Value {
    let pg_type_lc = pg_type.to_lowercase();
    if (pg_type_lc.ends_with("[]") || pg_type_lc.contains("array") || pg_type_lc == "set")
        && let Value::String(s) = &value
    {
        let elements: Vec<Value> = s
            .split(',')
            .map(|item| Value::String(item.trim_matches('"').trim_matches('\'').to_string()))
            .collect();
        return Value::Array(elements);
    }
    value
}
