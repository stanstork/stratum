use crate::error::ProducerError;
use engine_state::MerkleStore;
use model::{
    integrity::{
        config::IntegrityConfig, hasher::RowHasher, merkle::MerkleTree,
        receipt::VerificationReceipt,
    },
    records::Record,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

/// Cross-lane accumulator of unique row hashes per table.
pub type LaneHashSink = Arc<Mutex<HashMap<String, HashSet<[u8; 32]>>>>;

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
    /// When set, this lane accumulates ALL tables (including the primary) as sorted hashes.
    lane_sink: Option<LaneHashSink>,
    config: IntegrityConfig,

    // Fast-path index lookups to avoid HashMaps and String allocations per batch
    table_names: Vec<String>,
    table_indices: HashMap<String, usize>,
}

impl IntegrityState {
    pub fn new(config: IntegrityConfig, merkle_store: Arc<dyn MerkleStore>) -> Self {
        let mut hashers = HashMap::with_capacity(config.tables.len());
        let mut batch_roots = HashMap::with_capacity(config.tables.len());
        let mut rows_per_batch = HashMap::with_capacity(config.tables.len());
        let mut row_hashes = HashMap::with_capacity(config.tables.len());
        let mut cascade_hashes = HashMap::with_capacity(config.tables.len());
        let mut table_names = Vec::with_capacity(config.tables.len());
        let mut table_indices = HashMap::with_capacity(config.tables.len());

        for (i, (table, cols)) in config.tables.iter().enumerate() {
            hashers.insert(
                table.clone(),
                RowHasher::new(cols.clone(), config.algorithm),
            );
            batch_roots.insert(table.clone(), Vec::new());
            rows_per_batch.insert(table.clone(), Vec::new());
            if config.store_row_hashes {
                row_hashes.insert(table.clone(), Vec::new());
            }
            cascade_hashes.insert(table.clone(), HashSet::new());

            table_names.push(table.clone());
            table_indices.insert(table.clone(), i);
        }

        Self {
            hashers,
            merkle_store,
            batch_roots,
            rows_per_batch,
            row_hashes,
            cascade_hashes,
            lane_sink: None,
            config,
            table_names,
            table_indices,
        }
    }

    /// Run in lane mode: accumulate every table as sorted hashes and merge into
    /// `sink` at finalization instead of writing per-lane receipts.
    pub fn with_lane_sink(mut self, sink: LaneHashSink) -> Self {
        self.lane_sink = Some(sink);
        self
    }

    /// Hash all rows in `rows`, grouped by destination table.
    /// Primary table rows are batched (insertion order); cascade table rows are
    /// deduplicated and accumulated for a single sorted Merkle root at finalization.
    pub fn hash_batch(&mut self, rows: &[Record]) {
        if rows.is_empty() {
            return;
        }

        // Group rows by destination table name.
        let mut groups: Vec<Vec<&Record>> = vec![Vec::new(); self.table_names.len()];

        for row in rows {
            let idx = self
                .table_indices
                .get(row.schema.as_str())
                .copied()
                .unwrap_or(0);
            groups[idx].push(row);
        }

        for (idx, table_rows) in groups.into_iter().enumerate() {
            if table_rows.is_empty() {
                continue;
            }

            // Clone the (small) table name so it doesn't hold an immutable borrow
            // of `self` across the `&mut self` `process_*` calls. This is once per
            // group (a handful per batch), not per row.
            let key = self.table_names[idx].clone();
            let is_primary = key == self.config.primary_table && self.lane_sink.is_none();

            if is_primary {
                self.process_primary_table(&key, &table_rows);
            } else {
                self.process_cascade_table(&key, &table_rows);
            }
        }
    }

    /// Build per-table Merkle receipts and persist them to the store.
    /// `skipped_rows` is the coordinator-level skip counter for the primary receipt.
    pub async fn save_receipts(
        &mut self,
        pipeline_name: &str,
        run_id: &str,
        skipped_rows: u64,
    ) -> Result<(), ProducerError> {
        if let Some(sink) = &self.lane_sink {
            let mut guard = sink.lock().expect("lane hash sink poisoned");

            // ZERO-COPY MERGE: Use `.drain()` to move the hashes into the shared sink
            // without cloning thousands of 32-byte arrays in memory.
            for (table, hashes) in self.cascade_hashes.drain() {
                guard.entry(table).or_default().extend(hashes);
            }
            return Ok(());
        }

        self.save_primary_tables(pipeline_name, run_id, skipped_rows)
            .await?;
        self.save_cascade_tables(pipeline_name, run_id).await?;

        Ok(())
    }

    /// Primary table: batch-based hashing (rows arrive in order
    /// from the offset strategy, so batches align with verify's reads).
    fn process_primary_table(&mut self, key: &str, rows: &[&Record]) {
        let empty_map = HashMap::new();
        let col_types = self.config.column_types.get(key).unwrap_or(&empty_map);
        let hasher = self.hashers.get_mut(key).expect("hasher pre-populated");

        let row_hashes: Vec<[u8; 32]> = rows
            .iter()
            .map(|r| hasher.hash_row_coerced(r, col_types))
            .collect();

        let subtree_root = MerkleTree::root_from_hashes(&row_hashes, self.config.algorithm);

        // Maps are pre-populated in `new()`, so we avoid `.entry().or_default()` String allocations
        self.batch_roots.get_mut(key).unwrap().push(subtree_root);
        self.rows_per_batch
            .get_mut(key)
            .unwrap()
            .push(rows.len() as u64);

        if self.config.store_row_hashes {
            self.row_hashes
                .get_mut(key)
                .unwrap()
                .extend_from_slice(&row_hashes);
        }
    }

    /// Cascade table: accumulate unique row hashes. The same row may be
    /// referenced by multiple source batches.
    fn process_cascade_table(&mut self, key: &str, rows: &[&Record]) {
        let empty_map = HashMap::new();
        let col_types = self.config.column_types.get(key).unwrap_or(&empty_map);
        let hasher = self.hashers.get_mut(key).expect("hasher pre-populated");
        let set = self.cascade_hashes.get_mut(key).unwrap();

        for row in rows {
            set.insert(hasher.hash_row_coerced(row, col_types));
        }
    }

    async fn save_primary_tables(
        &self,
        pipeline_name: &str,
        run_id: &str,
        skipped_rows: u64,
    ) -> Result<(), ProducerError> {
        for (table_name, batch_roots) in &self.batch_roots {
            if batch_roots.is_empty() {
                continue;
            }

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
                run_id: run_id.to_string(),
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
        run_id: &str,
    ) -> Result<(), ProducerError> {
        for (table_name, hash_set) in &self.cascade_hashes {
            if hash_set.is_empty() {
                continue;
            }

            save_sorted_receipt(
                &self.merkle_store,
                &self.config,
                pipeline_name,
                run_id,
                table_name,
                hash_set,
            )
            .await?;
        }
        Ok(())
    }
}

/// Build and persist one order-independent (sorted) Merkle receipt for `table`
/// from a set of unique row hashes.
async fn save_sorted_receipt(
    merkle_store: &Arc<dyn MerkleStore>,
    config: &IntegrityConfig,
    pipeline_name: &str,
    run_id: &str,
    table_name: &str,
    hash_set: &HashSet<[u8; 32]>,
) -> Result<(), ProducerError> {
    let mut sorted_hashes: Vec<[u8; 32]> = hash_set.iter().copied().collect();
    sorted_hashes.sort_unstable();

    let total_rows = sorted_hashes.len() as u64;
    let table_root = MerkleTree::root_from_hashes(&sorted_hashes, config.algorithm);
    let column_order = config.tables.get(table_name).cloned().unwrap_or_default();

    let receipt = VerificationReceipt {
        run_id: run_id.to_string(),
        pipeline_name: pipeline_name.to_string(),
        table_name: table_name.to_string(),
        table_root,
        batch_roots: vec![table_root],
        column_order,
        total_rows,
        skipped_rows: 0,
        rows_per_batch: vec![total_rows],
        sorted_hashes: true,
        algorithm: config.algorithm,
        created_at: chrono::Utc::now(),
        row_hashes: None,
    };
    merkle_store.save_receipt(&receipt).await?;
    Ok(())
}

/// Write the combined per-table receipts after every lane has merged its hashes into `sink`.
pub async fn finalize_lane_sink(
    sink: &LaneHashSink,
    config: &IntegrityConfig,
    merkle_store: &Arc<dyn MerkleStore>,
    pipeline_name: &str,
    run_id: &str,
) -> Result<(), ProducerError> {
    let tables = std::mem::take(&mut *sink.lock().expect("lane hash sink poisoned"));

    for (table_name, hash_set) in tables {
        save_sorted_receipt(
            merkle_store,
            config,
            pipeline_name,
            run_id,
            &table_name,
            &hash_set,
        )
        .await?;
    }
    Ok(())
}
