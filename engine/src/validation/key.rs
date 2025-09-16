use common::{row_data::RowData, value::Value};
use sql_adapter::metadata::table::TableMetadata;
use std::collections::{HashMap, HashSet};

use crate::report::finding::Finding;

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

pub struct KeyChecker {
    // intra-batch seen sets to catch duplicates before hitting DB
    seen: HashMap<(String, KeyKind), HashSet<Vec<Value>>>,
    // pending tuples to check against destination (batched)
    pending: HashMap<(String, KeyKind), Vec<Vec<Value>>>,

    findings: HashSet<Finding>,
}

impl KeyChecker {
    pub fn new() -> Self {
        Self {
            seen: HashMap::new(),
            pending: HashMap::new(),
            findings: HashSet::new(),
        }
    }

    pub fn accumulate(
        &mut self,
        table: &str,
        meta: &TableMetadata,
        row: &RowData,
        policy: KeyCheckPolicy,
    ) {
        if matches!(policy, KeyCheckPolicy::None) {
            return;
        }
    }

    fn collect(&mut self, table: &str, kind: KeyKind, cols: &[String], row: &RowData) -> bool {
        let key = (table.to_string(), kind);
        let seen_set = self.seen.entry(key.clone()).or_default();
        if seen_set.contains(&values) {
            // duplicate inside batch
            true
        } else {
            seen_set.insert(values.clone());
            self.pending.entry(key).or_default().push(values);
            false
        }
    }
}
