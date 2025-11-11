use crate::{pagination::cursor::Cursor, records::record::Record};
use std::collections::HashMap;

pub struct Batch {
    pub id: String,
    pub rows: HashMap<String, Vec<Record>>, // already transformed
    pub next: Cursor,                       // resume-from cursor (end of this batch)
    pub manifest: Manifest,
    pub ts: chrono::DateTime<chrono::Utc>,
}

pub struct Manifest {
    pub row_count: usize,
    pub checksum_xxh3: u64, // fast rolling checksum over canonicalized row
}

pub fn manifest_for(rows: &HashMap<String, Vec<Record>>) -> Manifest {
    use xxhash_rust::xxh3::xxh3_64_with_seed;
    let mut h: u64 = 0;
    for r in rows.values().flat_map(|v| v.iter()) {
        let bytes = r.canonical_bytes();
        h = xxh3_64_with_seed(&bytes, h);
    }
    Manifest {
        row_count: rows.values().map(|v| v.len()).sum(),
        checksum_xxh3: h,
    }
}

impl Batch {
    pub fn is_empty(&self) -> bool {
        self.manifest.row_count == 0
    }
}
