use crate::{pagination::cursor::Cursor, records::row::RowData};

#[derive(Debug, Clone)]
pub struct Batch {
    pub id: String,
    pub rows: Vec<RowData>, // already transformed
    pub cursor: Cursor,     // cursor used to start this batch (last committed offset)
    pub next: Cursor,       // resume-from cursor (end of this batch)
    pub manifest: Manifest,
    pub ts: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub struct Manifest {
    pub row_count: usize,
    pub checksum_xxh3: u64, // fast rolling checksum over canonicalized row
}

pub fn manifest_for(rows: &[RowData]) -> Manifest {
    // use xxhash_rust::xxh3::xxh3_64_with_seed;
    // TODO: implement proper rolling hash
    // let mut h: u64 = 0;
    // for r in rows.iter() {
    //     let bytes = r.canonical_bytes();
    //     h = xxh3_64_with_seed(&bytes, h);
    // }
    Manifest {
        row_count: rows.len(),
        checksum_xxh3: 0, // placeholder
    }
}

impl Batch {
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn size_bytes(&self) -> usize {
        self.rows.iter().map(|r| r.size_bytes()).sum()
    }
}
