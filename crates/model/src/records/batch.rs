use crate::{pagination::cursor::Cursor, records::Record};

#[derive(Debug, Clone)]
pub struct Batch {
    pub id: String,
    pub rows: Vec<Record>, // already transformed
    pub cursor: Cursor,    // cursor used to start this batch (last committed offset)
    pub next: Cursor,      // resume-from cursor (end of this batch)
    pub ts: chrono::DateTime<chrono::Utc>,
}

impl Batch {
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn size_bytes(&self) -> usize {
        self.rows.iter().map(|r| r.size_bytes()).sum()
    }
}
