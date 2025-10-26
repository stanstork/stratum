use chrono::{DateTime, Utc};
use query_builder::offsets::Cursor;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Checkpoint {
    pub run_id: String,
    pub item_id: String,
    pub part_id: String,
    pub stage: String, // "read", "committed", "validated"
    pub src_offset: Cursor,
    pub batch_id: String,
    pub rows_done: u64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum WallEntry {
    RunStart {
        run_id: String,
        spec_id: String,
    },
    BatchBegin {
        run_id: String,
        item_id: String,
        part_id: String,
        batch_id: String,
    },
    BatchCommit {
        run_id: String,
        item_id: String,
        part_id: String,
        batch_id: String,
    },
    ItemDone {
        run_id: String,
        item_id: String,
    },
    RunDone {
        run_id: String,
    },
}

impl WallEntry {
    pub fn run_id(&self) -> &str {
        match self {
            WallEntry::RunStart { run_id, .. } => run_id,
            WallEntry::BatchBegin { run_id, .. } => run_id,
            WallEntry::BatchCommit { run_id, .. } => run_id,
            WallEntry::ItemDone { run_id, .. } => run_id,
            WallEntry::RunDone { run_id } => run_id,
        }
    }
}
