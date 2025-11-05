use chrono::{DateTime, Utc};
use model::pagination::cursor::Cursor;
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
pub enum WalEntry {
    RunStart {
        run_id: String,
        plan_hash: String,
    },
    ItemStart {
        run_id: String,
        item_id: String,
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
    Heartbeat {
        run_id: String,
        item_id: String,
        part_id: String,
        at: DateTime<Utc>,
    },
}

impl WalEntry {
    pub fn run_id(&self) -> &str {
        match self {
            WalEntry::RunStart { run_id, .. } => run_id,
            WalEntry::BatchBegin { run_id, .. } => run_id,
            WalEntry::BatchCommit { run_id, .. } => run_id,
            WalEntry::ItemDone { run_id, .. } => run_id,
            WalEntry::RunDone { run_id } => run_id,
            WalEntry::ItemStart { run_id, .. } => run_id,
            WalEntry::Heartbeat { run_id, .. } => run_id,
        }
    }
}
