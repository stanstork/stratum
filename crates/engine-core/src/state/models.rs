use chrono::{DateTime, Utc};
use model::pagination::cursor::Cursor;
use serde::{Deserialize, Serialize};

/// A durable checkpoint describing producer or consumer progress.
///
/// Semantics:
/// - stage="read":
///     - `src_offset` = cursor used to start this batch
///     - `pending_offset` = cursor after this batch (the “next” cursor)
/// - stage="write":
///     - `src_offset` = cursor after this batch (same as pending_offset)
///     - `pending_offset` = always None
/// - stage="committed":
///     - `src_offset` = fully committed cursor; safe resume point
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Checkpoint {
    pub run_id: String,
    pub item_id: String,
    pub part_id: String,
    pub stage: String, // "read", "committed", "validated"
    pub src_offset: Cursor,
    #[serde(default)]
    pub pending_offset: Option<Cursor>,
    pub batch_id: String,
    pub rows_done: u64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct CheckpointSummary {
    pub stage: String,
    pub src_offset: Cursor,
    pub pending_offset: Option<Cursor>,
    pub batch_id: String,
}

impl From<Checkpoint> for CheckpointSummary {
    fn from(value: Checkpoint) -> Self {
        Self {
            stage: value.stage,
            src_offset: value.src_offset,
            pending_offset: value.pending_offset,
            batch_id: value.batch_id,
        }
    }
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
    BatchBeginWrite {
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
            WalEntry::BatchBeginWrite { run_id, .. } => run_id,
            WalEntry::BatchCommit { run_id, .. } => run_id,
            WalEntry::ItemDone { run_id, .. } => run_id,
            WalEntry::RunDone { run_id } => run_id,
            WalEntry::ItemStart { run_id, .. } => run_id,
            WalEntry::Heartbeat { run_id, .. } => run_id,
        }
    }
}
