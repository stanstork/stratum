use chrono::{DateTime, Utc};
use model::pagination::cursor::Cursor;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum CheckpointStage {
    Read = 1,
    Write = 2,
    Committed = 3,
    Paused = 4, // intentional pause (resume will pick up from here)
    Validated = 5,
}

impl std::fmt::Display for CheckpointStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            CheckpointStage::Read => "read",
            CheckpointStage::Write => "write",
            CheckpointStage::Committed => "committed",
            CheckpointStage::Paused => "paused",
            CheckpointStage::Validated => "validated",
        };
        f.write_str(s)
    }
}

/// A durable checkpoint describing producer or consumer progress.
///
/// Semantics:
/// - stage="read":
///     - `src_offset` = cursor used to start this batch
///     - `pending_offset` = cursor after this batch (the "next" cursor)
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
    pub stage: CheckpointStage,
    pub src_offset: Cursor,
    #[serde(default)]
    pub pending_offset: Option<Cursor>,
    pub batch_id: String,
    pub rows_done: u64,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct CheckpointSummary {
    pub stage: CheckpointStage,
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
    BatchCommit {
        run_id: String,
        item_id: String,
        part_id: String,
        batch_id: String,
        ts: DateTime<Utc>,
    },
    ItemDone {
        run_id: String,
        item_id: String,
    },
    RunDone {
        run_id: String,
    },
    RunPaused {
        run_id: String,
        reason: PauseReason,
    },
    RunResumed {
        run_id: String,
    },
    Heartbeat {
        run_id: String,
        item_id: String,
        part_id: String,
        at: DateTime<Utc>,
    },
    CircuitBreakerOpen {
        run_id: String,
        item_id: String,
        part_id: String,
        stage: String,
        failures: u32,
        last_error: String,
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
            WalEntry::RunPaused { run_id, .. } => run_id,
            WalEntry::RunResumed { run_id, .. } => run_id,
            WalEntry::ItemStart { run_id, .. } => run_id,
            WalEntry::Heartbeat { run_id, .. } => run_id,
            WalEntry::CircuitBreakerOpen { run_id, .. } => run_id,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum RunStatus {
    Running,
    Paused {
        reason: PauseReason,
        paused_at: DateTime<Utc>,
    },
    Completed {
        completed_at: DateTime<Utc>,
    },
    Failed {
        error: String,
        failed_at: DateTime<Utc>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PauseReason {
    /// User sent pause signal (SIGUSR1 or `stratum pause`)
    Manual,
    /// Time limit reached (--run-for, --run-until)
    TimeLimit,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunState {
    pub run_id: String,
    pub config_path: String,
    pub config_hash: String,
    pub status: RunStatus,
    pub started_at: DateTime<Utc>,
    pub total_pipelines: usize,
    pub pipelines: Vec<PipelineRunState>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineRunState {
    pub name: String,
    pub item_id: String,
    pub status: PipelineStatus,
    pub rows_done: u64,
    pub total_rows: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PipelineStatus {
    Pending,
    Running,
    Completed,
    Failed { error: String },
    Blocked, // waiting for another pipeline to complete
}
