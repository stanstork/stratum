use crate::pagination::cursor::Cursor;
use std::fmt::Debug;

/// A trait for events that can be published on the EventBus.
pub trait Event: Send + Sync + Debug + 'static {
    /// Returns a unique identifier for this event type.
    fn event_type(&self) -> &'static str;
}

/// Emitted when a migration run starts.
#[derive(Debug, Clone)]
pub struct MigrationRunStarted {
    pub run_id: String,
    pub item_id: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Event for MigrationRunStarted {
    fn event_type(&self) -> &'static str {
        "migration.started"
    }
}

/// Emitted when a migration run completes successfully.
#[derive(Debug, Clone)]
pub struct MigrationCompleted {
    pub run_id: String,
    pub item_id: String,
    pub rows_processed: u64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Event for MigrationCompleted {
    fn event_type(&self) -> &'static str {
        "migration.completed"
    }
}

/// Emitted when a migration run fails.
#[derive(Debug, Clone)]
pub struct MigrationFailed {
    pub run_id: String,
    pub item_id: String,
    pub error: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Event for MigrationFailed {
    fn event_type(&self) -> &'static str {
        "migration.failed"
    }
}

/// Emitted when a snapshot phase begins.
#[derive(Debug, Clone)]
pub struct SnapshotStarted {
    pub run_id: String,
    pub item_id: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Event for SnapshotStarted {
    fn event_type(&self) -> &'static str {
        "snapshot.started"
    }
}

/// Emitted when a snapshot phase completes.
#[derive(Debug, Clone)]
pub struct SnapshotCompleted {
    pub run_id: String,
    pub item_id: String,
    pub rows_processed: u64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Event for SnapshotCompleted {
    fn event_type(&self) -> &'static str {
        "snapshot.completed"
    }
}

/// Emitted when CDC (Change Data Capture) starts.
#[derive(Debug, Clone)]
pub struct CdcStarted {
    pub run_id: String,
    pub item_id: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Event for CdcStarted {
    fn event_type(&self) -> &'static str {
        "cdc.started"
    }
}

/// Emitted when CDC stops.
#[derive(Debug, Clone)]
pub struct CdcStopped {
    pub run_id: String,
    pub item_id: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Event for CdcStopped {
    fn event_type(&self) -> &'static str {
        "cdc.stopped"
    }
}

/// Emitted periodically during migration with progress updates.
#[derive(Debug, Clone)]
pub struct MigrationProgress {
    pub run_id: String,
    pub item_id: String,
    pub rows_processed: u64,
    pub percentage: Option<f64>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Event for MigrationProgress {
    fn event_type(&self) -> &'static str {
        "migration.progress"
    }
}

/// Emitted when a batch of data is processed.
#[derive(Debug, Clone)]
pub struct BatchProcessed {
    pub run_id: String,
    pub item_id: String,
    pub batch_id: String,
    pub row_count: u64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Event for BatchProcessed {
    fn event_type(&self) -> &'static str {
        "batch.processed"
    }
}

/// Emitted when an actor encounters an error.
#[derive(Debug, Clone)]
pub struct ActorError {
    pub actor_name: String,
    pub run_id: String,
    pub item_id: String,
    pub error: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Event for ActorError {
    fn event_type(&self) -> &'static str {
        "actor.error"
    }
}

/// Emitted when a producer starts.
#[derive(Debug, Clone)]
pub struct ProducerStarted {
    pub run_id: String,
    pub item_id: String,
    pub mode: ProducerMode,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProducerMode {
    Snapshot,
    Cdc,
}

impl Event for ProducerStarted {
    fn event_type(&self) -> &'static str {
        "producer.started"
    }
}

/// Emitted when a producer stops.
#[derive(Debug, Clone)]
pub struct ProducerStopped {
    pub run_id: String,
    pub item_id: String,
    pub mode: ProducerMode,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Event for ProducerStopped {
    fn event_type(&self) -> &'static str {
        "producer.stopped"
    }
}

/// Emitted when a consumer starts.
#[derive(Debug, Clone)]
pub struct ConsumerStarted {
    pub run_id: String,
    pub item_id: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Event for ConsumerStarted {
    fn event_type(&self) -> &'static str {
        "consumer.started"
    }
}

/// Emitted when a consumer stops.
#[derive(Debug, Clone)]
pub struct ConsumerStopped {
    pub run_id: String,
    pub item_id: String,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Event for ConsumerStopped {
    fn event_type(&self) -> &'static str {
        "consumer.stopped"
    }
}

/// Emitted during the verification phase
#[derive(Debug, Clone)]
pub struct VerificationProgress {
    pub run_id: String,
    pub item_id: String,
    pub rows_verified: u64,
    pub mismatched_rows: f64,
    pub integrity_score: f64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Event for VerificationProgress {
    fn event_type(&self) -> &'static str {
        "verification.progress"
    }
}

/// Emitted when the verification phase completes for a table.
#[derive(Debug, Clone)]
pub struct VerificationCompleted {
    pub run_id: String,
    pub item_id: String,
    pub passed: bool,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Event for VerificationCompleted {
    fn event_type(&self) -> &'static str {
        "verification.completed"
    }
}

/// Emitted for non-fatal issues (e.g., type coercion, connection retries).
/// Distinct from "ActorError" which usually implies a failure.
#[derive(Debug, Clone)]
pub struct WarningEvent {
    pub run_id: String,
    pub item_id: String,
    pub message: String,
    pub code: String, // e.g., "WARN_TRUNCATION"
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Event for WarningEvent {
    fn event_type(&self) -> &'static str {
        "system.warning"
    }
}

/// Emitted when a graceful shutdown is requested (SIGINT/SIGTERM).
/// Support for "Signal Handling" task.
#[derive(Debug, Clone)]
pub struct ShutdownRequested {
    pub reason: String, // e.g., "SIGINT received"
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Event for ShutdownRequested {
    fn event_type(&self) -> &'static str {
        "system.shutdown_requested"
    }
}

/// Emitted when a checkpoint is successfully saved.
/// Support for "Checkpoint on shutdown" task.
#[derive(Debug, Clone)]
pub struct CheckpointSaved {
    pub run_id: String,
    pub item_id: String,
    pub cursor: Cursor,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Event for CheckpointSaved {
    fn event_type(&self) -> &'static str {
        "checkpoint.saved"
    }
}
