use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;

use crate::events::Event;

/// Comprehensive event enum covering all possible events during database migration lifecycle.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MigrationEvent {
    // === Migration Lifecycle ===
    /// Emitted when a migration run starts
    Started {
        run_id: String,
        item_id: String,
        source: String,
        destination: String,
        timestamp: DateTime<Utc>,
    },

    /// Emitted when a migration run completes successfully
    Completed {
        run_id: String,
        item_id: String,
        rows_processed: u64,
        duration_ms: u64,
        timestamp: DateTime<Utc>,
    },

    /// Emitted when a migration run fails permanently
    Failed {
        run_id: String,
        item_id: String,
        error: String,
        error_code: Option<String>,
        rows_processed: u64,
        timestamp: DateTime<Utc>,
    },

    /// Emitted when a migration is paused
    Paused {
        run_id: String,
        item_id: String,
        reason: PauseReason,
        rows_processed: u64,
        timestamp: DateTime<Utc>,
    },

    /// Emitted when a migration resumes from pause
    Resumed {
        run_id: String,
        item_id: String,
        timestamp: DateTime<Utc>,
    },

    /// Emitted when a migration is cancelled by user
    Cancelled {
        run_id: String,
        item_id: String,
        rows_processed: u64,
        timestamp: DateTime<Utc>,
    },

    // === Phase Transitions ===
    /// Emitted when snapshot phase begins
    SnapshotStarted {
        run_id: String,
        item_id: String,
        estimated_rows: Option<u64>,
        timestamp: DateTime<Utc>,
    },

    /// Emitted when snapshot phase completes
    SnapshotCompleted {
        run_id: String,
        item_id: String,
        rows_processed: u64,
        duration_ms: u64,
        timestamp: DateTime<Utc>,
    },

    /// Emitted when CDC (Change Data Capture) starts
    CdcStarted {
        run_id: String,
        item_id: String,
        starting_position: Option<String>,
        timestamp: DateTime<Utc>,
    },

    /// Emitted when CDC stops
    CdcStopped {
        run_id: String,
        item_id: String,
        final_position: Option<String>,
        timestamp: DateTime<Utc>,
    },

    // === Progress Tracking ===
    /// Emitted periodically during migration with progress updates
    Progress {
        run_id: String,
        item_id: String,
        rows_processed: u64,
        rows_per_second: f64,
        percentage: Option<f64>,
        eta_seconds: Option<u64>,
        timestamp: DateTime<Utc>,
    },

    // === Producer Events ===
    /// Emitted when a producer starts
    ProducerStarted {
        run_id: String,
        item_id: String,
        mode: ProducerMode,
        timestamp: DateTime<Utc>,
    },

    /// Emitted when a producer stops
    ProducerStopped {
        run_id: String,
        item_id: String,
        rows_produced: u64,
        timestamp: DateTime<Utc>,
    },

    /// Emitted when a batch is read by producer
    BatchRead {
        run_id: String,
        item_id: String,
        batch_id: String,
        row_count: usize,
        timestamp: DateTime<Utc>,
    },

    // === Consumer Events ===
    /// Emitted when a consumer starts
    ConsumerStarted {
        run_id: String,
        item_id: String,
        part_id: String,
        timestamp: DateTime<Utc>,
    },

    /// Emitted when a consumer stops
    ConsumerStopped {
        run_id: String,
        item_id: String,
        part_id: String,
        rows_written: u64,
        timestamp: DateTime<Utc>,
    },

    /// Emitted when a batch is successfully written by consumer
    BatchWritten {
        run_id: String,
        item_id: String,
        batch_id: String,
        row_count: usize,
        duration_ms: u64,
        timestamp: DateTime<Utc>,
    },

    /// Emitted when batch processing completes (generic)
    BatchProcessed {
        run_id: String,
        item_id: String,
        batch_id: String,
        row_count: usize,
        timestamp: DateTime<Utc>,
    },

    // === Error Handling & Retry ===
    /// Emitted when a batch write fails and will be retried
    BatchRetrying {
        run_id: String,
        item_id: String,
        batch_id: String,
        attempt: u32,
        max_attempts: u32,
        error: String,
        retry_delay_ms: u64,
        timestamp: DateTime<Utc>,
    },

    /// Emitted when a batch permanently fails after all retries
    BatchFailed {
        run_id: String,
        item_id: String,
        batch_id: String,
        attempts: u32,
        error: String,
        timestamp: DateTime<Utc>,
    },

    /// Emitted when an actor encounters an error
    ActorError {
        actor_name: String,
        run_id: Option<String>,
        item_id: Option<String>,
        error: String,
        recoverable: bool,
        timestamp: DateTime<Utc>,
    },

    // === Flow Control & Backpressure ===
    /// Emitted when consumer queue reaches high watermark
    BackpressureDetected {
        run_id: String,
        item_id: String,
        queue_size: usize,
        queue_capacity: usize,
        timestamp: DateTime<Utc>,
    },

    /// Emitted when backpressure is relieved
    BackpressureRelieved {
        run_id: String,
        item_id: String,
        queue_size: usize,
        timestamp: DateTime<Utc>,
    },

    // === Connection & Resource Events ===
    /// Emitted when connection to source/destination is lost
    ConnectionLost {
        run_id: String,
        item_id: String,
        connection_type: ConnectionType,
        error: String,
        timestamp: DateTime<Utc>,
    },

    /// Emitted when connection is restored
    ConnectionRestored {
        run_id: String,
        item_id: String,
        connection_type: ConnectionType,
        timestamp: DateTime<Utc>,
    },

    // === Coordination Events ===
    /// Emitted when all consumers are ready to start
    AllConsumersReady {
        run_id: String,
        item_id: String,
        consumer_count: usize,
        timestamp: DateTime<Utc>,
    },

    /// Emitted when a consumer is lagging behind
    ConsumerLagging {
        run_id: String,
        item_id: String,
        part_id: String,
        lag_count: usize,
        timestamp: DateTime<Utc>,
    },

    // === Validation Events ===
    /// Emitted when validation fails
    ValidationFailed {
        run_id: String,
        item_id: String,
        batch_id: Option<String>,
        validation_type: ValidationType,
        errors: Vec<String>,
        timestamp: DateTime<Utc>,
    },

    /// Emitted when validation succeeds
    ValidationPassed {
        run_id: String,
        item_id: String,
        validation_type: ValidationType,
        timestamp: DateTime<Utc>,
    },

    // === Schema Events ===
    /// Emitted when schema creation starts
    SchemaCreationStarted {
        run_id: String,
        item_id: String,
        table_count: usize,
        timestamp: DateTime<Utc>,
    },

    /// Emitted when schema creation completes
    SchemaCreationCompleted {
        run_id: String,
        item_id: String,
        tables_created: usize,
        duration_ms: u64,
        timestamp: DateTime<Utc>,
    },

    /// Emitted when schema creation fails
    SchemaCreationFailed {
        run_id: String,
        item_id: String,
        error: String,
        timestamp: DateTime<Utc>,
    },
}

// === Supporting Enums ===

/// Reason for pausing a migration
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PauseReason {
    /// Manually paused by user
    Manual,
    /// Paused due to error
    Error,
    /// Paused due to backpressure
    Backpressure,
    /// Paused due to resource limits
    ResourceLimit,
    /// Paused due to maintenance
    Maintenance,
}

/// Producer operation mode
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProducerMode {
    /// Initial snapshot of existing data
    Snapshot,
    /// Change Data Capture (streaming changes)
    Cdc,
}

/// Type of connection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionType {
    /// Source database connection
    Source,
    /// Destination database connection
    Destination,
}

/// Type of validation performed
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationType {
    /// Schema structure validation
    Schema,
    /// Data integrity validation
    DataIntegrity,
    /// Constraint validation
    Constraint,
    /// Row count validation
    RowCount,
}

// === Display Implementation ===

impl fmt::Display for MigrationEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MigrationEvent::Started {
                run_id,
                item_id,
                source,
                destination,
                timestamp,
            } => write!(
                f,
                "[{}] Migration started: {} -> {} (run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                source,
                destination,
                run_id,
                item_id
            ),

            MigrationEvent::Completed {
                run_id,
                item_id,
                rows_processed,
                duration_ms,
                timestamp,
            } => write!(
                f,
                "[{}] Migration completed: {} rows in {}ms (run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                rows_processed,
                duration_ms,
                run_id,
                item_id
            ),

            MigrationEvent::Failed {
                run_id,
                item_id,
                error,
                rows_processed,
                timestamp,
                ..
            } => write!(
                f,
                "[{}] Migration failed: {} (processed {} rows, run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                error,
                rows_processed,
                run_id,
                item_id
            ),

            MigrationEvent::Paused {
                run_id,
                item_id,
                reason,
                rows_processed,
                timestamp,
            } => write!(
                f,
                "[{}] Migration paused: {:?} (processed {} rows, run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                reason,
                rows_processed,
                run_id,
                item_id
            ),

            MigrationEvent::Resumed {
                run_id,
                item_id,
                timestamp,
            } => write!(
                f,
                "[{}] Migration resumed (run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                run_id,
                item_id
            ),

            MigrationEvent::Cancelled {
                run_id,
                item_id,
                rows_processed,
                timestamp,
            } => write!(
                f,
                "[{}] Migration cancelled: {} rows processed (run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                rows_processed,
                run_id,
                item_id
            ),

            MigrationEvent::Progress {
                run_id,
                item_id,
                rows_processed,
                rows_per_second,
                percentage,
                timestamp,
                ..
            } => {
                let pct_str = percentage
                    .map(|p| format!("{:.1}%", p))
                    .unwrap_or_else(|| "N/A".to_string());
                write!(
                    f,
                    "[{}] Progress: {} rows ({}) @ {:.0} rows/s (run={}, item={})",
                    timestamp.format("%Y-%m-%d %H:%M:%S"),
                    rows_processed,
                    pct_str,
                    rows_per_second,
                    run_id,
                    item_id
                )
            }

            MigrationEvent::SnapshotStarted {
                run_id,
                item_id,
                estimated_rows,
                timestamp,
            } => {
                let estimate = estimated_rows
                    .map(|r| format!(" (~{} rows)", r))
                    .unwrap_or_default();
                write!(
                    f,
                    "[{}] Snapshot started{} (run={}, item={})",
                    timestamp.format("%Y-%m-%d %H:%M:%S"),
                    estimate,
                    run_id,
                    item_id
                )
            }

            MigrationEvent::SnapshotCompleted {
                run_id,
                item_id,
                rows_processed,
                duration_ms,
                timestamp,
            } => write!(
                f,
                "[{}] Snapshot completed: {} rows in {}ms (run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                rows_processed,
                duration_ms,
                run_id,
                item_id
            ),

            MigrationEvent::CdcStarted {
                run_id,
                item_id,
                starting_position,
                timestamp,
            } => {
                let pos = starting_position
                    .as_ref()
                    .map(|p| format!(" from {}", p))
                    .unwrap_or_default();
                write!(
                    f,
                    "[{}] CDC started{} (run={}, item={})",
                    timestamp.format("%Y-%m-%d %H:%M:%S"),
                    pos,
                    run_id,
                    item_id
                )
            }

            MigrationEvent::CdcStopped {
                run_id,
                item_id,
                final_position,
                timestamp,
            } => {
                let pos = final_position
                    .as_ref()
                    .map(|p| format!(" at {}", p))
                    .unwrap_or_default();
                write!(
                    f,
                    "[{}] CDC stopped{} (run={}, item={})",
                    timestamp.format("%Y-%m-%d %H:%M:%S"),
                    pos,
                    run_id,
                    item_id
                )
            }

            MigrationEvent::ProducerStarted {
                run_id,
                item_id,
                mode,
                timestamp,
            } => write!(
                f,
                "[{}] Producer started: {:?} mode (run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                mode,
                run_id,
                item_id
            ),

            MigrationEvent::ProducerStopped {
                run_id,
                item_id,
                rows_produced,
                timestamp,
            } => write!(
                f,
                "[{}] Producer stopped: {} rows produced (run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                rows_produced,
                run_id,
                item_id
            ),

            MigrationEvent::BatchRead {
                run_id,
                item_id,
                batch_id,
                row_count,
                timestamp,
            } => write!(
                f,
                "[{}] Batch read: {} rows (batch={}, run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                row_count,
                batch_id,
                run_id,
                item_id
            ),

            MigrationEvent::ConsumerStarted {
                run_id,
                item_id,
                part_id,
                timestamp,
            } => write!(
                f,
                "[{}] Consumer started (part={}, run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                part_id,
                run_id,
                item_id
            ),

            MigrationEvent::ConsumerStopped {
                run_id,
                item_id,
                part_id,
                rows_written,
                timestamp,
            } => write!(
                f,
                "[{}] Consumer stopped: {} rows written (part={}, run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                rows_written,
                part_id,
                run_id,
                item_id
            ),

            MigrationEvent::BatchWritten {
                run_id,
                item_id,
                batch_id,
                row_count,
                duration_ms,
                timestamp,
            } => write!(
                f,
                "[{}] Batch written: {} rows in {}ms (batch={}, run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                row_count,
                duration_ms,
                batch_id,
                run_id,
                item_id
            ),

            MigrationEvent::BatchProcessed {
                run_id,
                item_id,
                batch_id,
                row_count,
                timestamp,
            } => write!(
                f,
                "[{}] Batch processed: {} rows (batch={}, run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                row_count,
                batch_id,
                run_id,
                item_id
            ),

            MigrationEvent::BatchRetrying {
                run_id,
                item_id,
                batch_id,
                attempt,
                max_attempts,
                error,
                retry_delay_ms,
                timestamp,
            } => write!(
                f,
                "[{}] Retrying batch {}/{}: {} (delay={}ms, batch={}, run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                attempt,
                max_attempts,
                error,
                retry_delay_ms,
                batch_id,
                run_id,
                item_id
            ),

            MigrationEvent::BatchFailed {
                run_id,
                item_id,
                batch_id,
                attempts,
                error,
                timestamp,
            } => write!(
                f,
                "[{}] Batch failed after {} attempts: {} (batch={}, run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                attempts,
                error,
                batch_id,
                run_id,
                item_id
            ),

            MigrationEvent::ActorError {
                actor_name,
                run_id,
                item_id,
                error,
                recoverable,
                timestamp,
            } => {
                let context = match (run_id, item_id) {
                    (Some(r), Some(i)) => format!("run={}, item={}", r, i),
                    (Some(r), None) => format!("run={}", r),
                    _ => "N/A".to_string(),
                };
                write!(
                    f,
                    "[{}] Actor error [{}]: {} (recoverable={}, {})",
                    timestamp.format("%Y-%m-%d %H:%M:%S"),
                    actor_name,
                    error,
                    recoverable,
                    context
                )
            }

            MigrationEvent::BackpressureDetected {
                run_id,
                item_id,
                queue_size,
                queue_capacity,
                timestamp,
            } => write!(
                f,
                "[{}] Backpressure detected: queue {}/{} (run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                queue_size,
                queue_capacity,
                run_id,
                item_id
            ),

            MigrationEvent::BackpressureRelieved {
                run_id,
                item_id,
                queue_size,
                timestamp,
            } => write!(
                f,
                "[{}] Backpressure relieved: queue size {} (run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                queue_size,
                run_id,
                item_id
            ),

            MigrationEvent::ConnectionLost {
                run_id,
                item_id,
                connection_type,
                error,
                timestamp,
            } => write!(
                f,
                "[{}] Connection lost [{:?}]: {} (run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                connection_type,
                error,
                run_id,
                item_id
            ),

            MigrationEvent::ConnectionRestored {
                run_id,
                item_id,
                connection_type,
                timestamp,
            } => write!(
                f,
                "[{}] Connection restored [{:?}] (run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                connection_type,
                run_id,
                item_id
            ),

            MigrationEvent::AllConsumersReady {
                run_id,
                item_id,
                consumer_count,
                timestamp,
            } => write!(
                f,
                "[{}] All {} consumers ready (run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                consumer_count,
                run_id,
                item_id
            ),

            MigrationEvent::ConsumerLagging {
                run_id,
                item_id,
                part_id,
                lag_count,
                timestamp,
            } => write!(
                f,
                "[{}] Consumer lagging: {} batches behind (part={}, run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                lag_count,
                part_id,
                run_id,
                item_id
            ),

            MigrationEvent::ValidationFailed {
                run_id,
                item_id,
                batch_id,
                validation_type,
                errors,
                timestamp,
            } => {
                let batch = batch_id
                    .as_ref()
                    .map(|b| format!(", batch={}", b))
                    .unwrap_or_default();
                write!(
                    f,
                    "[{}] Validation failed [{:?}]: {} errors{} (run={}, item={})",
                    timestamp.format("%Y-%m-%d %H:%M:%S"),
                    validation_type,
                    errors.len(),
                    batch,
                    run_id,
                    item_id
                )
            }

            MigrationEvent::ValidationPassed {
                run_id,
                item_id,
                validation_type,
                timestamp,
            } => write!(
                f,
                "[{}] Validation passed [{:?}] (run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                validation_type,
                run_id,
                item_id
            ),

            MigrationEvent::SchemaCreationStarted {
                run_id,
                item_id,
                table_count,
                timestamp,
            } => write!(
                f,
                "[{}] Schema creation started: {} tables (run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                table_count,
                run_id,
                item_id
            ),

            MigrationEvent::SchemaCreationCompleted {
                run_id,
                item_id,
                tables_created,
                duration_ms,
                timestamp,
            } => write!(
                f,
                "[{}] Schema creation completed: {} tables in {}ms (run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                tables_created,
                duration_ms,
                run_id,
                item_id
            ),

            MigrationEvent::SchemaCreationFailed {
                run_id,
                item_id,
                error,
                timestamp,
            } => write!(
                f,
                "[{}] Schema creation failed: {} (run={}, item={})",
                timestamp.format("%Y-%m-%d %H:%M:%S"),
                error,
                run_id,
                item_id
            ),
        }
    }
}

impl MigrationEvent {
    /// Get the event type as a string (useful for filtering/routing)
    pub fn event_type(&self) -> &'static str {
        match self {
            MigrationEvent::Started { .. } => "migration.started",
            MigrationEvent::Completed { .. } => "migration.completed",
            MigrationEvent::Failed { .. } => "migration.failed",
            MigrationEvent::Paused { .. } => "migration.paused",
            MigrationEvent::Resumed { .. } => "migration.resumed",
            MigrationEvent::Cancelled { .. } => "migration.cancelled",
            MigrationEvent::SnapshotStarted { .. } => "snapshot.started",
            MigrationEvent::SnapshotCompleted { .. } => "snapshot.completed",
            MigrationEvent::CdcStarted { .. } => "cdc.started",
            MigrationEvent::CdcStopped { .. } => "cdc.stopped",
            MigrationEvent::Progress { .. } => "migration.progress",
            MigrationEvent::ProducerStarted { .. } => "producer.started",
            MigrationEvent::ProducerStopped { .. } => "producer.stopped",
            MigrationEvent::BatchRead { .. } => "batch.read",
            MigrationEvent::ConsumerStarted { .. } => "consumer.started",
            MigrationEvent::ConsumerStopped { .. } => "consumer.stopped",
            MigrationEvent::BatchWritten { .. } => "batch.written",
            MigrationEvent::BatchProcessed { .. } => "batch.processed",
            MigrationEvent::BatchRetrying { .. } => "batch.retrying",
            MigrationEvent::BatchFailed { .. } => "batch.failed",
            MigrationEvent::ActorError { .. } => "actor.error",
            MigrationEvent::BackpressureDetected { .. } => "backpressure.detected",
            MigrationEvent::BackpressureRelieved { .. } => "backpressure.relieved",
            MigrationEvent::ConnectionLost { .. } => "connection.lost",
            MigrationEvent::ConnectionRestored { .. } => "connection.restored",
            MigrationEvent::AllConsumersReady { .. } => "consumers.ready",
            MigrationEvent::ConsumerLagging { .. } => "consumer.lagging",
            MigrationEvent::ValidationFailed { .. } => "validation.failed",
            MigrationEvent::ValidationPassed { .. } => "validation.passed",
            MigrationEvent::SchemaCreationStarted { .. } => "schema.creation.started",
            MigrationEvent::SchemaCreationCompleted { .. } => "schema.creation.completed",
            MigrationEvent::SchemaCreationFailed { .. } => "schema.creation.failed",
        }
    }

    /// Get the run_id for this event (if applicable)
    pub fn run_id(&self) -> Option<&str> {
        match self {
            MigrationEvent::Started { run_id, .. }
            | MigrationEvent::Completed { run_id, .. }
            | MigrationEvent::Failed { run_id, .. }
            | MigrationEvent::Paused { run_id, .. }
            | MigrationEvent::Resumed { run_id, .. }
            | MigrationEvent::Cancelled { run_id, .. }
            | MigrationEvent::SnapshotStarted { run_id, .. }
            | MigrationEvent::SnapshotCompleted { run_id, .. }
            | MigrationEvent::CdcStarted { run_id, .. }
            | MigrationEvent::CdcStopped { run_id, .. }
            | MigrationEvent::Progress { run_id, .. }
            | MigrationEvent::ProducerStarted { run_id, .. }
            | MigrationEvent::ProducerStopped { run_id, .. }
            | MigrationEvent::BatchRead { run_id, .. }
            | MigrationEvent::ConsumerStarted { run_id, .. }
            | MigrationEvent::ConsumerStopped { run_id, .. }
            | MigrationEvent::BatchWritten { run_id, .. }
            | MigrationEvent::BatchProcessed { run_id, .. }
            | MigrationEvent::BatchRetrying { run_id, .. }
            | MigrationEvent::BatchFailed { run_id, .. }
            | MigrationEvent::BackpressureDetected { run_id, .. }
            | MigrationEvent::BackpressureRelieved { run_id, .. }
            | MigrationEvent::ConnectionLost { run_id, .. }
            | MigrationEvent::ConnectionRestored { run_id, .. }
            | MigrationEvent::AllConsumersReady { run_id, .. }
            | MigrationEvent::ConsumerLagging { run_id, .. }
            | MigrationEvent::ValidationFailed { run_id, .. }
            | MigrationEvent::ValidationPassed { run_id, .. }
            | MigrationEvent::SchemaCreationStarted { run_id, .. }
            | MigrationEvent::SchemaCreationCompleted { run_id, .. }
            | MigrationEvent::SchemaCreationFailed { run_id, .. } => Some(run_id),
            MigrationEvent::ActorError { run_id, .. } => run_id.as_deref(),
        }
    }

    /// Get the item_id for this event (if applicable)
    pub fn item_id(&self) -> Option<&str> {
        match self {
            MigrationEvent::Started { item_id, .. }
            | MigrationEvent::Completed { item_id, .. }
            | MigrationEvent::Failed { item_id, .. }
            | MigrationEvent::Paused { item_id, .. }
            | MigrationEvent::Resumed { item_id, .. }
            | MigrationEvent::Cancelled { item_id, .. }
            | MigrationEvent::SnapshotStarted { item_id, .. }
            | MigrationEvent::SnapshotCompleted { item_id, .. }
            | MigrationEvent::CdcStarted { item_id, .. }
            | MigrationEvent::CdcStopped { item_id, .. }
            | MigrationEvent::Progress { item_id, .. }
            | MigrationEvent::ProducerStarted { item_id, .. }
            | MigrationEvent::ProducerStopped { item_id, .. }
            | MigrationEvent::BatchRead { item_id, .. }
            | MigrationEvent::ConsumerStarted { item_id, .. }
            | MigrationEvent::ConsumerStopped { item_id, .. }
            | MigrationEvent::BatchWritten { item_id, .. }
            | MigrationEvent::BatchProcessed { item_id, .. }
            | MigrationEvent::BatchRetrying { item_id, .. }
            | MigrationEvent::BatchFailed { item_id, .. }
            | MigrationEvent::BackpressureDetected { item_id, .. }
            | MigrationEvent::BackpressureRelieved { item_id, .. }
            | MigrationEvent::ConnectionLost { item_id, .. }
            | MigrationEvent::ConnectionRestored { item_id, .. }
            | MigrationEvent::AllConsumersReady { item_id, .. }
            | MigrationEvent::ConsumerLagging { item_id, .. }
            | MigrationEvent::ValidationFailed { item_id, .. }
            | MigrationEvent::ValidationPassed { item_id, .. }
            | MigrationEvent::SchemaCreationStarted { item_id, .. }
            | MigrationEvent::SchemaCreationCompleted { item_id, .. }
            | MigrationEvent::SchemaCreationFailed { item_id, .. } => Some(item_id),
            MigrationEvent::ActorError { item_id, .. } => item_id.as_deref(),
        }
    }

    /// Get the timestamp for this event
    pub fn timestamp(&self) -> &DateTime<Utc> {
        match self {
            MigrationEvent::Started { timestamp, .. }
            | MigrationEvent::Completed { timestamp, .. }
            | MigrationEvent::Failed { timestamp, .. }
            | MigrationEvent::Paused { timestamp, .. }
            | MigrationEvent::Resumed { timestamp, .. }
            | MigrationEvent::Cancelled { timestamp, .. }
            | MigrationEvent::SnapshotStarted { timestamp, .. }
            | MigrationEvent::SnapshotCompleted { timestamp, .. }
            | MigrationEvent::CdcStarted { timestamp, .. }
            | MigrationEvent::CdcStopped { timestamp, .. }
            | MigrationEvent::Progress { timestamp, .. }
            | MigrationEvent::ProducerStarted { timestamp, .. }
            | MigrationEvent::ProducerStopped { timestamp, .. }
            | MigrationEvent::BatchRead { timestamp, .. }
            | MigrationEvent::ConsumerStarted { timestamp, .. }
            | MigrationEvent::ConsumerStopped { timestamp, .. }
            | MigrationEvent::BatchWritten { timestamp, .. }
            | MigrationEvent::BatchProcessed { timestamp, .. }
            | MigrationEvent::BatchRetrying { timestamp, .. }
            | MigrationEvent::BatchFailed { timestamp, .. }
            | MigrationEvent::ActorError { timestamp, .. }
            | MigrationEvent::BackpressureDetected { timestamp, .. }
            | MigrationEvent::BackpressureRelieved { timestamp, .. }
            | MigrationEvent::ConnectionLost { timestamp, .. }
            | MigrationEvent::ConnectionRestored { timestamp, .. }
            | MigrationEvent::AllConsumersReady { timestamp, .. }
            | MigrationEvent::ConsumerLagging { timestamp, .. }
            | MigrationEvent::ValidationFailed { timestamp, .. }
            | MigrationEvent::ValidationPassed { timestamp, .. }
            | MigrationEvent::SchemaCreationStarted { timestamp, .. }
            | MigrationEvent::SchemaCreationCompleted { timestamp, .. }
            | MigrationEvent::SchemaCreationFailed { timestamp, .. } => timestamp,
        }
    }

    /// Check if this is an error event
    pub fn is_error(&self) -> bool {
        matches!(
            self,
            MigrationEvent::Failed { .. }
                | MigrationEvent::BatchFailed { .. }
                | MigrationEvent::ActorError { .. }
                | MigrationEvent::ValidationFailed { .. }
                | MigrationEvent::SchemaCreationFailed { .. }
                | MigrationEvent::ConnectionLost { .. }
        )
    }

    /// Check if this is a lifecycle event
    pub fn is_lifecycle(&self) -> bool {
        matches!(
            self,
            MigrationEvent::Started { .. }
                | MigrationEvent::Completed { .. }
                | MigrationEvent::Failed { .. }
                | MigrationEvent::Paused { .. }
                | MigrationEvent::Resumed { .. }
                | MigrationEvent::Cancelled { .. }
        )
    }

    /// Check if this is a progress event
    pub fn is_progress(&self) -> bool {
        matches!(self, MigrationEvent::Progress { .. })
    }
}

impl Event for MigrationEvent {
    fn event_type(&self) -> &'static str {
        self.event_type()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_serialization() {
        let event = MigrationEvent::Started {
            run_id: "run-123".to_string(),
            item_id: "item-456".to_string(),
            source: "mysql://localhost".to_string(),
            destination: "postgres://localhost".to_string(),
            timestamp: Utc::now(),
        };

        let json = serde_json::to_string(&event).unwrap();
        let deserialized: MigrationEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(event.event_type(), deserialized.event_type());
        assert_eq!(event.run_id(), deserialized.run_id());
    }

    #[test]
    fn test_event_display() {
        let event = MigrationEvent::Progress {
            run_id: "run-123".to_string(),
            item_id: "item-456".to_string(),
            rows_processed: 10000,
            rows_per_second: 1234.5,
            percentage: Some(45.6),
            eta_seconds: Some(120),
            timestamp: Utc::now(),
        };

        let display = format!("{}", event);
        assert!(display.contains("Progress"));
        assert!(display.contains("10000"));
        assert!(display.contains("45.6%"));
    }

    #[test]
    fn test_event_type_helpers() {
        let error_event = MigrationEvent::Failed {
            run_id: "run-123".to_string(),
            item_id: "item-456".to_string(),
            error: "Connection lost".to_string(),
            error_code: None,
            rows_processed: 5000,
            timestamp: Utc::now(),
        };

        assert!(error_event.is_error());
        assert!(error_event.is_lifecycle());
        assert!(!error_event.is_progress());

        let progress_event = MigrationEvent::Progress {
            run_id: "run-123".to_string(),
            item_id: "item-456".to_string(),
            rows_processed: 10000,
            rows_per_second: 1234.5,
            percentage: Some(45.6),
            eta_seconds: Some(120),
            timestamp: Utc::now(),
        };

        assert!(!progress_event.is_error());
        assert!(!progress_event.is_lifecycle());
        assert!(progress_event.is_progress());
    }
}
