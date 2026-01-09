use crate::tui::{
    app::state::ErrorEntry,
    pipeline::{PipelineState, PipelineStatus},
};
use crossterm::event::{Event, EventStream, KeyEvent};
use futures_util::StreamExt;
use model::events::migration::MigrationEvent;
use std::{collections::HashMap, time::Instant};
use tokio::sync::mpsc;
use tracing::{debug, warn};

#[derive(Clone, Copy, Debug)]
pub enum TerminalEvent {
    Key(KeyEvent),
    Mouse,
    Resize,
}

/// Spawn terminal event reader in background
pub fn spawn_terminal_events() -> mpsc::Receiver<TerminalEvent> {
    let (tx, rx) = mpsc::channel(100);

    tokio::spawn(async move {
        let mut reader = EventStream::new();
        loop {
            match reader.next().await {
                Some(Ok(Event::Key(key))) => {
                    let _ = tx.send(TerminalEvent::Key(key)).await;
                }
                Some(Ok(Event::Mouse(_mouse))) => {
                    let _ = tx.send(TerminalEvent::Mouse).await;
                }
                Some(Ok(Event::Resize(_w, _h))) => {
                    let _ = tx.send(TerminalEvent::Resize).await;
                }
                _ => {}
            }
        }
    });

    rx
}

/// Handle a migration event and update pipeline state
pub fn handle_migration_event(
    pipelines: &mut HashMap<String, PipelineState>,
    errors: &mut Vec<ErrorEntry>,
    event: MigrationEvent,
) {
    debug!("Received migration event: {}", event.event_type());

    let item_id = match event.item_id() {
        Some(id) => id.to_string(),
        None => return,
    };

    if !pipelines.contains_key(&item_id) {
        warn!(item_id, "Received event for unknown pipeline");
        return;
    }

    dispatch_event(pipelines, errors, &item_id, event);
}

fn dispatch_event(
    pipelines: &mut HashMap<String, PipelineState>,
    errors: &mut Vec<ErrorEntry>,
    item_id: &str,
    event: MigrationEvent,
) {
    use MigrationEvent as ME;

    match &event {
        // Lifecycle
        ME::Started { .. }
        | ME::Completed { .. }
        | ME::Failed { .. }
        | ME::Paused { .. }
        | ME::Resumed { .. }
        | ME::Cancelled { .. } => {
            handle_lifecycle_event(pipelines, errors, item_id, &event);
        }

        // Progress & Throughput
        ME::Progress { .. }
        | ME::SnapshotStarted { .. }
        | ME::SnapshotCompleted { .. }
        | ME::BatchRead { .. }
        | ME::BatchWritten { .. }
        | ME::BatchProcessed { .. } => {
            handle_progress_event(pipelines, item_id, &event);
        }

        // Infrastructure / Actors
        ME::ProducerStopped { .. } | ME::ConsumerStopped { .. } => {
            handle_io_event(pipelines, item_id, &event);
        }

        // Errors & Retries
        ME::BatchRetrying { .. }
        | ME::BatchFailed { .. }
        | ME::ActorError { .. }
        | ME::ConnectionLost { .. }
        | ME::ValidationFailed { .. }
        | ME::SchemaCreationFailed { .. } => {
            handle_error_event(pipelines, errors, item_id, &event);
        }

        _ => debug!("Unhandled event type: {}", event.event_type()),
    }
}

fn handle_lifecycle_event(
    pipelines: &mut std::collections::HashMap<String, PipelineState>,
    errors: &mut Vec<ErrorEntry>,
    id: &str,
    event: &MigrationEvent,
) {
    let p = pipelines.get_mut(id).unwrap();

    match event {
        MigrationEvent::Started { .. } => {
            p.status = PipelineStatus::Running;
            p.started_at = Some(Instant::now());
        }
        MigrationEvent::Completed {
            rows_processed,
            rows_skipped,
            rows_failed,
            ..
        } => {
            p.status = PipelineStatus::Completed;
            p.completed_at = Some(Instant::now());
            p.processed_rows = *rows_processed;
            p.skipped_rows = *rows_skipped;
            p.failed_rows = *rows_failed;
            p.throughput.record(*rows_processed);
        }
        MigrationEvent::Failed {
            error,
            rows_processed,
            ..
        } => {
            p.status = PipelineStatus::Failed(error.clone());
            p.processed_rows = *rows_processed;
            p.completed_at = Some(Instant::now());
            record_error(errors, Some(id), error);
        }
        MigrationEvent::Paused { rows_processed, .. } => {
            p.status = PipelineStatus::Paused;
            p.processed_rows = *rows_processed;
        }
        MigrationEvent::Resumed { .. } => {
            p.status = PipelineStatus::Running;
        }
        MigrationEvent::Cancelled { rows_processed, .. } => {
            p.status = PipelineStatus::Skipped;
            p.processed_rows = *rows_processed;
            p.completed_at = Some(Instant::now());
        }
        _ => {}
    }
}

fn handle_progress_event(
    pipelines: &mut std::collections::HashMap<String, PipelineState>,
    id: &str,
    event: &MigrationEvent,
) {
    let p = pipelines.get_mut(id).unwrap();

    match event {
        MigrationEvent::Progress {
            rows_processed,
            rows_skipped,
            rows_failed,
            bytes_transferred,
            ..
        } => {
            p.processed_rows = *rows_processed;
            p.skipped_rows = *rows_skipped;
            p.failed_rows = *rows_failed;
            p.bytes_transferred = *bytes_transferred;
            p.throughput.record(*rows_processed);
        }
        MigrationEvent::SnapshotStarted {
            estimated_rows: Some(rows),
            ..
        } => {
            p.source_rows = *rows;
        }
        MigrationEvent::BatchWritten { row_count, .. }
        | MigrationEvent::BatchProcessed { row_count, .. } => {
            p.processed_rows += *row_count as u64;
            p.throughput.record(p.processed_rows);
        }
        MigrationEvent::BatchRead { .. } => {
            p.current_batch += 1;
        }
        _ => {}
    }
}

fn handle_error_event(
    pipelines: &mut std::collections::HashMap<String, PipelineState>,
    errors: &mut Vec<ErrorEntry>,
    id: &str,
    event: &MigrationEvent,
) {
    let p = pipelines.get_mut(id).unwrap();

    let msg = match event {
        MigrationEvent::BatchRetrying {
            error,
            batch_id,
            attempt,
            max_attempts,
            ..
        } => {
            format!(
                "Batch {} retry {}/{}: {}",
                batch_id, attempt, max_attempts, error
            )
        }
        MigrationEvent::BatchFailed {
            error, batch_id, ..
        } => {
            // Note: failed_rows count is tracked via Progress/Completed events
            format!("Batch {} failed: {}", batch_id, error)
        }
        MigrationEvent::ActorError {
            error, recoverable, ..
        } => {
            format!(
                "{} Actor Error: {}",
                if *recoverable { "Recoverable" } else { "Fatal" },
                error
            )
        }
        MigrationEvent::ConnectionLost { error, .. } => {
            format!("Connection lost: {}", error)
        }
        MigrationEvent::ValidationFailed {
            validation_type,
            errors: validation_errors,
            ..
        } => {
            format!(
                "Validation {:?} failed: {} errors",
                validation_type,
                validation_errors.len()
            )
        }
        _ => "Unknown error event".to_string(),
    };

    p.last_error = Some(msg.clone());
    record_error(errors, Some(id), &msg);
}

fn handle_io_event(
    pipelines: &mut std::collections::HashMap<String, PipelineState>,
    id: &str,
    event: &MigrationEvent,
) {
    let p = pipelines.get_mut(id).unwrap();

    match event {
        MigrationEvent::ProducerStopped { rows_produced, .. } => {
            p.processed_rows = *rows_produced;
        }
        MigrationEvent::ConsumerStopped { rows_written, .. } => {
            p.processed_rows = p.processed_rows.max(*rows_written);
        }
        _ => {}
    }
}

fn record_error(errors: &mut Vec<ErrorEntry>, item_id: Option<&str>, message: &str) {
    errors.insert(
        0,
        ErrorEntry::new(message.to_string(), item_id.map(|s| s.to_string())),
    );

    // Keep only last 100 errors
    if errors.len() > 100 {
        errors.truncate(100);
    }
}
