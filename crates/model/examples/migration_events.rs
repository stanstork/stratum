/// Example demonstrating the comprehensive MigrationEvent enum usage
use model::events::migration::*;
use chrono::Utc;

fn main() {
    println!("=== Comprehensive MigrationEvent Example ===\n");

    // 1. Migration Lifecycle Events
    println!("--- Migration Lifecycle ---");

    let started = MigrationEvent::Started {
        run_id: "run-2025-01-29".to_string(),
        item_id: "users-table".to_string(),
        source: "mysql://source-db:3306/app".to_string(),
        destination: "postgres://dest-db:5432/app".to_string(),
        timestamp: Utc::now(),
    };
    println!("{}", started);

    let progress = MigrationEvent::Progress {
        run_id: "run-2025-01-29".to_string(),
        item_id: "users-table".to_string(),
        rows_processed: 500_000,
        rows_per_second: 5000.0,
        percentage: Some(50.0),
        eta_seconds: Some(100),
        timestamp: Utc::now(),
    };
    println!("{}", progress);

    let completed = MigrationEvent::Completed {
        run_id: "run-2025-01-29".to_string(),
        item_id: "users-table".to_string(),
        rows_processed: 1_000_000,
        duration_ms: 200_000,
        timestamp: Utc::now(),
    };
    println!("{}", completed);

    // 2. Phase Transition Events
    println!("\n--- Phase Transitions ---");

    let snapshot_started = MigrationEvent::SnapshotStarted {
        run_id: "run-2025-01-29".to_string(),
        item_id: "users-table".to_string(),
        estimated_rows: Some(1_000_000),
        timestamp: Utc::now(),
    };
    println!("{}", snapshot_started);

    let snapshot_completed = MigrationEvent::SnapshotCompleted {
        run_id: "run-2025-01-29".to_string(),
        item_id: "users-table".to_string(),
        rows_processed: 1_000_000,
        duration_ms: 180_000,
        timestamp: Utc::now(),
    };
    println!("{}", snapshot_completed);

    let cdc_started = MigrationEvent::CdcStarted {
        run_id: "run-2025-01-29".to_string(),
        item_id: "users-table".to_string(),
        starting_position: Some("00/1234ABCD".to_string()),
        timestamp: Utc::now(),
    };
    println!("{}", cdc_started);

    // 3. Producer & Consumer Events
    println!("\n--- Producer & Consumer ---");

    let producer_started = MigrationEvent::ProducerStarted {
        run_id: "run-2025-01-29".to_string(),
        item_id: "users-table".to_string(),
        mode: ProducerMode::Snapshot,
        timestamp: Utc::now(),
    };
    println!("{}", producer_started);

    let consumer_started = MigrationEvent::ConsumerStarted {
        run_id: "run-2025-01-29".to_string(),
        item_id: "users-table".to_string(),
        part_id: "part-0".to_string(),
        timestamp: Utc::now(),
    };
    println!("{}", consumer_started);

    let batch_read = MigrationEvent::BatchRead {
        run_id: "run-2025-01-29".to_string(),
        item_id: "users-table".to_string(),
        batch_id: "batch-001".to_string(),
        row_count: 10_000,
        timestamp: Utc::now(),
    };
    println!("{}", batch_read);

    let batch_written = MigrationEvent::BatchWritten {
        run_id: "run-2025-01-29".to_string(),
        item_id: "users-table".to_string(),
        batch_id: "batch-001".to_string(),
        row_count: 10_000,
        duration_ms: 250,
        timestamp: Utc::now(),
    };
    println!("{}", batch_written);

    // 4. Error Handling & Retry
    println!("\n--- Error Handling ---");

    let batch_retrying = MigrationEvent::BatchRetrying {
        run_id: "run-2025-01-29".to_string(),
        item_id: "users-table".to_string(),
        batch_id: "batch-042".to_string(),
        attempt: 2,
        max_attempts: 5,
        error: "Temporary connection timeout".to_string(),
        retry_delay_ms: 1000,
        timestamp: Utc::now(),
    };
    println!("{}", batch_retrying);

    let actor_error = MigrationEvent::ActorError {
        actor_name: "producer".to_string(),
        run_id: Some("run-2025-01-29".to_string()),
        item_id: Some("users-table".to_string()),
        error: "Circuit breaker tripped".to_string(),
        recoverable: true,
        timestamp: Utc::now(),
    };
    println!("{}", actor_error);

    // 5. Flow Control & Backpressure
    println!("\n--- Flow Control ---");

    let backpressure_detected = MigrationEvent::BackpressureDetected {
        run_id: "run-2025-01-29".to_string(),
        item_id: "users-table".to_string(),
        queue_size: 950,
        queue_capacity: 1000,
        timestamp: Utc::now(),
    };
    println!("{}", backpressure_detected);

    let backpressure_relieved = MigrationEvent::BackpressureRelieved {
        run_id: "run-2025-01-29".to_string(),
        item_id: "users-table".to_string(),
        queue_size: 100,
        timestamp: Utc::now(),
    };
    println!("{}", backpressure_relieved);

    // 6. Pause/Resume
    println!("\n--- Pause/Resume ---");

    let paused = MigrationEvent::Paused {
        run_id: "run-2025-01-29".to_string(),
        item_id: "users-table".to_string(),
        reason: PauseReason::Manual,
        rows_processed: 300_000,
        timestamp: Utc::now(),
    };
    println!("{}", paused);

    let resumed = MigrationEvent::Resumed {
        run_id: "run-2025-01-29".to_string(),
        item_id: "users-table".to_string(),
        timestamp: Utc::now(),
    };
    println!("{}", resumed);

    // 7. Connection Events
    println!("\n--- Connection Events ---");

    let connection_lost = MigrationEvent::ConnectionLost {
        run_id: "run-2025-01-29".to_string(),
        item_id: "users-table".to_string(),
        connection_type: ConnectionType::Source,
        error: "Connection reset by peer".to_string(),
        timestamp: Utc::now(),
    };
    println!("{}", connection_lost);

    let connection_restored = MigrationEvent::ConnectionRestored {
        run_id: "run-2025-01-29".to_string(),
        item_id: "users-table".to_string(),
        connection_type: ConnectionType::Source,
        timestamp: Utc::now(),
    };
    println!("{}", connection_restored);

    // 8. Validation Events
    println!("\n--- Validation ---");

    let validation_passed = MigrationEvent::ValidationPassed {
        run_id: "run-2025-01-29".to_string(),
        item_id: "users-table".to_string(),
        validation_type: ValidationType::RowCount,
        timestamp: Utc::now(),
    };
    println!("{}", validation_passed);

    // 9. Schema Events
    println!("\n--- Schema Operations ---");

    let schema_started = MigrationEvent::SchemaCreationStarted {
        run_id: "run-2025-01-29".to_string(),
        item_id: "users-table".to_string(),
        table_count: 5,
        timestamp: Utc::now(),
    };
    println!("{}", schema_started);

    let schema_completed = MigrationEvent::SchemaCreationCompleted {
        run_id: "run-2025-01-29".to_string(),
        item_id: "users-table".to_string(),
        tables_created: 5,
        duration_ms: 5000,
        timestamp: Utc::now(),
    };
    println!("{}", schema_completed);

    // 10. Serialization Example
    println!("\n--- JSON Serialization ---");

    let event = MigrationEvent::Progress {
        run_id: "run-123".to_string(),
        item_id: "orders-table".to_string(),
        rows_processed: 750_000,
        rows_per_second: 7500.0,
        percentage: Some(75.0),
        eta_seconds: Some(25),
        timestamp: Utc::now(),
    };

    let json = serde_json::to_string_pretty(&event).unwrap();
    println!("Serialized Progress Event:");
    println!("{}\n", json);

    // Deserialize back
    let deserialized: MigrationEvent = serde_json::from_str(&json).unwrap();
    println!("Deserialized: {}", deserialized);

    // 11. Helper Methods
    println!("\n--- Helper Methods ---");

    println!("Event type: {}", event.event_type());
    println!("Run ID: {:?}", event.run_id());
    println!("Item ID: {:?}", event.item_id());
    println!("Is error: {}", event.is_error());
    println!("Is lifecycle: {}", event.is_lifecycle());
    println!("Is progress: {}", event.is_progress());

    // 12. Pattern Matching
    println!("\n--- Pattern Matching ---");

    handle_event(started);
    handle_event(progress);
    handle_event(actor_error);
    handle_event(completed);
}

fn handle_event(event: MigrationEvent) {
    match event {
        MigrationEvent::Started { run_id, item_id, source, destination, .. } => {
            println!("→ Handling migration start: {} → {} (run={}, item={})", source, destination, run_id, item_id);
        }
        MigrationEvent::Progress { rows_processed, percentage, .. } => {
            let pct = percentage.map(|p| format!("{:.1}%", p)).unwrap_or_else(|| "N/A".to_string());
            println!("→ Progress update: {} rows ({})", rows_processed, pct);
        }
        MigrationEvent::Completed { rows_processed, duration_ms, .. } => {
            println!("→ Migration completed! {} rows in {}s", rows_processed, duration_ms / 1000);
        }
        MigrationEvent::Failed { error, .. } => {
            eprintln!("→ Migration failed: {}", error);
        }
        MigrationEvent::ActorError { actor_name, error, recoverable, .. } => {
            if recoverable {
                println!("→ Recoverable error in {}: {}", actor_name, error);
            } else {
                eprintln!("→ Fatal error in {}: {}", actor_name, error);
            }
        }
        _ => {
            println!("→ Other event: {}", event.event_type());
        }
    }
}
