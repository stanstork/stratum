use crate::{actor::ActorRef, error::ActorError};
use engine_core::event_bus::bus::EventBus;

#[derive(Debug, Clone)]
pub struct ChangeEvent {
    pub table: String,
}

#[derive(Debug, Clone)]
pub struct MetricSample {
    pub name: String,
    pub value: f64,
    pub tags: Vec<(String, String)>,
    pub ts: chrono::DateTime<chrono::Utc>,
}

/// Messages for the Producer actor.
///
/// Responsible for snapshot + CDC reading.
#[derive(Debug)]
pub enum ProducerMsg {
    /// Initialize the actor with its own reference for tick scheduling.
    SetActorRef(ActorRef<ProducerMsg>),

    /// Set the EventBus for publishing migration events.
    SetEventBus(EventBus),

    /// Start (or resume) snapshot for a given migration item.
    StartSnapshot { run_id: String, item_id: String },

    /// Start CDC stream for a given migration item.
    StartCdc { run_id: String, item_id: String },

    /// Periodic maintenance / health-check tick.
    Tick,

    /// Graceful shutdown.
    Stop { run_id: String, item_id: String },
}

/// Messages for the Consumer actor.
///
/// Responsible for applying batches to the destination.
#[derive(Debug)]
pub enum ConsumerMsg {
    /// Initialize the actor with its own reference for tick scheduling.
    SetActorRef(ActorRef<ConsumerMsg>),

    /// Set the EventBus for publishing migration events.
    SetEventBus(EventBus),

    /// Start the consumer for a specific item.
    Start {
        run_id: String,
        item_id: String,
        part_id: String,
    },

    /// Periodic tick to process one batch.
    Tick,

    /// Flush any buffered work.
    Flush { run_id: String, item_id: String },

    /// Graceful shutdown.
    Stop {
        run_id: String,
        item_id: String,
        part_id: String,
    },
}

/// Messages for the CDC actor.
///
/// Responsible for normalizing change events (Postgres WAL, MySQL binlog, etc.).
#[derive(Debug)]
pub enum CdcMsg {
    /// A batch of CDC events to process.
    EventBatch {
        run_id: String,
        item_id: String,
        events: Vec<ChangeEvent>,
    },

    /// Backoff / retry notification (e.g., after connection errors).
    Backoff {
        run_id: String,
        item_id: String,
        error: String,
    },

    /// Graceful shutdown.
    Stop,
}

/// Messages for the Telemetry actor.
///
/// Responsible for collecting metrics / errors from other actors.
#[derive(Debug)]
pub enum TelementryMsg {
    /// A single metric sample (throughput, lag, etc.).
    Metric(MetricSample),

    /// Report an error from another actor.
    ActorError {
        actor_name: String,
        error: ActorError,
    },

    /// Signal to flush data to Control Plane / logs.
    Flush,

    /// Graceful shutdown.
    Stop,
}
