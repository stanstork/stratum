use std::fmt::Debug;

pub mod migration;

/// A trait for events that can be published on the EventBus.
pub trait Event: Send + Sync + Debug + 'static {
    /// Returns a unique identifier for this event type.
    fn event_type(&self) -> &'static str;
}
