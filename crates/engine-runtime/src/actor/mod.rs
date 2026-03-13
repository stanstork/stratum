pub mod consumer;
pub mod coordinator;
pub mod messages;
pub mod producer;

use crate::error::ActorError;

/// Outcome of processing a single tick or control message.
enum TickAction {
    /// Continue the loop, reset interval immediately.
    Continue,
    /// Continue the loop, switch to idle interval.
    Idle,
    /// Stop the task with success.
    Done,
    /// Stop the task with error.
    Failed(ActorError),
}
