use model::records::batch::Batch;
use std::sync::Arc;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// One permit per KiB, so the permit count for a batch stays small (fits `u32`)
/// even for large batches.
const BYTES_PER_PERMIT: usize = 1024;

/// A rows batch in transit from producer to consumer, holding the slice of the
/// in-flight byte budget it occupies. Dropping the envelope - which the consumer
/// does once the batch is written - returns that budget to the producer.
pub struct BatchEnvelope {
    pub batch: Batch,
    // Released on drop; keep last so the budget frees only after the batch does.
    _permit: OwnedSemaphorePermit,
}

impl BatchEnvelope {
    pub fn new(batch: Batch, permit: OwnedSemaphorePermit) -> Self {
        Self {
            batch,
            _permit: permit,
        }
    }
}

/// Bounds the total *data bytes* of batches in flight, independent of the
/// batch-count bound on the channel itself.
#[derive(Clone)]
pub struct ByteBudget {
    sem: Arc<Semaphore>,
    max_permits: usize,
}

impl ByteBudget {
    pub fn new(max_bytes: usize) -> Self {
        let max_permits = (max_bytes / BYTES_PER_PERMIT).max(1);
        Self {
            sem: Arc::new(Semaphore::new(max_permits)),
            max_permits,
        }
    }

    /// Reserve budget for a batch of `bytes`, waiting while the in-flight total
    /// is at the cap. A batch larger than the whole budget reserves all of it -
    /// so it runs alone rather than deadlocking against a limit it can never
    /// fit under.
    pub async fn reserve(&self, bytes: usize) -> OwnedSemaphorePermit {
        // div_ceil avoids over-allocating on exact multiples of BYTES_PER_PERMIT.
        // clamp(1, max_permits) ensures 0-byte batches still request a valid 1 permit
        // and massive batches cap out gracefully.
        let permits = bytes.div_ceil(BYTES_PER_PERMIT).clamp(1, self.max_permits) as u32;

        self.sem
            .clone()
            .acquire_many_owned(permits)
            .await
            .expect("byte-budget semaphore is never closed")
    }
}
