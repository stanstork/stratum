use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

#[derive(Debug, Default)]
struct InnerMetrics {
    records_processed: AtomicU64,
    bytes_transferred: AtomicU64,
    batches_processed: AtomicU64,
    failure_count: AtomicU64,
    retry_count: AtomicU64,
}

#[derive(Debug, Clone)]
pub struct Metrics {
    inner: Arc<InnerMetrics>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MetricsSnapshot {
    pub records_processed: u64,
    pub bytes_transferred: u64,
    pub batches_processed: u64,
    pub failure_count: u64,
    pub retry_count: u64,
}

impl Metrics {
    pub fn new() -> Self {
        Metrics {
            inner: Arc::new(InnerMetrics::default()),
        }
    }

    pub async fn increment_records(&self, count: u64) {
        self.inner
            .records_processed
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_bytes(&self, count: u64) {
        self.inner
            .bytes_transferred
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_batches(&self, count: u64) {
        self.inner
            .batches_processed
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_failures(&self, count: u64) {
        self.inner.failure_count.fetch_add(count, Ordering::Relaxed);
    }

    pub fn increment_retries(&self, count: u64) {
        self.inner.retry_count.fetch_add(count, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            records_processed: self.inner.records_processed.load(Ordering::Relaxed),
            bytes_transferred: self.inner.bytes_transferred.load(Ordering::Relaxed),
            batches_processed: self.inner.batches_processed.load(Ordering::Relaxed),
            failure_count: self.inner.failure_count.load(Ordering::Relaxed),
            retry_count: self.inner.retry_count.load(Ordering::Relaxed),
        }
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
