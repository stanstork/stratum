use futures::lock::Mutex;
use std::sync::Arc;

#[derive(Debug, Clone, Default)]
struct InnerMetrics {
    records_processed: u64,
    bytes_transferred: u64,
}

pub struct Metrics {
    inner: Arc<Mutex<InnerMetrics>>,
}

impl Metrics {
    pub fn new() -> Self {
        Metrics {
            inner: Arc::new(Mutex::new(InnerMetrics::default())),
        }
    }

    pub async fn increment_records(&self, count: u64) {
        let mut inner = self.inner.lock().await;
        inner.records_processed += count;
    }

    pub async fn increment_bytes(&self, count: u64) {
        let mut inner = self.inner.lock().await;
        inner.bytes_transferred += count;
    }

    pub async fn get_metrics(&self) -> (u64, u64) {
        {
            let inner = self.inner.lock().await;
            (inner.records_processed, inner.bytes_transferred)
        }
    }
}
