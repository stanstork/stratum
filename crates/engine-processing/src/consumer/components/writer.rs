use crate::{
    error::ConsumerError,
    retry::{classify_db_error, classify_sink_error},
};
use connectors::sql::base::metadata::table::TableMetadata;
use engine_core::{connectors::destination::Destination, retry::RetryPolicy};
use model::records::batch::Batch;
use tracing::{info, warn};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum WriteStrategy {
    /// Use sink for fast bulk writes (COPY, MERGE, etc.)
    FastPath,
    /// Use regular destination write (INSERT statements)
    Regular,
}

#[derive(Debug, Clone)]
pub struct WriteResult {
    pub rows_written: usize,
    pub duration: std::time::Duration,
    pub strategy: WriteStrategy,
}

/// Handles writing batches to the destination with retry logic.
pub struct BatchWriter {
    destination: Destination,
    retry: RetryPolicy,
    strategy: WriteStrategy,
    meta: Vec<TableMetadata>,
}

impl BatchWriter {
    pub fn new(destination: Destination, retry: RetryPolicy, meta: &[TableMetadata]) -> Self {
        Self {
            destination,
            retry,
            strategy: WriteStrategy::Regular, // Default to regular
            meta: meta.to_owned(),
        }
    }

    /// Create a writer with explicit strategy.
    pub fn with_strategy(mut self, strategy: WriteStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Detect and set the optimal write strategy based on capabilities.
    pub async fn auto_detect_strategy(mut self) -> Self {
        match self.can_use_fast_path().await {
            Ok(true) => {
                info!("Fast path available, using sink for writes");
                self.strategy = WriteStrategy::FastPath;
            }
            Ok(false) => {
                info!("Fast path not available, using regular destination writes");
                self.strategy = WriteStrategy::Regular;
            }
            Err(e) => {
                warn!(error = %e, "Failed to detect fast path, falling back to regular writes");
                self.strategy = WriteStrategy::Regular;
            }
        }
        self
    }

    /// Write a batch using the configured strategy.
    pub async fn write_batch(&self, batch: &Batch) -> Result<WriteResult, ConsumerError> {
        match self.strategy {
            WriteStrategy::FastPath => self.write_batch_fast(batch).await,
            WriteStrategy::Regular => self.write_batch_regular(batch).await,
        }
    }

    /// Get current write strategy.
    pub fn strategy(&self) -> WriteStrategy {
        self.strategy
    }

    /// Check if fast path (sink) is available.
    async fn can_use_fast_path(&self) -> Result<bool, ConsumerError> {
        let fast = self.destination.sink().support_fast_path().await?;
        if self.meta.is_empty() {
            warn!("No table metadata available to determine fast path support");
            return Ok(false);
        }
        let meta = &self.meta[0]; // For now, check only the first table
        info!(table = %meta.name, fast_path = %fast, "Fast path support checked");
        Ok(fast && !meta.primary_keys.is_empty())
    }

    /// Write batch using fast path (sink: COPY, MERGE, etc.).
    async fn write_batch_fast(&self, batch: &Batch) -> Result<WriteResult, ConsumerError> {
        let start = std::time::Instant::now();

        info!(
            batch_id = %batch.id,
            row_count = batch.rows.len(),
            strategy = "fast_path",
            "Writing batch to destination via sink"
        );

        if self.meta.is_empty() {
            warn!("No table metadata available for fast path write. Skipping write.");
            return Ok(WriteResult {
                rows_written: 0,
                duration: start.elapsed(),
                strategy: WriteStrategy::FastPath,
            });
        }

        // For now we support only single destination table
        let meta = self.meta[0].clone();

        // Use retry policy for transient failures
        self.retry
            .run(
                || {
                    let sink = self.destination.sink().clone();
                    let meta = meta.clone();
                    async move { sink.write_fast_path(&meta, batch).await }
                },
                classify_sink_error,
            )
            .await
            .map_err(|e| ConsumerError::Write {
                batch_id: batch.id.clone(),
                source: Box::new(std::io::Error::other(format!("{:?}", e))),
            })?;

        let duration = start.elapsed();
        let rows_written = batch.rows.len();
        let rows_per_sec = rows_written as f64 / duration.as_secs_f64();

        info!(
            batch_id = %batch.id,
            rows = rows_written,
            strategy = "fast_path",
            duration_ms = duration.as_millis(),
            rows_per_sec = %format!("{:.2}", rows_per_sec),
            "Batch written successfully via sink"
        );

        Ok(WriteResult {
            rows_written,
            duration,
            strategy: WriteStrategy::FastPath,
        })
    }

    /// Write batch using regular path (INSERT statements).
    async fn write_batch_regular(&self, batch: &Batch) -> Result<WriteResult, ConsumerError> {
        let start = std::time::Instant::now();

        info!(
            batch_id = %batch.id,
            row_count = batch.rows.len(),
            strategy = "regular",
            "Writing batch to destination"
        );

        if self.meta.is_empty() {
            warn!("No table metadata available for regular write. Skipping write.");
            return Ok(WriteResult {
                rows_written: 0,
                duration: start.elapsed(),
                strategy: WriteStrategy::Regular,
            });
        }

        // For now we support only single destination table
        let meta = self.meta[0].clone();

        // Use retry policy for transient failures
        self.retry
            .run(
                || {
                    let dest = self.destination.clone();
                    let meta = meta.clone();
                    async move { dest.write_batch(&meta, &batch.rows).await }
                },
                classify_db_error,
            )
            .await
            .map_err(|e| ConsumerError::Write {
                batch_id: batch.id.clone(),
                source: Box::new(std::io::Error::other(format!("{:?}", e))),
            })?;

        let duration = start.elapsed();
        let rows_written = batch.rows.len();
        let rows_per_sec = rows_written as f64 / duration.as_secs_f64();

        info!(
            batch_id = %batch.id,
            rows = rows_written,
            strategy = "regular",
            duration_ms = duration.as_millis(),
            rows_per_sec = %format!("{:.2}", rows_per_sec),
            "Batch written successfully via destination"
        );

        Ok(WriteResult {
            rows_written,
            duration,
            strategy: WriteStrategy::Regular,
        })
    }
}
