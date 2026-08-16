use crate::io::destination::Destination;
use crate::{
    error::ConsumerError,
    io::error::SinkError,
    retry::{classify_driver_error, classify_sink_error},
};
use connectors::sql::metadata::table::TableMetadata;
use engine_infra::retry::RetryPolicy;
use model::records::Record;
use model::records::batch::Batch;
use tracing::{debug, trace, warn};

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
                debug!("fast path available, using sink for writes");
                self.strategy = WriteStrategy::FastPath;
            }
            Ok(false) => {
                debug!("fast path unavailable, using regular INSERT writes");
                self.strategy = WriteStrategy::Regular;
            }
            Err(e) => {
                warn!(error = %e, "failed to detect fast path, falling back to regular writes");
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

    /// Run the destination sink's one-time setup before the first batch.
    pub async fn prepare(&self) -> Result<(), ConsumerError> {
        self.destination
            .prepare()
            .await
            .map_err(|e| ConsumerError::Sink(SinkError::Driver(e)))
    }

    /// Run the destination sink's one-time teardown after the last batch.
    pub async fn finalize(&self) -> Result<(), ConsumerError> {
        self.destination
            .finalize()
            .await
            .map_err(|e| ConsumerError::Sink(SinkError::Driver(e)))
    }

    /// Get current write strategy.
    pub fn strategy(&self) -> WriteStrategy {
        self.strategy
    }

    /// Check if fast path (sink) is available.
    async fn can_use_fast_path(&self) -> Result<bool, ConsumerError> {
        let fast = self.destination.sink().support_fast_path().await?;

        if self.meta.is_empty() {
            warn!("no table metadata available to determine fast path support");
            return Ok(false);
        }

        debug!(fast_path = %fast, "fast path support checked");
        Ok(fast)
    }

    /// Write batch using fast path (sink: COPY, MERGE, etc.).
    async fn write_batch_fast(&self, batch: &Batch) -> Result<WriteResult, ConsumerError> {
        let start = std::time::Instant::now();

        trace!(
            batch_id = %batch.id,
            row_count = batch.rows.len(),
            strategy = "fast_path",
            "writing batch via sink"
        );

        if self.meta.is_empty() {
            warn!(batch_id = %batch.id, "no table metadata for fast-path write, skipping batch");
            return Ok(WriteResult {
                rows_written: 0,
                duration: start.elapsed(),
                strategy: WriteStrategy::FastPath,
            });
        }

        let mut rows_written = 0;
        for (meta, rows) in self.group_rows(&batch.rows) {
            self.retry
                .run(
                    || {
                        let sink = self.destination.sink().clone();
                        async move { sink.write_fast_path(meta, rows).await }
                    },
                    classify_sink_error,
                )
                .await
                .map_err(|e| ConsumerError::Write {
                    batch_id: batch.id.clone(),
                    source: e.into_inner(),
                })?;
            rows_written += rows.len();
        }

        let duration = start.elapsed();

        Ok(WriteResult {
            rows_written,
            duration,
            strategy: WriteStrategy::FastPath,
        })
    }

    /// Write batch using regular path (INSERT statements).
    async fn write_batch_regular(&self, batch: &Batch) -> Result<WriteResult, ConsumerError> {
        let start = std::time::Instant::now();

        trace!(
            batch_id = %batch.id,
            row_count = batch.rows.len(),
            strategy = "regular",
            "writing batch via INSERT"
        );

        if self.meta.is_empty() {
            warn!(batch_id = %batch.id, "no table metadata for regular write, skipping batch");
            return Ok(WriteResult {
                rows_written: 0,
                duration: start.elapsed(),
                strategy: WriteStrategy::Regular,
            });
        }

        let mut rows_written = 0;
        for (meta, rows) in self.group_rows(&batch.rows) {
            self.retry
                .run(
                    || {
                        let sink = self.destination.sink().clone();
                        async move { sink.write_batch(meta, rows).await }
                    },
                    classify_driver_error,
                )
                .await
                .map_err(|e| ConsumerError::Write {
                    batch_id: batch.id.clone(),
                    source: SinkError::Driver(e.into_inner()),
                })?;
            rows_written += rows.len();
        }

        let duration = start.elapsed();

        Ok(WriteResult {
            rows_written,
            duration,
            strategy: WriteStrategy::Regular,
        })
    }

    /// Partition a batch's rows into contiguous same-table runs.
    ///
    /// Cascade fetches already emit rows grouped by table, so in
    /// practice this yields exactly one run per table.
    fn group_rows<'a>(&'a self, rows: &'a [Record]) -> Vec<(&'a TableMetadata, &'a [Record])> {
        if self.meta.len() == 1 {
            // Single table: the whole batch is one borrowed slice.
            return vec![(&self.meta[0], rows)];
        }

        let mut groups = Vec::new();
        let mut start = 0;
        while start < rows.len() {
            let schema = rows[start].table();
            let len = rows[start..]
                .iter()
                .take_while(|r| r.table() == schema)
                .count();
            groups.push((self.meta_for(schema), &rows[start..start + len]));
            start += len;
        }
        groups
    }

    /// Destination metadata for a row `schema`, falling back to the first table
    /// when the schema is unknown.
    fn meta_for(&self, schema: &str) -> &TableMetadata {
        self.meta
            .iter()
            .find(|m| m.name == schema)
            .unwrap_or(&self.meta[0])
    }
}
