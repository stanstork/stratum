use crate::{error::ProducerError, retry::classify_adapter_error};
use engine_core::{
    connectors::source::Source,
    retry::{RetryError, RetryPolicy},
};
use model::pagination::{cursor::Cursor, page::FetchResult};

/// Handles data fetching from source with retry logic.
pub struct SnapshotReader {
    source: Source,
    retry: RetryPolicy,
    batch_size: usize,
}

impl SnapshotReader {
    pub fn new(source: Source, retry: RetryPolicy, batch_size: usize) -> Self {
        Self {
            source,
            retry,
            batch_size,
        }
    }

    /// Fetch a batch of data with automatic retry on transient failures.
    pub async fn fetch(&self, cursor: Cursor) -> Result<FetchResult, ProducerError> {
        let source = self.source.clone();
        let cursor_template = cursor.clone();
        let batch_size = self.batch_size;

        let fetch_result = self
            .retry
            .run(
                || {
                    let source = source.clone();
                    let cursor = cursor_template.clone();
                    async move { source.fetch_data(batch_size, cursor).await }
                },
                classify_adapter_error,
            )
            .await;

        let res = match fetch_result {
            Ok(res) => res,
            Err(RetryError::Fatal(e)) => {
                return Err(ProducerError::Fetch {
                    cursor: cursor_template,
                    source: Box::new(std::io::Error::new(std::io::ErrorKind::Other, e)),
                });
            }
            Err(RetryError::AttemptsExceeded(e)) => {
                return Err(ProducerError::RetriesExhausted(e.to_string()));
            }
        };

        Ok(res)
    }

    /// Check if a fetch result indicates completion
    pub fn is_complete(result: &FetchResult) -> bool {
        result.reached_end && result.row_count == 0
    }

    /// Check if we should continue despite empty results
    pub fn should_advance(result: &FetchResult) -> bool {
        result.row_count == 0
            && result.next_cursor.is_some()
            && result.next_cursor.as_ref() != Some(&Cursor::None)
    }
}
