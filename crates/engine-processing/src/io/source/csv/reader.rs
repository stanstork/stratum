use crate::io::source::reader::SourceReader;
use async_trait::async_trait;
use connectors::{drivers::csv::source::CsvDataSource, error::DriverError};
use model::pagination::{cursor::Cursor, page::FetchResult};
use std::sync::{Arc, Mutex};

/// Streams a CSV file as a pipeline source.
pub struct CsvSourceReader {
    inner: Arc<Mutex<CsvDataSource>>,
    table: String,
}

impl CsvSourceReader {
    pub fn new(source: CsvDataSource, table: impl Into<String>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(source)),
            table: table.into(),
        }
    }
}

#[async_trait]
impl SourceReader for CsvSourceReader {
    async fn fetch(&self, batch_size: usize, cursor: Cursor) -> Result<FetchResult, DriverError> {
        let mut guard = self.inner.lock().expect("csv source mutex poisoned");
        guard.fetch(batch_size, cursor).map_err(|e| {
            DriverError::QueryError(format!("csv source '{}' read failed: {e}", self.table))
        })
    }
}
