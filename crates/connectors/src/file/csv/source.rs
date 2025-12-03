use crate::file::csv::{
    adapter::CsvAdapter,
    error::FileError,
    filter::CsvFilter,
    metadata::{CsvMetadata, MetadataHelper, normalize_col_name},
    types::CsvType,
};
use model::{
    core::value::FieldValue,
    pagination::{cursor::Cursor, page::FetchResult},
    records::row::RowData,
};
use std::sync::Arc;
use tracing::warn;

pub trait FileDataSource: MetadataHelper + Send + Sync {
    type Error;

    fn fetch(&mut self, batch_size: usize, cursor: Cursor) -> Result<FetchResult, Self::Error>;
}

pub struct CsvDataSource {
    pub adapter: CsvAdapter,
    pub primary_meta: Option<CsvMetadata>,
    pub filter: Option<CsvFilter>,
    /// Tracks how many rows have been consumed from the file.
    rows_read: usize,
}

impl CsvDataSource {
    pub fn new(adapter: CsvAdapter, filter: Option<CsvFilter>) -> Self {
        CsvDataSource {
            adapter,
            filter,
            primary_meta: None,
            rows_read: 0,
        }
    }
}

impl FileDataSource for CsvDataSource {
    type Error = FileError;

    fn fetch(&mut self, batch_size: usize, cursor: Cursor) -> Result<FetchResult, Self::Error> {
        let start = std::time::Instant::now();
        let meta = self.primary_meta.clone().expect("Metadata not set");
        let entity_name = meta.name.clone();

        // Pre-map headers -> ColumnMetadata
        let mut headers_meta = Vec::with_capacity(self.adapter.headers.len());
        for hdr in &self.adapter.headers {
            let col_meta = meta
                .columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(&normalize_col_name(hdr)))
                .expect("Column metadata not found")
                .clone();
            headers_meta.push((hdr.clone(), col_meta));
        }

        let mut data_iter = self
            .adapter
            .data_iter
            .lock()
            .map_err(|_| FileError::LockError("Failed to lock CSV reader".into()))?;

        let target_offset = match cursor {
            Cursor::None => 0,
            Cursor::Default { offset } => offset,
            other => {
                return Err(FileError::InvalidCursor(format!(
                    "Unsupported cursor: {other:?}"
                )));
            }
        };

        if target_offset < self.rows_read {
            return Err(FileError::InvalidCursor(format!(
                "Cursor offset {target_offset} is behind current offset {}",
                self.rows_read
            )));
        }

        while self.rows_read < target_offset {
            match data_iter.next() {
                Some(Ok(_)) => {
                    self.rows_read += 1;
                }
                Some(Err(e)) => {
                    return Err(FileError::ReadError(format!(
                        "Error reading CSV record: {e}"
                    )));
                }
                None => {
                    let took_ms = start.elapsed().as_millis();
                    return Ok(FetchResult {
                        rows: Vec::new(),
                        next_cursor: None,
                        reached_end: true,
                        row_count: 0,
                        took_ms,
                    });
                }
            }
        }

        let mut result = Vec::new();
        let mut reached_end = false;
        while result.len() < batch_size {
            match data_iter.next() {
                Some(Ok(record)) => {
                    self.rows_read += 1;

                    if let Some(ref filter) = self.filter
                        && !filter.eval(&record, &headers_meta)
                    {
                        continue;
                    }

                    let mut fields = Vec::with_capacity(headers_meta.len());
                    let mut skip_row = false;

                    for (hdr, col_meta) in headers_meta.iter() {
                        let cell = record.get(col_meta.ordinal).unwrap_or("");
                        let value = col_meta.data_type.get_value(cell);

                        // if value is None but column is not nullable, skip entire row
                        if value.is_none() && !col_meta.is_nullable {
                            warn!("Skipping row: column '{}' is null but not nullable", hdr);
                            skip_row = true;
                            break;
                        }

                        fields.push(FieldValue {
                            name: col_meta.name.clone(),
                            value,
                            data_type: col_meta.data_type.clone(),
                        });
                    }

                    if skip_row {
                        continue;
                    }

                    let row = RowData::new(&entity_name, fields);
                    result.push(row);
                }
                Some(Err(e)) => {
                    return Err(FileError::ReadError(format!(
                        "Error reading CSV record: {e}"
                    )));
                }
                // End of file
                None => {
                    reached_end = true;
                    break;
                }
            }
        }

        let row_count = result.len();
        let next_cursor = if reached_end {
            None
        } else {
            Some(Cursor::Default {
                offset: self.rows_read,
            })
        };
        let took_ms = start.elapsed().as_millis();

        Ok(FetchResult {
            rows: result,
            next_cursor,
            reached_end,
            row_count,
            took_ms,
        })
    }
}

impl MetadataHelper for CsvDataSource {
    fn adapter(&self) -> Arc<CsvAdapter> {
        Arc::new(self.adapter.clone())
    }

    fn set_metadata(&mut self, meta: CsvMetadata) {
        self.primary_meta = Some(meta);
    }
}
