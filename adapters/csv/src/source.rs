use crate::{
    adapter::CsvAdapter,
    error::FileError,
    filter::CsvFilter,
    metadata::{normalize_col_name, CsvMetadata, MetadataHelper},
    types::CsvType,
};
use common::{row_data::RowData, value::FieldValue};
use std::sync::Arc;
use tracing::warn;

pub trait FileDataSource: MetadataHelper + Send + Sync {
    type Error;

    fn fetch(&mut self, batch_size: usize) -> Result<Vec<RowData>, Self::Error>;
}

pub struct CsvDataSource {
    pub adapter: CsvAdapter,
    pub primary_meta: Option<CsvMetadata>,
    pub filter: Option<CsvFilter>,
}

impl CsvDataSource {
    pub fn new(adapter: CsvAdapter, filter: Option<CsvFilter>) -> Self {
        CsvDataSource {
            adapter,
            filter,
            primary_meta: None,
        }
    }
}

impl FileDataSource for CsvDataSource {
    type Error = FileError;

    fn fetch(&mut self, batch_size: usize) -> Result<Vec<RowData>, Self::Error> {
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

        let mut result = Vec::new();
        while result.len() < batch_size {
            match data_iter.next() {
                Some(Ok(record)) => {
                    if let Some(ref filter) = self.filter {
                        if !filter.eval(&record, &headers_meta) {
                            continue;
                        }
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
                        "Error reading CSV record: {}",
                        e
                    )));
                }
                // End of file
                None => break,
            }
        }

        Ok(result)
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
