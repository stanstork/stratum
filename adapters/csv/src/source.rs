use crate::{
    adapter::CsvAdapter,
    error::FileError,
    metadata::{CsvMetadata, MetadataHelper},
    types::CsvType,
};
use common::{row_data::RowData, value::FieldValue};
use std::sync::Arc;
use tracing::warn;

pub trait FileDataSource: MetadataHelper + Send + Sync {
    type Error;

    fn fetch(
        &mut self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Result<Vec<RowData>, Self::Error>;
}

pub struct CsvDataSource {
    pub adapter: CsvAdapter,
    pub primary_meta: Option<CsvMetadata>,
}

impl CsvDataSource {
    pub fn new(adapter: CsvAdapter) -> Self {
        CsvDataSource {
            adapter,
            primary_meta: None,
        }
    }
}

impl FileDataSource for CsvDataSource {
    type Error = FileError;

    fn fetch(
        &mut self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Result<Vec<RowData>, Self::Error> {
        let offset = offset.unwrap_or(0);
        let records = self.adapter.read(batch_size, offset)?;

        let meta = self.primary_meta.clone().expect("Metadata not set");
        let entity_name = meta.name.clone();

        // Pre-map headers -> ColumnMetadata
        let mut headers_meta = Vec::with_capacity(self.adapter.headers.len());
        for hdr in &self.adapter.headers {
            let col_meta = meta
                .columns
                .iter()
                .find(|c| c.name.eq_ignore_ascii_case(hdr))
                .expect("Column metadata not found")
                .clone();
            headers_meta.push((hdr.clone(), col_meta));
        }

        let mut result = Vec::with_capacity(records.len());
        for (row_idx, record) in records.into_iter().enumerate() {
            let mut fields = Vec::with_capacity(headers_meta.len());
            let mut skip_row = false;

            for (col_idx, (hdr, col_meta)) in headers_meta.iter().enumerate() {
                let cell = record.get(col_idx).unwrap_or("");
                let value = col_meta.data_type.get_value(cell);

                // if value is None but column is not nullable, skip entire row
                if value.is_none() && !col_meta.is_nullable {
                    warn!(
                        "Skipping row {}: column '{}' is null but not nullable",
                        offset + row_idx + 1,
                        hdr
                    );
                    skip_row = true;
                    break;
                }

                fields.push(FieldValue {
                    name: hdr.clone(),
                    value,
                });
            }

            if skip_row {
                continue;
            }

            let row = RowData::new(&entity_name, fields);
            result.push(row);
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
