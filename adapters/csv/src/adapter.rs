use crate::{
    error::FileError,
    metadata::{CsvColumnMetadata, CsvMetadata},
    settings::CsvSettings,
    types::CsvType,
};
use common::types::DataType;
use std::{
    fs::File,
    path::Path,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct CsvAdapter {
    pub reader: Arc<Mutex<csv::Reader<File>>>,
    pub settings: CsvSettings,
}

impl CsvAdapter {
    pub fn new(file_path: &str, settings: CsvSettings) -> Result<Self, FileError> {
        let file = File::open(file_path)?;
        let reader = csv::ReaderBuilder::new()
            .delimiter(settings.delimiter as u8)
            .has_headers(settings.has_headers)
            .from_reader(file);
        let reader = Arc::new(Mutex::new(reader));

        Ok(CsvAdapter { reader, settings })
    }

    pub fn read(
        &mut self,
        batch_size: usize,
        offset: usize,
    ) -> Result<Vec<csv::StringRecord>, FileError> {
        let mut reader = self
            .reader
            .lock()
            .map_err(|_| FileError::LockError("Failed to lock CSV reader".to_string()))?;
        reader
            .records()
            .skip(offset)
            .take(batch_size)
            .map(|record| record.map_err(|e| e.into()))
            .collect()
    }

    pub async fn fetch_metadata(&self, file_path: &str) -> Result<CsvMetadata, FileError> {
        // Lock the reader for the entire sampling process
        let mut reader = self
            .reader
            .lock()
            .map_err(|_| FileError::LockError("Failed to lock CSV reader".into()))?;

        // Initialize column metadata from headers
        let headers = reader.headers()?;
        let mut columns: Vec<CsvColumnMetadata> = headers
            .iter()
            .map(|h| CsvColumnMetadata {
                name: h.to_string(),
                data_type: DataType::Short,
                is_nullable: false,
            })
            .collect();

        for result in reader.records().take(self.settings.sample_size) {
            let record = result?;
            for (col_meta, field) in columns.iter_mut().zip(record.iter()) {
                if field.is_empty() {
                    col_meta.is_nullable = true;
                }
                col_meta.data_type = col_meta.data_type.promote(field);
            }
        }

        Ok(CsvMetadata {
            name: self.get_name(file_path),
            columns,
            delimiter: self.settings.delimiter,
            has_header: self.settings.has_headers,
        })
    }

    fn get_name(&self, file_path: &str) -> String {
        Path::new(file_path)
            .file_stem()
            .and_then(|os| os.to_str())
            .unwrap_or(file_path)
            .to_string()
    }
}
