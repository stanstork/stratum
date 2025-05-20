use crate::{
    error::FileError,
    metadata::{CsvColumnMetadata, CsvMetadata},
    settings::CsvSettings,
    types::CsvType,
};
use common::types::DataType;
use std::{
    fs::File,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct CsvAdapter {
    pub reader: Arc<Mutex<csv::Reader<File>>>,
    pub settings: CsvSettings,
    pub headers: Vec<String>,
}

impl CsvAdapter {
    pub fn new(file_path: &str, settings: CsvSettings) -> Result<Self, FileError> {
        let file = File::open(file_path)?;
        let mut reader = csv::ReaderBuilder::new()
            .delimiter(settings.delimiter as u8)
            .has_headers(settings.has_headers)
            .from_reader(file);
        let headers = reader.headers()?.iter().map(|s| s.to_string()).collect();
        let reader = Arc::new(Mutex::new(reader));

        Ok(CsvAdapter {
            reader,
            settings,
            headers,
        })
    }

    pub fn read(
        &mut self,
        batch_size: usize,
        offset: usize,
    ) -> Result<Vec<csv::StringRecord>, FileError> {
        let _ = offset;
        self.reader
            .lock()
            .map_err(|_| FileError::LockError("…".into()))?
            .records()
            .take(batch_size)
            .map(|r| r.map_err(Into::into))
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
            .enumerate()
            .map(|(i, h)| CsvColumnMetadata {
                name: h.to_string(),
                data_type: DataType::Short,
                is_nullable: false,
                ordinal: i,
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
            name: file_path.to_string(),
            columns,
            delimiter: self.settings.delimiter,
            has_header: self.settings.has_headers,
        })
    }

    fn headers(&self) -> &Vec<String> {
        &self.headers
    }
}
