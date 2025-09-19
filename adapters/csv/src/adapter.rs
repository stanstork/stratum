use crate::{
    error::FileError,
    metadata::{normalize_col_name, CsvColumnMetadata, CsvMetadata},
    settings::CsvSettings,
    types::CsvType,
};
use common::types::DataType;
use csv::Position;
use std::{
    fs::File,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct CsvAdapter {
    /// Used only when inferring schema or re-reading headers
    pub meta_reader: Arc<Mutex<csv::Reader<File>>>,

    /// Used as a one-pass streaming iterator for actual data rows
    pub data_iter: Arc<Mutex<csv::StringRecordsIntoIter<File>>>,

    pub settings: CsvSettings,
    pub headers: Vec<String>,
}

impl CsvAdapter {
    pub fn new(file_path: &str, settings: CsvSettings) -> Result<Self, FileError> {
        // Build a shared builder so we don't repeat options
        let mut builder = csv::ReaderBuilder::new();
        let builder = builder
            .delimiter(settings.delimiter as u8)
            .has_headers(settings.has_headers)
            .flexible(true);

        // Open file + reader for metadata
        let meta_file = File::open(file_path)?;
        let mut meta_rdr = builder.from_reader(meta_file);
        let headers = meta_rdr.headers()?.iter().map(String::from).collect();

        // Open file + into_records iterator for streaming data
        let data_file = File::open(file_path)?;
        let data_rdr = builder.from_reader(data_file);
        let data_iter = data_rdr.into_records();

        Ok(CsvAdapter {
            meta_reader: Arc::new(Mutex::new(meta_rdr)),
            data_iter: Arc::new(Mutex::new(data_iter)),
            headers,
            settings,
        })
    }

    pub fn read(&mut self, batch_size: usize) -> Result<Vec<csv::StringRecord>, FileError> {
        let mut records = self
            .data_iter
            .lock()
            .map_err(|_| FileError::LockError("Failed to lock CSV reader".into()))?;

        let mut batch = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            if let Some(record) = records.next() {
                batch.push(record?);
            } else {
                break;
            }
        }

        Ok(batch)
    }

    pub async fn fetch_metadata(&self, file_path: &str) -> Result<CsvMetadata, FileError> {
        // Lock the reader for the entire sampling process
        let mut reader = self
            .meta_reader
            .lock()
            .map_err(|_| FileError::LockError("Failed to lock CSV reader".into()))?;

        // Initialize column metadata from headers
        let headers = reader.headers()?;
        let mut columns: Vec<CsvColumnMetadata> = headers
            .iter()
            .enumerate()
            .map(|(i, h)| CsvColumnMetadata {
                name: normalize_col_name(h),
                data_type: DataType::Short,
                is_nullable: false,
                is_primary_key: self.is_primary_key(h),
                ordinal: i,
            })
            .collect();

        let skip = if self.settings.has_headers { 1 } else { 0 };
        for result in reader.records().skip(skip).take(self.settings.sample_size) {
            let record = result?;
            for (col_meta, field) in columns.iter_mut().zip(record.iter()) {
                if field.is_empty() {
                    col_meta.is_nullable = true;
                }
                col_meta.data_type = col_meta.data_type.promote(field);
            }
        }

        // Reset the reader to the beginning of the file
        let mut position = Position::new();
        position.set_byte(0);
        reader.seek(position)?;

        Ok(CsvMetadata {
            name: file_path.to_string(),
            columns,
            delimiter: self.settings.delimiter,
            has_header: self.settings.has_headers,
        })
    }

    fn is_primary_key(&self, col_name: &str) -> bool {
        self.settings
            .pk_column
            .as_ref()
            .is_some_and(|pk| pk.eq_ignore_ascii_case(col_name))
    }
}
