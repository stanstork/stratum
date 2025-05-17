use crate::{error::FileError, settings::CsvSettings};
use std::{
    fs::File,
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
}
