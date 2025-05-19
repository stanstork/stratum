use crate::{adapter::CsvAdapter, error::FileError, metadata::MetadataHelper};
use std::sync::Arc;

pub trait FileDataSource: MetadataHelper + Send + Sync {
    type Error;

    fn fetch(
        &mut self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Result<Vec<String>, Self::Error>;
}

pub struct CsvDataSource {
    pub adapter: CsvAdapter,
}

impl CsvDataSource {
    pub fn new(adapter: CsvAdapter) -> Self {
        CsvDataSource { adapter }
    }
}

impl FileDataSource for CsvDataSource {
    type Error = FileError;

    fn fetch(
        &mut self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Result<Vec<String>, Self::Error> {
        // Read the CSV file using the adapter
        let records = self.adapter.read(batch_size, offset.unwrap_or(0))?;
        // Convert the records to a vector of strings
        let mut result = Vec::new();
        for record in records {
            let mut row = String::new();
            for field in record.iter() {
                row.push_str(&format!("{},", field));
            }
            // Remove the trailing comma
            if !row.is_empty() {
                row.pop();
            }
            result.push(row);
        }

        println!("Rows fetched: {:?}", result);

        todo!("Implement the fetch method for CSV data source");
    }
}

impl MetadataHelper for CsvDataSource {
    fn adapter(&self) -> Arc<CsvAdapter> {
        Arc::new(self.adapter.clone())
    }
}
