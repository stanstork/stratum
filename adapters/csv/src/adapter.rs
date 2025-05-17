use crate::settings::CsvSettings;
use std::fs::File;

pub struct CsvAdapter {
    pub reader: csv::Reader<File>,
    pub settings: CsvSettings,
}

impl CsvAdapter {
    pub fn new(file_path: &str, settings: CsvSettings) -> Result<Self, csv::Error> {
        let file = File::open(file_path)?;
        let reader = csv::ReaderBuilder::new()
            .delimiter(settings.delimiter as u8)
            .has_headers(settings.has_headers)
            .from_reader(file);

        Ok(CsvAdapter { reader, settings })
    }

    pub fn read(&mut self) -> Result<Vec<String>, csv::Error> {
        let mut records = Vec::new();
        for result in self.reader.records() {
            let record = result?;
            records.push(record.iter().map(|s| s.to_string()).collect());
        }
        Ok(records)
    }
}
