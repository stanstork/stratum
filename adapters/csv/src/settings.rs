#[derive(Clone)]
pub struct CsvSettings {
    pub delimiter: char,
    pub has_headers: bool,
    pub pk_column: Option<String>,
    pub sample_size: usize,
}

impl CsvSettings {
    pub fn new(delimiter: char, has_headers: bool, pk_column: Option<String>) -> Self {
        CsvSettings {
            delimiter,
            has_headers,
            pk_column,
            sample_size: 50, // Default sample size
        }
    }
}
