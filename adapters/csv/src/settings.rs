#[derive(Clone)]
pub struct CsvSettings {
    pub delimiter: char,
    pub has_headers: bool,
}

impl CsvSettings {
    pub fn new(delimiter: char, has_headers: bool) -> Self {
        CsvSettings {
            delimiter,
            has_headers,
        }
    }
}
