use std::{fs, path::Path};

pub struct QueryLoader;

impl QueryLoader {
    pub fn table_metadata_query() -> Result<String, Box<dyn std::error::Error>> {
        Self::load_query("queries/table_metadata.sql")
    }

    fn load_query(file_path: &str) -> Result<String, Box<dyn std::error::Error>> {
        let path = Path::new(file_path);
        match fs::read_to_string(path) {
            Ok(query) => Ok(query),
            Err(e) => Err(format!("Failed to read query file: {}", e).into()),
        }
    }
}
