use std::{fs, io, path::Path};

pub struct QueryLoader;

impl QueryLoader {
    pub fn load_query(file_path: &str) -> Result<String, io::Error> {
        let path = Path::new(file_path);
        match fs::read_to_string(path) {
            Ok(query) => Ok(query),
            Err(e) => Err(e),
        }
    }
}
