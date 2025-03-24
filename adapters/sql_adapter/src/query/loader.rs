use crate::db_type::DbType;
use std::{fs, path::Path};

pub struct QueryLoader;

impl QueryLoader {
    pub fn table_metadata_query(db_type: DbType) -> Result<String, Box<dyn std::error::Error>> {
        let file_path = match db_type {
            DbType::Postgres => "queries/pg/table_metadata.sql",
            DbType::MySql => "queries/mysql/table_metadata.sql",
            _ => return Err("Unsupported database type".into()),
        };
        Self::load_query(file_path)
    }

    pub fn table_referencing_query(db_type: DbType) -> Result<String, Box<dyn std::error::Error>> {
        let file_path = match db_type {
            DbType::Postgres => "queries/pg/table_referencing.sql",
            DbType::MySql => "queries/mysql/table_referencing.sql",
            _ => return Err("Unsupported database type".into()),
        };
        Self::load_query(file_path)
    }

    pub fn table_exists_query(db_type: DbType) -> Result<String, Box<dyn std::error::Error>> {
        let file_path = match db_type {
            DbType::Postgres => "queries/pg/table_exists.sql",
            DbType::MySql => "queries/mysql/table_exists.sql",
            _ => return Err("Unsupported database type".into()),
        };
        Self::load_query(file_path)
    }

    pub fn truncate_table_query(db_type: DbType) -> Result<String, Box<dyn std::error::Error>> {
        let file_path = match db_type {
            DbType::Postgres => "queries/pg/truncate_table.sql",
            DbType::MySql => "queries/mysql/truncate_table.sql",
            _ => return Err("Unsupported database type".into()),
        };
        Self::load_query(file_path)
    }

    fn load_query(file_path: &str) -> Result<String, Box<dyn std::error::Error>> {
        let path = Path::new(file_path);
        match fs::read_to_string(path) {
            Ok(query) => Ok(query),
            Err(e) => Err(format!("Failed to read query file: {}", e).into()),
        }
    }
}
