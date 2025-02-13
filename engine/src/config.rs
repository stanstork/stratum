use crate::database::mapping::TableMapping;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub source: String,
    pub destination: String,
    pub mappings: Vec<TableMapping>,
}

impl Config {
    pub fn source(&self) -> &str {
        &self.source
    }

    pub fn destination(&self) -> &str {
        &self.destination
    }

    pub fn mappings(&self) -> &[TableMapping] {
        &self.mappings
    }
}

pub fn read_config(path: &str) -> Result<Config, Box<dyn std::error::Error>> {
    let config = std::fs::read_to_string(path)?;
    let config: Config = serde_yaml::from_str(&config)?;
    Ok(config)
}
