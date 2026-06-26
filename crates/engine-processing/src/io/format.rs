use engine_core::schema::type_registry::Dialect;
use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DataFormat {
    MySql,
    Postgres,
    Csv,
    Wasm,
}

impl Display for DataFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataFormat::MySql => write!(f, "MySQL"),
            DataFormat::Postgres => write!(f, "Postgres"),
            DataFormat::Csv => write!(f, "CSV"),
            DataFormat::Wasm => write!(f, "WASM"),
        }
    }
}

impl DataFormat {
    pub fn parse(format: &str) -> Option<Self> {
        match format.to_lowercase().as_str() {
            "mysql" => Some(DataFormat::MySql),
            "postgres" => Some(DataFormat::Postgres),
            "csv" => Some(DataFormat::Csv),
            "wasm" => Some(DataFormat::Wasm),
            _ => None,
        }
    }

    pub fn to_dialect(self) -> Dialect {
        match self {
            DataFormat::MySql => Dialect::MySql,
            DataFormat::Postgres => Dialect::Postgres,
            _ => panic!("Unsupported format for dialect conversion: {:?}", self),
        }
    }
}
