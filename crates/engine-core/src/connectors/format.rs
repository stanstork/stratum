use std::fmt::Display;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DataFormat {
    MySql,
    Postgres,
    Csv,
}

impl Display for DataFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataFormat::MySql => write!(f, "MySQL"),
            DataFormat::Postgres => write!(f, "Postgres"),
            DataFormat::Csv => write!(f, "CSV"),
        }
    }
}
