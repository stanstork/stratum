use smql::statements::connection::DataFormat;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DbType {
    MySql,
    Postgres,
    Other(String),
}

impl DbType {
    pub fn from_data_format(data_format: DataFormat) -> Self {
        match data_format {
            DataFormat::Postgres => DbType::Postgres,
            DataFormat::MySql => DbType::MySql,
            _ => panic!("Unsupported data format"),
        }
    }
}
