use crate::error::MigrationError;
use csv::{adapter::CsvAdapter, settings::CsvSettings};
use mysql::adapter::MySqlAdapter;
use postgres::adapter::PgAdapter;
use smql::statements::{connection::DataFormat, setting::Settings};
use sql_adapter::adapter::SqlAdapter;

#[derive(Clone)]
pub enum Adapter {
    MySql(MySqlAdapter),
    Postgres(PgAdapter),
    Csv(CsvAdapter),
}

impl Adapter {
    pub async fn new_sql(format: DataFormat, conn_str: &str) -> Result<Self, MigrationError> {
        match format {
            DataFormat::MySql => {
                let adapter = MySqlAdapter::connect(conn_str).await?;
                Ok(Adapter::MySql(adapter))
            }
            DataFormat::Postgres => {
                let adapter = PgAdapter::connect(conn_str).await?;
                Ok(Adapter::Postgres(adapter))
            }
            _ => Err(MigrationError::UnsupportedFormat(format.to_string())),
        }
    }

    pub fn new_file(path: &str, settings: Settings) -> Result<Self, MigrationError> {
        let csv_settings = CsvSettings::new(settings.csv_delimiter, settings.csv_header);
        let adapter = CsvAdapter::new(path, csv_settings)?;
        Ok(Adapter::Csv(adapter))
    }

    pub fn get_sql_adapter(&self) -> &dyn SqlAdapter {
        match self {
            Adapter::MySql(adapter) => adapter,
            Adapter::Postgres(adapter) => adapter,
            _ => panic!("Unsupported adapter type"),
        }
    }
}
