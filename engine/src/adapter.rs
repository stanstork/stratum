use crate::error::MigrationError;
use csv::{adapter::CsvAdapter, settings::CsvSettings};
use mysql::adapter::MySqlAdapter;
use postgres::adapter::PgAdapter;
use smql::statements::connection::DataFormat;
use sql_adapter::adapter::SqlAdapter;

#[derive(Clone)]
pub enum Adapter {
    MySql(MySqlAdapter),
    Postgres(PgAdapter),
    Csv(CsvAdapter),
}

impl Adapter {
    pub async fn sql(format: DataFormat, conn_str: &str) -> Result<Self, MigrationError> {
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

    pub fn file(path: &str, settings: CsvSettings) -> Result<Self, MigrationError> {
        let adapter = CsvAdapter::new(path, settings)?;
        Ok(Adapter::Csv(adapter))
    }

    pub fn get_sql(&self) -> &dyn SqlAdapter {
        match self {
            Adapter::MySql(adapter) => adapter,
            Adapter::Postgres(adapter) => adapter,
            _ => panic!("Unsupported adapter type"),
        }
    }
}
