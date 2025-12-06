use crate::{
    error::AdapterError,
    file::csv::{adapter::CsvAdapter, settings::CsvSettings},
    sql::{base::adapter::SqlAdapter, mysql::adapter::MySqlAdapter, postgres::adapter::PgAdapter},
};
use smql_syntax::ast_v2::connection::DataFormat;

#[derive(Clone)]
pub enum Adapter {
    MySql(MySqlAdapter),
    Postgres(PgAdapter),
    Csv(CsvAdapter),
}

impl Adapter {
    pub async fn sql(format: DataFormat, conn_str: &str) -> Result<Self, AdapterError> {
        match format {
            DataFormat::MySql => {
                let adapter = MySqlAdapter::connect(conn_str).await?;
                Ok(Adapter::MySql(adapter))
            }
            DataFormat::Postgres => {
                let adapter = PgAdapter::connect(conn_str).await?;
                Ok(Adapter::Postgres(adapter))
            }
            _ => Err(AdapterError::UnsupportedFormat(format.to_string())),
        }
    }

    pub fn file(path: &str, settings: CsvSettings) -> Result<Self, AdapterError> {
        let adapter = CsvAdapter::new(path, settings)?;
        Ok(Adapter::Csv(adapter))
    }

    pub fn get_sql(&self) -> &(dyn SqlAdapter + Send + Sync) {
        match self {
            Adapter::MySql(adapter) => adapter,
            Adapter::Postgres(adapter) => adapter,
            _ => panic!("Unsupported adapter type"),
        }
    }
}
