use crate::{
    driver::SqlDriver,
    error::AdapterError,
    file::csv::{adapter::CsvAdapter, settings::CsvSettings},
    sql::{base::adapter::SqlAdapter, mysql::adapter::MySqlAdapter, postgres::adapter::PgAdapter},
};

#[derive(Clone)]
pub enum Adapter {
    MySql(MySqlAdapter),
    Postgres(PgAdapter),
    Csv(CsvAdapter),
}

impl Adapter {
    pub async fn sql(driver: SqlDriver, conn_str: &str) -> Result<Self, AdapterError> {
        match driver {
            SqlDriver::MySql => {
                let adapter = MySqlAdapter::connect(conn_str).await?;
                Ok(Adapter::MySql(adapter))
            }
            SqlDriver::Postgres => {
                let adapter = PgAdapter::connect(conn_str).await?;
                Ok(Adapter::Postgres(adapter))
            }
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
