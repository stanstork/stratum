use crate::error::MigrationError;
use mysql::adapter::MySqlAdapter;
use postgres::adapter::PgAdapter;
use smql_v02::statements::connection::DataFormat;
use sql_adapter::adapter::SqlAdapter;

#[derive(Clone)]
/// Represents the SQL adapter for different database types.
pub enum Adapter {
    MySql(MySqlAdapter),
    Postgres(PgAdapter),
}

impl Adapter {
    pub async fn new(format: DataFormat, conn_str: &str) -> Result<Self, MigrationError> {
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

    pub fn get_adapter(&self) -> &dyn SqlAdapter {
        match self {
            Adapter::MySql(adapter) => adapter,
            Adapter::Postgres(adapter) => adapter,
        }
    }
}
