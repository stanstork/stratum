use adapter::DbAdapter;
use std::error::Error;

pub mod adapter;
pub mod db_type;
pub mod metadata;
pub mod mysql;
pub mod postgres;
pub mod query;
pub mod row;

pub enum DbEngine {
    Postgres,
    MySql,
}

pub async fn get_db_adapter(
    engine: DbEngine,
    conn_str: &str,
) -> Result<Box<dyn DbAdapter + Send + Sync>, Box<dyn Error>> {
    match engine {
        DbEngine::Postgres => {
            let adapter = postgres::PgAdapter::connect(conn_str).await?;
            Ok(Box::new(adapter))
        }
        DbEngine::MySql => {
            let adapter = mysql::MySqlAdapter::connect(conn_str).await?;
            Ok(Box::new(adapter))
        }
    }
}
