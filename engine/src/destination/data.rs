use crate::adapter::Adapter;
use postgres::destination::PgDestination;
use smql::statements::connection::DataFormat;
use sql_adapter::{
    adapter::SqlAdapter, destination::DbDataDestination, error::db::DbError,
    metadata::table::TableMetadata,
};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub enum DataDestination {
    Database(Arc<Mutex<dyn DbDataDestination<Error = DbError>>>),
}

impl DataDestination {
    pub fn from_adapter(format: DataFormat, adapter: &Option<Adapter>) -> Result<Self, DbError> {
        match format {
            DataFormat::Postgres => match adapter {
                Some(Adapter::Postgres(adapter)) => {
                    let destination = PgDestination::new(adapter.clone());
                    Ok(DataDestination::Database(Arc::new(Mutex::new(destination))))
                }
                _ => panic!("Expected Postgres adapter, but got a different type"),
            },
            DataFormat::MySql => {
                // Add once implemented
                panic!("MySql data destination is not implemented yet")
            }
            other => {
                panic!("Unsupported data destination format: {other:?}");
            }
        }
    }

    pub async fn fetch_meta(&self, table: String) -> Result<TableMetadata, DbError> {
        match &self {
            DataDestination::Database(db) => {
                let db = db.lock().await.adapter();
                let metadata = db.fetch_metadata(&table).await?;
                Ok(metadata)
            }
        }
    }

    pub async fn adapter(&self) -> Arc<(dyn SqlAdapter + Send + Sync)> {
        match &self {
            DataDestination::Database(db) => db.lock().await.adapter(),
        }
    }
}
