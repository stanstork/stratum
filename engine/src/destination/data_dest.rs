use crate::adapter::Adapter;
use postgres::destination::PgDestination;
use smql_v02::statements::connection::DataFormat;
use sql_adapter::{destination::DbDataDestination, metadata::table::TableMetadata};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub enum DataDestination {
    Database(Arc<Mutex<dyn DbDataDestination>>),
}

impl DataDestination {
    pub fn from_adapter(
        format: DataFormat,
        adapter: &Adapter,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        match format {
            DataFormat::Postgres => match adapter {
                Adapter::Postgres(adapter) => {
                    let destination = PgDestination::new(adapter.clone());
                    Ok(DataDestination::Database(Arc::new(Mutex::new(destination))))
                }
                _ => Err("Expected Postgres adapter, but got a different type".into()),
            },
            DataFormat::MySql => {
                // Add once implemented
                Err("MySql data destination is not implemented yet".into())
            }
            other => Err(format!("Unsupported data source format: {:?}", other).into()),
        }
    }

    pub async fn fetch_meta(
        &self,
        table: &str,
    ) -> Result<TableMetadata, Box<dyn std::error::Error>> {
        match &self {
            DataDestination::Database(db) => {
                let db = db.lock().await.adapter();
                let metadata = db.fetch_metadata(table).await?;
                Ok(metadata)
            }
        }
    }
}
