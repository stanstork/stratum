use crate::adapter::Adapter;
use postgres::destination::PgDestination;
use smql::statements::connection::DataFormat;
use sql_adapter::destination::DbDataDestination;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub enum DataDestination {
    Database(Arc<Mutex<dyn DbDataDestination>>),
}

impl DataDestination {
    pub fn from_adapter(
        format: DataFormat,
        adapter: Adapter,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        match format {
            DataFormat::Postgres => match adapter {
                Adapter::Postgres(adapter) => {
                    let destination = PgDestination::new(adapter);
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
}
