use crate::connectors::sink::{Sink, postgres::PostgresSink};
use connectors::{
    adapter::Adapter,
    sql::{
        base::{
            adapter::SqlAdapter, destination::DbDataDestination, error::DbError,
            metadata::table::TableMetadata,
        },
        postgres::destination::PgDestination,
    },
};
use futures::lock::Mutex;
use model::records::record::Record;
use planner::query::dialect;
use smql_syntax::ast::connection::DataFormat;
use std::sync::Arc;

#[derive(Clone)]
pub struct DatabaseDestination {
    pub data: Arc<Mutex<dyn DbDataDestination<Error = DbError>>>,
    pub sink: Arc<dyn Sink + Send + Sync>,
}

#[derive(Clone)]
pub enum DataDestination {
    Database(DatabaseDestination),
}

#[derive(Clone)]
pub struct Destination {
    pub name: String,
    pub format: DataFormat,
    pub data_dest: DataDestination,
}

impl DataDestination {
    pub fn from_adapter(format: DataFormat, adapter: &Option<Adapter>) -> Result<Self, DbError> {
        match format {
            DataFormat::Postgres => match adapter {
                Some(Adapter::Postgres(adapter)) => {
                    let destination = PgDestination::new(adapter.clone());
                    let sink = Arc::new(PostgresSink::new(adapter.clone()));
                    Ok(DataDestination::Database(DatabaseDestination {
                        data: Arc::new(Mutex::new(destination)),
                        sink,
                    }))
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
                let db = db.data.lock().await.adapter();
                let metadata = db.table_metadata(&table).await?;
                Ok(metadata)
            }
        }
    }

    pub async fn adapter(&self) -> Arc<dyn SqlAdapter + Send + Sync> {
        match &self {
            DataDestination::Database(db) => db.data.lock().await.adapter(),
        }
    }
}

impl Destination {
    pub fn new(name: String, format: DataFormat, data_dest: DataDestination) -> Self {
        Destination {
            name,
            format,
            data_dest,
        }
    }

    pub async fn write_batch(
        &self,
        metadata: &TableMetadata,
        records: Vec<Record>,
    ) -> Result<(), DbError> {
        match &self.data_dest {
            DataDestination::Database(db) => {
                db.data
                    .lock()
                    .await
                    .write_batch(
                        metadata,
                        records
                            .iter()
                            .filter_map(|r| r.to_row_data())
                            .cloned()
                            .collect(),
                    )
                    .await
            }
        }
    }

    pub async fn toggle_trigger(&self, table: &str, enable: bool) -> Result<(), DbError> {
        match &self.data_dest {
            DataDestination::Database(db) => {
                db.data.lock().await.toggle_trigger(table, enable).await
            }
        }
    }

    pub fn dialect(&self) -> Box<dyn dialect::Dialect> {
        Box::new(dialect::Postgres) // Currently only Postgres is supported
    }

    pub fn name(&self) -> String {
        self.name.clone()
    }
}
