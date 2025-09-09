use common::record::Record;
use data::DataDestination;
use query_builder::dialect;
use smql::statements::connection::DataFormat;
use sql_adapter::{error::db::DbError, metadata::table::TableMetadata};

pub mod data;

#[derive(Clone)]
pub struct Destination {
    pub name: String,
    pub format: DataFormat,
    pub data_dest: DataDestination,
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
                db.lock()
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
            DataDestination::Database(db) => db.lock().await.toggle_trigger(table, enable).await,
        }
    }

    pub fn dialect(&self) -> Box<dyn dialect::Dialect> {
        Box::new(dialect::Postgres) // Currently only Postgres is supported
    }
}
