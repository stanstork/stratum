use super::data_dest::DataDestination;
use crate::record::Record;
use smql_v02::statements::connection::DataFormat;
use sql_adapter::metadata::table::TableMetadata;

#[derive(Clone)]
pub struct Destination {
    /// Format of the destination data (e.g., SQL, CSV, JSON)
    pub format: DataFormat,
    pub data_dest: DataDestination,
}

impl Destination {
    pub fn new(format: DataFormat, data_dest: DataDestination) -> Self {
        Destination { format, data_dest }
    }

    pub async fn write_batch(
        &self,
        metadata: &TableMetadata,
        records: Vec<Record>,
    ) -> Result<(), Box<dyn std::error::Error>> {
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

    pub async fn toggle_trigger(
        &self,
        table: &str,
        enable: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match &self.data_dest {
            DataDestination::Database(db) => db.lock().await.toggle_trigger(table, enable).await,
        }
    }
}
