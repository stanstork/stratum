use crate::{
    buffer::RecordBuffer, destination::data_dest::DataDestination, source::data_source::DataSource,
    state::MigrationState,
};
use smql::{plan::MigrationPlan, statements::connection::DataFormat};
use sql_adapter::metadata::table::TableMetadata;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use tracing::info;

pub struct MigrationContext {
    pub state: Arc<Mutex<MigrationState>>,
    pub source: DataSource,
    pub destination: DataDestination,
    pub buffer: Arc<RecordBuffer>,
    pub source_data_format: DataFormat,
    pub destination_data_format: DataFormat,
    pub src_dst_name_map: HashMap<String, String>,
}

impl MigrationContext {
    pub fn init(
        source: DataSource,
        destination: DataDestination,
        plan: &MigrationPlan,
    ) -> Arc<Mutex<MigrationContext>> {
        let state = Arc::new(Mutex::new(MigrationState::new()));
        let buffer = Arc::new(RecordBuffer::new("migration_buffer"));
        let source_data_format = plan.connections.source.data_format;
        let destination_data_format = plan.connections.destination.data_format;

        Arc::new(Mutex::new(MigrationContext {
            state,
            source,
            destination,
            buffer,
            source_data_format,
            destination_data_format,
            src_dst_name_map: HashMap::new(),
        }))
    }

    pub async fn get_source_metadata(&self) -> Result<TableMetadata, Box<dyn std::error::Error>> {
        match (&self.source, &self.source_data_format) {
            (DataSource::Database(source), format)
                if format.intersects(DataFormat::sql_databases()) =>
            {
                source.get_metadata().await
            }
            _ => Err("Unsupported data source format".into()),
        }
    }

    pub async fn get_destination_metadata(
        &self,
        table: &str,
    ) -> Result<TableMetadata, Box<dyn std::error::Error>> {
        match (&self.destination, self.destination_data_format) {
            (DataDestination::Database(destination), format)
                if format.intersects(DataFormat::sql_databases()) =>
            {
                destination
                    .lock()
                    .await
                    .adapter()
                    .fetch_metadata(table)
                    .await
            }
            _ => unimplemented!("Unsupported data destination"),
        }
    }

    pub fn set_dst_name(&mut self, src_name: &str, dst_name: &str) {
        self.src_dst_name_map
            .insert(src_name.to_string(), dst_name.to_string());
    }

    pub async fn debug_state(&self) {
        let state = self.state.lock().await;
        info!("State: {:?}", state);
    }
}
