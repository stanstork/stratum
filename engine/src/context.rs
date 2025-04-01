use crate::{
    buffer::SledBuffer, destination::data_dest::DataDestination, source::data_source::DataSource,
    state::MigrationState,
};
use common::mapping::{FieldMapping, NameMap, NamespaceMap};
use smql::{plan::MigrationPlan, statements::connection::DataFormat};
use sql_adapter::metadata::table::TableMetadata;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

pub struct MigrationContext {
    pub state: Arc<Mutex<MigrationState>>,
    pub source: DataSource,
    pub destination: DataDestination,
    pub buffer: Arc<SledBuffer>,
    pub source_data_format: DataFormat,
    pub dest_data_format: DataFormat,
    pub name_mapping: NameMap,
    pub field_mapping: NamespaceMap,
}

impl MigrationContext {
    pub fn init(
        source: DataSource,
        destination: DataDestination,
        plan: &MigrationPlan,
    ) -> Arc<Mutex<MigrationContext>> {
        let state = Arc::new(Mutex::new(MigrationState::new()));
        let buffer = Arc::new(SledBuffer::new("migration_buffer"));
        let source_data_format = plan.connections.source.data_format;
        let dest_data_format = plan.connections.destination.data_format;

        let field_mapping = FieldMapping::extract_field_map(&plan.mapping);
        let name_mapping = FieldMapping::extract_name_map(&plan.migration);

        Arc::new(Mutex::new(MigrationContext {
            state,
            source,
            destination,
            buffer,
            source_data_format,
            dest_data_format,
            name_mapping,
            field_mapping,
        }))
    }

    pub async fn get_source_metadata(&self) -> Result<TableMetadata, Box<dyn std::error::Error>> {
        match (&self.source, &self.source_data_format) {
            (DataSource::Database(source), format)
                if format.intersects(DataFormat::sql_databases()) =>
            {
                Ok(source.lock().await.get_metadata().clone())
            }
            _ => Err("Unsupported data source format".into()),
        }
    }

    pub async fn get_dest_metadata(&self) -> Result<TableMetadata, Box<dyn std::error::Error>> {
        match (&self.destination, self.dest_data_format) {
            (DataDestination::Database(destination), format)
                if format.intersects(DataFormat::sql_databases()) =>
            {
                Ok(destination.lock().await.metadata().clone())
            }
            _ => unimplemented!("Unsupported data destination"),
        }
    }

    pub async fn debug_state(&self) {
        let state = self.state.lock().await;
        info!("State: {:?}", state);
    }
}
