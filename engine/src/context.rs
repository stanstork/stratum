use crate::{
    buffer::SledBuffer,
    destination::{data_dest::DataDestination, destination::Destination},
    source::{data_source::DataSource, source::Source},
    state::MigrationState,
};
use common::mapping::EntityMappingContext;
use smql::statements::connection::DataFormat;
use sql_adapter::metadata::table::TableMetadata;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

pub struct MigrationContext {
    /// Shared migration state used for coordination and progress tracking
    pub state: Arc<Mutex<MigrationState>>,

    /// Input data source (e.g., databases, files)
    pub source: Source,

    /// Output data destination (e.g., databases, files)
    pub destination: Destination,

    /// Temporary storage for intermediate migration data
    ///
    /// This buffer is backed by Sled (a high-performance embedded key-value store)
    /// and helps facilitate efficient data transfer between sources and destinations.
    pub buffer: Arc<SledBuffer>,

    /// Mapping of entity names between source and destination
    pub mapping: EntityMappingContext,
}

impl MigrationContext {
    pub fn init(
        source: Source,
        destination: Destination,
        mapping: EntityMappingContext,
    ) -> Arc<Mutex<MigrationContext>> {
        let state = Arc::new(Mutex::new(MigrationState::new()));
        let buffer = Arc::new(SledBuffer::new("migration_buffer"));

        Arc::new(Mutex::new(MigrationContext {
            state,
            source,
            destination,
            buffer,
            mapping,
        }))
    }

    pub async fn get_source_metadata(
        &self,
        source_name: &str,
    ) -> Result<TableMetadata, Box<dyn std::error::Error>> {
        match (&self.source.primary, &self.source.format) {
            (DataSource::Database(db), format)
                if format.intersects(DataFormat::sql_databases()) =>
            {
                Ok(db.lock().await.get_metadata(source_name).clone())
            }
            _ => Err("Unsupported data source format".into()),
        }
    }

    pub async fn get_destination_metadata(
        &self,
        destination_name: &str,
    ) -> Result<TableMetadata, Box<dyn std::error::Error>> {
        match (&self.destination.data_dest, &self.destination.format) {
            (DataDestination::Database(db), format)
                if format.intersects(DataFormat::sql_databases()) =>
            {
                Ok(db.lock().await.get_metadata(destination_name).clone())
            }
            _ => Err("Unsupported data destination format".into()),
        }
    }

    pub async fn debug_state(&self) {
        let state = self.state.lock().await;
        info!("State: {:?}", state);
    }
}
