use crate::{
    buffer::SledBuffer,
    destination::{data_dest::DataDestination, destination::Destination},
    source::{data_source::DataSource, source::Source},
    state::MigrationState,
};
use common::mapping::EntityMapping;
use smql_v02::statements::connection::DataFormat;
use sql_adapter::metadata::table::TableMetadata;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

/// Represents the context for a single item in the migration process.
pub struct ItemContext {
    /// Shared migration state used for coordination and progress tracking.
    pub state: Arc<Mutex<MigrationState>>,

    /// Input data source (e.g., databases, files).
    pub source: Source,

    /// Output data destination (e.g., databases, files).
    pub destination: Destination,

    /// Temporary storage for intermediate migration data
    ///
    /// This buffer is backed by Sled (a high-performance embedded key-value store)
    /// and helps facilitate efficient data transfer between sources and destinations.
    pub buffer: Arc<SledBuffer>,

    /// Mapping of entity names between source and destination.
    pub mapping: EntityMapping,
}

impl ItemContext {
    /// Initializes a new `ItemContext` with the provided source, destination, and mapping.
    pub fn new(
        source: Source,
        destination: Destination,
        mapping: EntityMapping,
        state: MigrationState,
    ) -> Arc<Mutex<ItemContext>> {
        let state = Arc::new(Mutex::new(state));
        let buffer = Arc::new(SledBuffer::new(&format!(
            "migration_buffer_{}",
            source.name
        )));

        Arc::new(Mutex::new(ItemContext {
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
            (DataSource::Database(db), format) if format.intersects(Self::sql_databases()) => {
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
            (DataDestination::Database(db), format) if format.intersects(Self::sql_databases()) => {
                Ok(db.lock().await.get_metadata(destination_name).clone())
            }
            _ => Err("Unsupported data destination format".into()),
        }
    }

    pub async fn debug_state(&self) {
        let state = self.state.lock().await;
        info!("State: {:#?}", state);
    }

    pub fn sql_databases() -> DataFormat {
        DataFormat::MySql
            .union(DataFormat::Postgres)
            .union(DataFormat::Sqlite)
    }
}
