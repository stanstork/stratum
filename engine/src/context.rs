use crate::{
    buffer::SledBuffer,
    destination::data_dest::DataDestination,
    source::{data_source::DataSource, source::Source},
    state::MigrationState,
};
use common::mapping::{FieldMappings, FieldNameMap};
use smql::{
    plan::MigrationPlan,
    statements::{connection::DataFormat, load::Load},
};
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
    pub destination: DataDestination,

    /// Temporary storage for intermediate migration data
    ///
    /// This buffer is backed by Sled (a high-performance embedded key-value store)
    /// and helps facilitate efficient data transfer between sources and destinations.
    pub buffer: Arc<SledBuffer>,

    /// Format of the source data (e.g., SQL, CSV, JSON)
    pub source_format: DataFormat,

    /// Format of the destination data (e.g., SQL, CSV, JSON)
    pub destination_format: DataFormat,

    /// Maps source entity names to destination entity names
    ///
    /// Typically used for renaming tables or collections.
    pub entity_name_map: FieldNameMap,

    /// Maps source field names to destination field names
    ///
    /// Typically used for renaming columns or attributes.
    pub field_name_map: FieldMappings,

    /// Data to be loaded outside of source
    /// Used for loading data from files or other sources
    /// and combining it with the main data source for migration.
    ///
    /// This is a list of load operations that will be performed
    pub loads: Vec<Load>,
}

impl MigrationContext {
    pub fn init(
        source: Source,
        destination: DataDestination,
        plan: &MigrationPlan,
    ) -> Arc<Mutex<MigrationContext>> {
        let state = Arc::new(Mutex::new(MigrationState::new()));
        let buffer = Arc::new(SledBuffer::new("migration_buffer"));
        let source_format = plan.connections.source.data_format;
        let destination_format = plan.connections.destination.data_format;

        let entity_name_map = FieldNameMap::get_field_name_map(plan);
        let field_name_map = FieldNameMap::get_field_mappings(&plan.mapping);

        println!("Field name map: {:?}", field_name_map);

        let loads = plan.loads.clone();

        Arc::new(Mutex::new(MigrationContext {
            state,
            source,
            destination,
            buffer,
            source_format,
            destination_format,
            entity_name_map,
            field_name_map,
            loads,
        }))
    }

    pub async fn get_source_metadata(
        &self,
        source_name: &str,
    ) -> Result<TableMetadata, Box<dyn std::error::Error>> {
        match (&self.source.primary, &self.source_format) {
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
        match (&self.destination, &self.destination_format) {
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
