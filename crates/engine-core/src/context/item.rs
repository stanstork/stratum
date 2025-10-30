use crate::{
    connectors::{
        destination::{DataDestination, Destination},
        source::{DataSource, Source},
    },
    migration_state::MigrationState,
    state::buffer::SledBuffer,
};
use connectors::{
    error::AdapterError,
    metadata::entity::EntityMetadata,
    sql::base::{
        error::DbError,
        metadata::{provider::MetadataHelper, table::TableMetadata},
    },
};
use futures::lock::Mutex;
use model::transform::mapping::EntityMapping;
use smql_syntax::ast::connection::DataFormat;
use std::{future::Future, sync::Arc};
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
    ) -> Self {
        let state = Arc::new(Mutex::new(state));
        let buffer = Arc::new(SledBuffer::new(&format!(
            "migration_buffer_{}",
            source.name
        )));

        ItemContext {
            state,
            source,
            destination,
            buffer,
            mapping,
        }
    }

    /// Fetch and apply source metadata (table or CSV) into the internal state.
    pub async fn set_src_meta(&self) -> Result<(), AdapterError> {
        // Fetch metadata by source name
        let name = &self.source.name;
        let meta = self.source.primary.fetch_meta(name.clone()).await?;

        // Do nothing if metadata is not valid
        if !meta.is_valid() {
            return Ok(());
        }

        match (&self.source.primary, meta) {
            (DataSource::Database(db), EntityMetadata::Table(table_meta)) => {
                db.lock().await.set_metadata(table_meta);
                Ok(())
            }
            (DataSource::File(file), EntityMetadata::Csv(csv_meta)) => {
                file.lock().await.set_metadata(csv_meta);
                Ok(())
            }
            // Any other combination is an unexpected mismatch
            _ => Err(AdapterError::InvalidMetadata(
                "Mismatch between data source and fetched metadata".into(),
            )),
        }
    }

    pub async fn set_dest_meta(&self) -> Result<(), DbError> {
        let name = &self.destination.name;
        let db = match &self.destination.data_dest {
            DataDestination::Database(db) => Some(db),
        };

        let fetch_meta_fn = |tbl: String| self.destination.data_dest.fetch_meta(tbl);
        Self::set_meta(name, db, fetch_meta_fn).await?;

        Ok(())
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

    async fn set_meta<F, Fut, M>(
        table: &str,
        db: Option<&Arc<Mutex<M>>>,
        fetch_meta_fn: F,
    ) -> Result<(), DbError>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Result<TableMetadata, DbError>>,
        M: MetadataHelper + Send + Sync + ?Sized,
    {
        let db = match db {
            Some(db) => db,
            None => return Ok(()),
        };

        let meta = fetch_meta_fn(table.to_string()).await?;
        if meta.is_valid() {
            db.lock().await.set_metadata(meta);
        }

        Ok(())
    }
}
