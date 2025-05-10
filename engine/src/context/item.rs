use crate::{
    buffer::SledBuffer,
    destination::{data::DataDestination, Destination},
    source::{data::DataSource, Source},
    state::MigrationState,
};
use common::mapping::EntityMapping;
use smql::statements::connection::DataFormat;
use sql_adapter::{
    error::db::DbError,
    metadata::{
        provider::{MetadataHelper, MetadataProvider},
        table::TableMetadata,
    },
};
use std::{future::Future, sync::Arc};
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

    pub async fn set_src_meta(&self) -> Result<(), DbError> {
        // Extract the table name and flags
        let name = self.source.name.clone();
        let cascade_schema = {
            let state = self.state.lock().await;
            state.cascade_schema
        };

        // Only proceed if primary source is a Database
        let db_opt = match &self.source.primary {
            DataSource::Database(db) => Some(db.clone()),
        };

        let fetch_meta = move |tbl: String| self.source.primary.fetch_meta(tbl);
        Self::set_meta(&name.clone(), db_opt.as_ref(), fetch_meta).await?;

        // If cascade_schema is enabled, build the metadata graph and set it
        if cascade_schema {
            if let Some(db_mutex) = db_opt {
                let mut db_src = db_mutex.lock().await;
                let adapter = db_src.adapter();
                let graph =
                    MetadataProvider::build_metadata_graph(adapter.as_ref(), &[name.clone()])
                        .await?;
                db_src.set_related_meta(graph);
            }
        }

        Ok(())
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

        db.lock().await.set_metadata(meta);
        Ok(())
    }
}
