use crate::{
    connectors::{
        destination::{DataDestination, Destination},
        source::{DataSource, Source},
    },
    state::sled_store::SledStateStore,
};
use connectors::{
    error::AdapterError,
    metadata::entity::EntityMetadata,
    sql::base::{
        error::DbError,
        metadata::{provider::MetadataStore, table::TableMetadata},
    },
};
use futures::lock::Mutex;
use model::{
    execution::pipeline::Pipeline, pagination::cursor::Cursor,
    transform::mapping::TransformationMetadata,
};
use planner::query::offsets::OffsetStrategy;
use std::{future::Future, sync::Arc};

/// Represents the context for a single item in the migration process.
pub struct ItemContext {
    pub run_id: String,
    pub item_id: String,
    pub source: Source,
    pub destination: Destination,
    pub pipeline: Pipeline,
    pub mapping: TransformationMetadata,
    pub state: Arc<SledStateStore>,
    pub offset_strategy: Arc<dyn OffsetStrategy>,
    pub cursor: Cursor,
}

/// Bundles the arguments required to create an `ItemContext`.
pub struct ItemContextParams {
    pub run_id: String,
    pub item_id: String,
    pub source: Source,
    pub destination: Destination,
    pub pipeline: Pipeline,
    pub mapping: TransformationMetadata,
    pub state: Arc<SledStateStore>,
    pub offset_strategy: Arc<dyn OffsetStrategy>,
    pub cursor: Cursor,
}

impl ItemContext {
    /// Initializes a new `ItemContext` with the provided source, destination, and mapping.
    pub fn new(params: ItemContextParams) -> Self {
        let ItemContextParams {
            run_id,
            item_id,
            source,
            destination,
            pipeline,
            mapping,
            state,
            offset_strategy,
            cursor,
        } = params;

        ItemContext {
            run_id,
            item_id,
            source,
            destination,
            pipeline,
            mapping,
            state,
            offset_strategy,
            cursor,
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
            DataDestination::Database(db) => Some(&db.data),
        };

        let fetch_meta_fn = |tbl: String| self.destination.data_dest.fetch_meta(tbl);
        Self::set_meta(name, db, fetch_meta_fn).await?;

        Ok(())
    }

    async fn set_meta<F, Fut, M>(
        table: &str,
        db: Option<&Arc<Mutex<M>>>,
        fetch_meta_fn: F,
    ) -> Result<(), DbError>
    where
        F: Fn(String) -> Fut,
        Fut: Future<Output = Result<TableMetadata, DbError>>,
        M: MetadataStore + Send + Sync + ?Sized,
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
