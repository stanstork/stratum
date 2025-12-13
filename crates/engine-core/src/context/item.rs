use crate::{
    connectors::{
        destination::{DataDestination, Destination},
        source::{DataSource, Source},
    },
    context::exec::ExecutionContext,
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
    pub exec_ctx: Arc<ExecutionContext>,
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
    pub fn builder(exec_ctx: Arc<ExecutionContext>) -> ItemContextBuilder {
        ItemContextBuilder::new(exec_ctx)
    }
}

pub struct ItemContextBuilder {
    exec_ctx: Arc<ExecutionContext>,
    run_id: Option<String>,
    item_id: Option<String>,
    source: Option<Source>,
    destination: Option<Destination>,
    pipeline: Option<Pipeline>,
    mapping: Option<TransformationMetadata>,
    state: Option<Arc<SledStateStore>>,
    offset_strategy: Option<Arc<dyn OffsetStrategy>>,
    cursor: Option<Cursor>,
}

impl ItemContextBuilder {
    fn new(exec_ctx: Arc<ExecutionContext>) -> Self {
        Self {
            exec_ctx,
            run_id: None,
            item_id: None,
            source: None,
            destination: None,
            pipeline: None,
            mapping: None,
            state: None,
            offset_strategy: None,
            cursor: None,
        }
    }

    pub fn run_id(mut self, run_id: String) -> Self {
        self.run_id = Some(run_id);
        self
    }

    pub fn item_id(mut self, item_id: String) -> Self {
        self.item_id = Some(item_id);
        self
    }

    pub fn source(mut self, source: Source) -> Self {
        self.source = Some(source);
        self
    }

    pub fn destination(mut self, destination: Destination) -> Self {
        self.destination = Some(destination);
        self
    }

    pub fn pipeline(mut self, pipeline: Pipeline) -> Self {
        self.pipeline = Some(pipeline);
        self
    }

    pub fn mapping(mut self, mapping: TransformationMetadata) -> Self {
        self.mapping = Some(mapping);
        self
    }

    pub fn state(mut self, state: Arc<SledStateStore>) -> Self {
        self.state = Some(state);
        self
    }

    pub fn offset_strategy(mut self, offset_strategy: Arc<dyn OffsetStrategy>) -> Self {
        self.offset_strategy = Some(offset_strategy);
        self
    }

    pub fn cursor(mut self, cursor: Cursor) -> Self {
        self.cursor = Some(cursor);
        self
    }

    pub fn build(self) -> ItemContext {
        ItemContext {
            exec_ctx: self.exec_ctx,
            run_id: self.run_id.expect("run_id is required"),
            item_id: self.item_id.expect("item_id is required"),
            source: self.source.expect("source is required"),
            destination: self.destination.expect("destination is required"),
            pipeline: self.pipeline.expect("pipeline is required"),
            mapping: self.mapping.expect("mapping is required"),
            state: self.state.expect("state is required"),
            offset_strategy: self.offset_strategy.expect("offset_strategy is required"),
            cursor: self.cursor.unwrap_or(Cursor::None),
        }
    }
}

impl ItemContext {
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
