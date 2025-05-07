use crate::{
    context::item::ItemContext,
    destination::{data_dest::DataDestination, destination::Destination},
    expr::types::boxed_infer_computed_type,
    source::{data_source::DataSource, source::Source},
    state::MigrationState,
};
use common::mapping::EntityMapping;
use postgres::data_type::PgColumnDataType;
use sql_adapter::{
    adapter::SqlAdapter,
    metadata::{
        column::{data_type::ColumnDataType, metadata::ColumnMetadata},
        table::TableMetadata,
    },
    schema::{plan::SchemaPlan, types::TypeEngine},
};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct SchemaSettingContext {
    pub source: Source,
    pub destination: Destination,
    pub mapping: EntityMapping,
    pub state: Arc<Mutex<MigrationState>>,
}

impl SchemaSettingContext {
    pub fn new(
        src: &Source,
        dest: &Destination,
        mapping: &EntityMapping,
        state: &Arc<Mutex<MigrationState>>,
    ) -> Self {
        Self {
            source: src.clone(),
            destination: dest.clone(),
            mapping: mapping.clone(),
            state: state.clone(),
        }
    }

    pub async fn source_adapter(
        &self,
    ) -> Result<Arc<dyn SqlAdapter + Send + Sync>, Box<dyn std::error::Error>> {
        match &self.source.primary {
            DataSource::Database(src) => Ok(src.lock().await.adapter()),
        }
    }

    pub async fn destination_exists(
        &self,
        table: &str,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        match &self.destination.data_dest {
            DataDestination::Database(dest) => Ok(dest.lock().await.table_exists(table).await?),
        }
    }

    pub async fn apply_to_destination(
        &self,
        schema_plan: SchemaPlan<'_>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self
            .destination
            .format
            .intersects(ItemContext::sql_databases())
        {
            if let DataDestination::Database(dest) = &self.destination.data_dest {
                dest.lock().await.infer_schema(&schema_plan).await?;
                return Ok(());
            }
        }
        Err("Unsupported data destination format".into())
    }

    pub async fn build_schema_plan(&self) -> Result<SchemaPlan<'_>, Box<dyn std::error::Error>> {
        let adapter = self.source_adapter().await?;
        let ignore_constraints = self.state.lock().await.ignore_constraints;
        let type_engine = TypeEngine::new(
            adapter.clone(),
            // converter
            &|meta: &ColumnMetadata| -> (String, Option<usize>) {
                ColumnDataType::to_pg_type(meta)
            },
            // extractor
            &|meta: &TableMetadata| -> Vec<ColumnMetadata> { TableMetadata::enums(meta) },
            // INFERENCER → just the function pointer
            boxed_infer_computed_type,
        );

        Ok(SchemaPlan::new(
            adapter,
            type_engine,
            ignore_constraints,
            self.mapping.clone(),
        ))
    }
}
