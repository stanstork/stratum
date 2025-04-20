use crate::{
    context::MigrationContext,
    destination::{data_dest::DataDestination, destination::Destination},
    source::{data_source::DataSource, source::Source},
    state::MigrationState,
};
use common::mapping::EntityMappingContext;
use postgres::data_type::PgColumnDataType;
use smql::statements::connection::DataFormat;
use sql_adapter::{
    adapter::SqlAdapter,
    metadata::{
        column::{data_type::ColumnDataType, metadata::ColumnMetadata},
        table::TableMetadata,
    },
    schema::plan::SchemaPlan,
};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct SchemaSettingContext {
    pub source: Source,
    pub destination: Destination,
    pub mapping: EntityMappingContext,
    pub state: Arc<Mutex<MigrationState>>,
}

impl SchemaSettingContext {
    pub async fn new(context: &Arc<Mutex<MigrationContext>>) -> Self {
        let ctx = context.lock().await;
        Self {
            source: ctx.source.clone(),
            destination: ctx.destination.clone(),
            mapping: ctx.mapping.clone(),
            state: ctx.state.clone(),
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
            .intersects(DataFormat::sql_databases())
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
        Ok(SchemaPlan::new(
            adapter,
            &|meta: &ColumnMetadata| ColumnDataType::to_pg_type(meta),
            &|meta: &TableMetadata| TableMetadata::enums(meta),
            ignore_constraints,
            self.mapping.clone(),
        ))
    }
}
