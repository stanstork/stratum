use super::{phase::MigrationSettingsPhase, MigrationSetting};
use crate::{
    context::MigrationContext,
    destination::data_dest::DataDestination,
    metadata::{set_destination_metadata, set_source_metadata},
    source::{data_source::DataSource, source::Source},
    state::MigrationState,
};
use async_trait::async_trait;
use common::mapping::{FieldMappings, FieldNameMap};
use postgres::data_type::PgColumnDataType;
use smql::{plan::MigrationPlan, statements::connection::DataFormat};
use sql_adapter::{
    adapter::SqlAdapter,
    metadata::{
        column::{data_type::ColumnDataType, metadata::ColumnMetadata},
        provider::MetadataProvider,
        table::TableMetadata,
    },
    schema::plan::SchemaPlan,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

pub struct InferSchemaSetting {
    source: Source,
    source_format: DataFormat,
    destination: DataDestination,
    dest_format: DataFormat,
    table_name_map: FieldNameMap,
    column_name_map: FieldMappings,
    state: Arc<Mutex<MigrationState>>,
}

#[async_trait]
impl MigrationSetting for InferSchemaSetting {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::InferSchema
    }

    async fn apply(
        &self,
        plan: &MigrationPlan,
        context: Arc<Mutex<MigrationContext>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.apply_schema(plan).await?;

        set_source_metadata(&context, &plan.migration.sources()).await?;
        set_destination_metadata(&context, &plan.migration.targets()).await?;

        // Set the infer schema flag to global state
        {
            let mut state = self.state.lock().await;
            state.infer_schema = true;
        }

        info!("Infer schema setting applied");
        Ok(())
    }
}

impl InferSchemaSetting {
    pub async fn new(context: &Arc<Mutex<MigrationContext>>) -> Self {
        let ctx = context.lock().await;
        InferSchemaSetting {
            source: ctx.source.clone(),
            source_format: ctx.source_format,
            destination: ctx.destination.clone(),
            dest_format: ctx.destination_format,
            table_name_map: ctx.entity_name_map.clone(),
            column_name_map: ctx.field_name_map.clone(),
            state: ctx.state.clone(),
        }
    }

    async fn apply_schema(&self, plan: &MigrationPlan) -> Result<(), Box<dyn std::error::Error>> {
        let type_converter = |meta: &ColumnMetadata| ColumnDataType::to_pg_type(meta);
        let type_extractor = |meta: &TableMetadata| TableMetadata::enums(meta);

        let source_adapter = self.source_adapter().await?;
        let ignore_constraints = self.state.lock().await.ignore_constraints;

        let mut schema_plan = SchemaPlan::new(
            source_adapter,
            &type_converter,
            &type_extractor,
            ignore_constraints,
            self.table_name_map.clone(),
            self.column_name_map.clone(),
        );

        for migration in plan.migration.migrations.iter() {
            if !self.destination_exists(&migration.target).await? {
                info!("Destination table does not exist. Infer schema setting will be applied");
                self.infer_schema(&migration.sources, &mut schema_plan)
                    .await?;
            }
        }

        self.apply_to_destination(schema_plan).await
    }

    async fn destination_exists(&self, table: &str) -> Result<bool, Box<dyn std::error::Error>> {
        match &self.destination {
            DataDestination::Database(dest) => Ok(dest.lock().await.table_exists(table).await?),
        }
    }

    async fn infer_schema(
        &self,
        tables: &Vec<String>,
        schema_plan: &mut SchemaPlan<'_>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let (DataSource::Database(_source), true) = (
            &self.source.primary,
            self.source_format.intersects(DataFormat::sql_databases()),
        ) {
            let adapter = self.source_adapter().await?;

            // Build full metadata for the source
            let metadata_graph = MetadataProvider::build_metadata_graph(&*adapter, &tables).await?;
            for metadata in metadata_graph.values() {
                let table_name = metadata.name.clone();

                if schema_plan.metadata_exists(&table_name) {
                    continue;
                }

                MetadataProvider::collect_schema_deps(&metadata, schema_plan);
                schema_plan.add_metadata(&table_name, metadata.clone());
            }
            Ok(())
        } else {
            Err("Unsupported data source format".into())
        }
    }

    async fn apply_to_destination(
        &self,
        schema_plan: SchemaPlan<'_>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match (
            &self.destination,
            self.dest_format.intersects(DataFormat::sql_databases()),
        ) {
            (DataDestination::Database(destination), true) => {
                let state = self.state.lock().await;
                destination.lock().await.infer_schema(&schema_plan).await?;
                Ok(())
            }
            _ => Err("Unsupported data destination format".into()),
        }
    }

    async fn source_adapter(
        &self,
    ) -> Result<Arc<dyn SqlAdapter + Send + Sync>, Box<dyn std::error::Error>> {
        match &self.source.primary {
            DataSource::Database(source) => Ok(source.lock().await.adapter()),
        }
    }
}
