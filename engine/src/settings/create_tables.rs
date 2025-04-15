use crate::{
    context::MigrationContext,
    destination::data_dest::DataDestination,
    metadata::fetch_source_metadata,
    source::{data_source::DataSource, source::Source},
    state::MigrationState,
};
use async_trait::async_trait;
use common::mapping::{EntityFieldsMap, NameMap};
use postgres::data_type::PgColumnDataType;
use smql::statements::connection::DataFormat;
use sql_adapter::{
    adapter::SqlAdapter,
    metadata::{
        column::{data_type::ColumnDataType, metadata::ColumnMetadata},
        fk,
        table::TableMetadata,
    },
    schema::plan::SchemaPlan,
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use super::MigrationSetting;

pub struct CreateMissingTablesSetting {
    source: Source,
    source_format: DataFormat,
    destination: DataDestination,
    dest_format: DataFormat,
    table_name_map: NameMap,
    column_name_map: EntityFieldsMap,
    state: Arc<Mutex<MigrationState>>,
}

#[async_trait]
impl MigrationSetting for CreateMissingTablesSetting {
    async fn apply(
        &self,
        plan: &smql::plan::MigrationPlan,
        context: Arc<Mutex<MigrationContext>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let context = context.lock().await;

        let type_converter = |meta: &ColumnMetadata| ColumnDataType::to_pg_type(meta);
        let type_extractor = |meta: &TableMetadata| TableMetadata::enums(meta);

        let source_adapter = self.source_adapter().await?;

        let mut schema_plan = SchemaPlan::new(
            source_adapter,
            &type_converter,
            &type_extractor,
            self.table_name_map.clone(),
            self.column_name_map.clone(),
        );

        for destination in plan.migration.targets() {
            if self.destination_exists(&destination).await? {
                continue;
            }

            let dest_name = destination.clone();
            let src_name = context.entity_name_map.reverse_resolve(&dest_name);

            let metadata = fetch_source_metadata(&context.source.data_source, &src_name).await?;

            schema_plan.add_column_defs(
                &metadata.name,
                metadata.column_defs(schema_plan.type_converter()),
            );

            let fk_defs = metadata.fk_defs();
            for fk in fk_defs {
                let target = fk.referenced_table.clone();
                if context.entity_name_map.contains_key(&target) {
                    schema_plan.add_fk_def(&metadata.name, fk.clone());
                }
            }

            for col in (schema_plan.type_extractor())(&metadata) {
                schema_plan.add_enum_def(&metadata.name, &col.name);
            }

            schema_plan.add_metadata(&src_name, metadata.clone());
        }

        self.apply_to_destination(schema_plan).await?;

        info!("Create missing tables setting applied");

        // Set the create missing tables flag to global state
        {
            let mut state = self.state.lock().await;
            state.create_missing_tables = true;
        }

        Ok(())
    }
}

impl CreateMissingTablesSetting {
    pub async fn new(context: &Arc<Mutex<MigrationContext>>) -> Self {
        let ctx = context.lock().await;
        CreateMissingTablesSetting {
            source: ctx.source.clone(),
            source_format: ctx.source_format,
            destination: ctx.destination.clone(),
            dest_format: ctx.destination_format,
            table_name_map: ctx.entity_name_map.clone(),
            column_name_map: ctx.field_name_map.clone(),
            state: ctx.state.clone(),
        }
    }

    async fn source_adapter(
        &self,
    ) -> Result<Arc<dyn SqlAdapter + Send + Sync>, Box<dyn std::error::Error>> {
        match &self.source.data_source {
            DataSource::Database(source) => Ok(source.lock().await.adapter()),
        }
    }

    async fn destination_exists(&self, table: &str) -> Result<bool, Box<dyn std::error::Error>> {
        match &self.destination {
            DataDestination::Database(dest) => Ok(dest.lock().await.table_exists(table).await?),
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
                destination.lock().await.infer_schema(&schema_plan).await?;
                Ok(())
            }
            _ => Err("Unsupported data destination format".into()),
        }
    }
}
