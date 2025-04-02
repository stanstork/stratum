use crate::state::MigrationState;
use crate::{
    context::MigrationContext, destination::data_dest::DataDestination,
    source::data_source::DataSource,
};
use async_trait::async_trait;
use common::mapping::{NameMap, NamespaceMap};
use postgres::data_type::PgColumnDataType;
use smql::statements::setting::{Setting, SettingValue};
use smql::{plan::MigrationPlan, statements::connection::DataFormat};
use sql_adapter::adapter::SqlAdapter;
use sql_adapter::metadata::column::data_type::ColumnDataType;
use sql_adapter::metadata::provider::MetadataProvider;
use sql_adapter::metadata::table::TableMetadata;
use sql_adapter::schema::plan::SchemaPlan;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

#[async_trait]
pub trait MigrationSetting {
    async fn apply(
        &self,
        plan: &MigrationPlan,
        context: Arc<Mutex<MigrationContext>>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

pub struct InferSchemaSetting {
    source: DataSource,
    source_format: DataFormat,
    destination: DataDestination,
    dest_format: DataFormat,
    table_name_map: NameMap,
    column_name_map: NamespaceMap,
    state: Arc<Mutex<MigrationState>>,
}

pub struct BatchSizeSetting(pub i64);

#[async_trait]
impl MigrationSetting for InferSchemaSetting {
    async fn apply(
        &self,
        plan: &MigrationPlan,
        _context: Arc<Mutex<MigrationContext>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.apply_schema(plan).await?;

        self.set_source_metadata(&plan.migration.sources()).await?;
        self.set_destination_metadata(&plan.migration.targets())
            .await?;

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
        for migration in plan.migration.migrations.iter() {
            if !self.destination_exists(&migration.target).await? {
                info!("Destination table does not exist. Infer schema setting will be applied");

                let schema_plan = self.infer_schema(&migration.sources).await?;

                if let (DataDestination::Database(destination), true) = (
                    &self.destination,
                    self.dest_format.intersects(DataFormat::sql_databases()),
                ) {
                    destination.lock().await.infer_schema(&schema_plan).await?;
                    return Ok(());
                } else {
                    return Err("Unsupported data destination format".into());
                }
            }
        }

        Ok(())
    }

    async fn destination_exists(&self, table: &str) -> Result<bool, Box<dyn std::error::Error>> {
        match &self.destination {
            DataDestination::Database(dest) => Ok(dest.lock().await.table_exists(table).await?),
        }
    }

    async fn infer_schema(
        &self,
        tables: &Vec<String>,
    ) -> Result<SchemaPlan, Box<dyn std::error::Error>> {
        if let (DataSource::Database(source), true) = (
            &self.source,
            self.source_format.intersects(DataFormat::sql_databases()),
        ) {
            let source = source.lock().await;
            let adapter: &(dyn SqlAdapter + Send + Sync) = source.adapter();

            // Build full metadata for the source
            let metadata_graph = MetadataProvider::build_metadata_graph(adapter, &tables).await?;

            SchemaPlan::build(
                adapter,
                metadata_graph,
                self.table_name_map.clone(),
                self.column_name_map.clone(),
                &ColumnDataType::to_pg_type,
                &TableMetadata::enums,
            )
            .await
        } else {
            Err("Unsupported data source format".into())
        }
    }

    pub async fn set_source_metadata(
        &self,
        source_tables: &[String],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let DataSource::Database(src) = &self.source {
            let mut src_guard = src.lock().await;
            let metadata =
                MetadataProvider::build_metadata_graph(src_guard.adapter(), source_tables).await?;
            src_guard.set_metadata(metadata);
        }
        Ok(())
    }

    pub async fn set_destination_metadata(
        &self,
        destination_tables: &[String],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let DataDestination::Database(dest) = &self.destination {
            let mut dest_guard = dest.lock().await;
            let metadata =
                MetadataProvider::build_metadata_graph(dest_guard.adapter(), destination_tables)
                    .await?;
            dest_guard.set_metadata(metadata);
        }
        Ok(())
    }
}

#[async_trait]
impl MigrationSetting for BatchSizeSetting {
    async fn apply(
        &self,
        _plan: &MigrationPlan,
        context: Arc<Mutex<MigrationContext>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let context = context.lock().await;
        let mut state = context.state.lock().await;
        state.batch_size = self.0 as usize;
        info!("Batch size setting applied");
        Ok(())
    }
}

pub async fn parse_settings(
    settings: &[Setting],
    context: &Arc<Mutex<MigrationContext>>,
) -> Vec<Box<dyn MigrationSetting>> {
    let mut migration_settings = Vec::new();
    for setting in settings {
        match (setting.key.as_str(), setting.value.clone()) {
            ("infer_schema", SettingValue::Boolean(true)) => {
                migration_settings
                    .push(Box::new(InferSchemaSetting::new(context).await)
                        as Box<dyn MigrationSetting>)
            }
            ("batch_size", SettingValue::Integer(size)) => migration_settings
                .push(Box::new(BatchSizeSetting(size)) as Box<dyn MigrationSetting>),
            _ => (), // Ignore unknown settings
        }
    }

    migration_settings
}
