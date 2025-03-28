use crate::mapping::field::FieldMapping;
use crate::state::MigrationState;
use crate::{
    context::MigrationContext, destination::data_dest::DataDestination,
    source::data_source::DataSource,
};
use async_trait::async_trait;
use postgres::data_type::PgColumnDataType;
use smql::statements::setting::{Setting, SettingValue};
use smql::{plan::MigrationPlan, statements::connection::DataFormat};
use sql_adapter::metadata::column::data_type::ColumnDataType;
use sql_adapter::metadata::provider::MetadataProvider;
use sql_adapter::metadata::table::TableMetadata;
use sql_adapter::schema::mapping::NameMap;
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
    state: Arc<Mutex<MigrationState>>,
}

pub struct BatchSizeSetting(pub i64);

#[async_trait]
impl MigrationSetting for InferSchemaSetting {
    async fn apply(
        &self,
        plan: &MigrationPlan,
        context: Arc<Mutex<MigrationContext>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Apply the schema only if the destination table does not exist
        if !self.destination_exists(plan).await? {
            info!("Destination table does not exist. Infer schema setting will be applied");

            let cx = context.lock().await;

            let col_mapping = NameMap::new(FieldMapping::extract_field_map(&plan.mapping));
            let table_mapping = NameMap::new(cx.name_mapping.clone());
            let schema_plan = self.infer_schema(table_mapping, col_mapping).await?;

            self.apply_schema(&schema_plan).await?;
        }

        self.set_metadata(plan).await?;

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
            source_format: ctx.source_data_format,
            destination: ctx.destination.clone(),
            dest_format: ctx.dest_data_format,
            state: ctx.state.clone(),
        }
    }

    async fn infer_schema(
        &self,
        table_name_map: NameMap,
        column_name_map: NameMap,
    ) -> Result<SchemaPlan, Box<dyn std::error::Error>> {
        if let (DataSource::Database(source), true) = (
            &self.source,
            self.source_format.intersects(DataFormat::sql_databases()),
        ) {
            let src = source.lock().await;
            let adapter = src.adapter();

            // Build full metadata for the source
            let metadata =
                MetadataProvider::build_metadata_with_deps(adapter, &src.table_name()).await?;

            SchemaPlan::build(
                adapter,
                metadata,
                table_name_map,
                column_name_map,
                &ColumnDataType::to_pg_type,
                &TableMetadata::enums,
            )
            .await
        } else {
            Err("Unsupported data source format".into())
        }
    }

    async fn apply_schema(
        &self,
        schema_plan: &SchemaPlan,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let (DataDestination::Database(destination), true) = (
            &self.destination,
            self.dest_format.intersects(DataFormat::sql_databases()),
        ) {
            destination.lock().await.infer_schema(schema_plan).await?;
            Ok(())
        } else {
            Err("Unsupported data destination format".into())
        }
    }

    async fn destination_exists(
        &self,
        plan: &MigrationPlan,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        match &self.destination {
            DataDestination::Database(dest) => Ok(dest
                .lock()
                .await
                .table_exists(&plan.migration.target)
                .await?),
        }
    }

    async fn set_metadata(&self, plan: &MigrationPlan) -> Result<(), Box<dyn std::error::Error>> {
        match &self.source {
            DataSource::Database(src) => {
                let mut src_guard = src.lock().await;
                let metadata = MetadataProvider::build_metadata_with_deps(
                    src_guard.adapter(),
                    &src_guard.table_name(),
                )
                .await?;
                src_guard.set_metadata(metadata);
            }
        }

        match &self.destination {
            DataDestination::Database(dest) => {
                let mut dest_guard = dest.lock().await;
                let metadata = MetadataProvider::build_metadata_with_deps(
                    dest_guard.adapter(),
                    &plan.migration.target,
                )
                .await?;
                dest_guard.set_metadata(metadata);
            }
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
