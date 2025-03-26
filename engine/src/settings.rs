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
use sql_adapter::schema_plan::SchemaPlan;
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

pub struct InferSchemaSetting;
pub struct BatchSizeSetting(pub i64);

#[async_trait]
impl MigrationSetting for InferSchemaSetting {
    async fn apply(
        &self,
        plan: &MigrationPlan,
        context: Arc<Mutex<MigrationContext>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (source, source_format, destination, dest_format, state, src_name) =
            self.extract_context(context.clone()).await;

        let mut schema_plan = self.build_schema_plan(source, source_format).await?;

        self.process_destination(
            destination,
            dest_format,
            &mut schema_plan,
            plan,
            context,
            &src_name,
        )
        .await?;

        state.lock().await.infer_schema = true;

        info!("Infer schema setting applied");
        Ok(())
    }
}

impl InferSchemaSetting {
    async fn extract_context(
        &self,
        context: Arc<Mutex<MigrationContext>>,
    ) -> (
        DataSource,
        DataFormat,
        DataDestination,
        DataFormat,
        Arc<Mutex<MigrationState>>,
        String,
    ) {
        let (source, source_format, destination, dest_format, state, src_name) = {
            let ctx = context.lock().await;
            let source = ctx.source.clone();
            let src_name = source.source_name().await.to_owned();

            (
                ctx.source.clone(),
                ctx.source_data_format,
                ctx.destination.clone(),
                ctx.destination_data_format,
                ctx.state.clone(),
                src_name,
            )
        };

        (
            source,
            source_format,
            destination,
            dest_format,
            state,
            src_name,
        )
    }

    async fn build_schema_plan(
        &self,
        source: DataSource,
        source_format: DataFormat,
    ) -> Result<SchemaPlan, Box<dyn std::error::Error>> {
        match (source, source_format) {
            (DataSource::Database(source), format)
                if format.intersects(DataFormat::sql_databases()) =>
            {
                let metadata = source.lock().await.get_metadata().await?;
                SchemaPlan::build(
                    source.lock().await.adapter(),
                    metadata,
                    &ColumnDataType::convert_pg_column_type,
                    &TableMetadata::collect_enum_types,
                )
                .await
            }
            _ => Err("Unsupported data source format".into()),
        }
    }

    async fn process_destination(
        &self,
        destination: DataDestination,
        dest_format: DataFormat,
        schema_plan: &mut SchemaPlan,
        plan: &MigrationPlan,
        context: Arc<Mutex<MigrationContext>>,
        src_name: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match (&destination, dest_format) {
            (DataDestination::Database(destination), format)
                if format.intersects(DataFormat::sql_databases()) =>
            {
                if src_name != plan.migration.target {
                    context
                        .lock()
                        .await
                        .set_dst_name(&plan.migration.target, &src_name);
                }

                // Set the metadata name to the target table name
                schema_plan.metadata.name = plan.migration.target.clone();

                let mut dest_guard = destination.lock().await;
                dest_guard.infer_schema(&schema_plan).await?;

                let dest_metadata = MetadataProvider::build_table_metadata(
                    dest_guard.adapter(),
                    &plan.migration.target,
                )
                .await?;

                dest_guard.set_metadata(dest_metadata);
            }
            _ => return Err("Unsupported data destination format".into()),
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

pub fn parse_settings(settings: &[Setting]) -> Vec<Box<dyn MigrationSetting>> {
    settings
        .iter()
        .filter_map(
            |setting| match (setting.key.as_str(), setting.value.clone()) {
                ("infer_schema", SettingValue::Boolean(true)) => {
                    Some(Box::new(InferSchemaSetting) as Box<dyn MigrationSetting>)
                }
                ("batch_size", SettingValue::Integer(size)) => {
                    Some(Box::new(BatchSizeSetting(size)) as Box<dyn MigrationSetting>)
                }
                _ => None, // Ignore unknown settings
            },
        )
        .collect()
}
