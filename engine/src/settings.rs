use crate::schema_plan::SchemaPlan;
use crate::{
    context::MigrationContext, destination::data_dest::DataDestination,
    source::data_source::DataSource,
};
use async_trait::async_trait;
use postgres::data_type::ColumnDataTypeMapper;
use smql::{plan::MigrationPlan, statements::connection::DataFormat};
use sql_adapter::metadata::column::data_type::ColumnDataType;
use sql_adapter::metadata::column::metadata::ColumnMetadata;
use sql_adapter::metadata::table::TableMetadata;
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
        let (source, source_format, destination, dest_format, state, src_name) = {
            let ctx = context.lock().await;
            let source = ctx.source.clone();
            let src_name = source.source_name().to_owned();

            (
                ctx.source.clone(),
                ctx.source_data_format,
                ctx.destination.clone(),
                ctx.destination_data_format,
                ctx.state.clone(),
                src_name,
            )
        };

        let mut schema_plan = match (source, source_format) {
            (DataSource::Database(source), format)
                if format.intersects(DataFormat::sql_databases()) =>
            {
                let metadata = source.get_metadata().await?;
                let type_converter = |col: &ColumnMetadata| match &col.data_type {
                    ColumnDataType::Enum => col.name.clone(),
                    ColumnDataType::Set => "TEXT[]".to_string(),
                    _ => ColumnDataType::to_pg_string(&col.data_type),
                };

                SchemaPlan::build(
                    &*source,
                    metadata,
                    &type_converter,
                    &TableMetadata::collect_enum_types,
                )
                .await?
            }
            _ => return Err("Unsupported data source format".into()),
        };

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

                let mut dest = destination.lock().await;

                dest.infer_schema(&schema_plan).await?;
                dest.set_metadata(schema_plan.metadata);
            }
            _ => return Err("Unsupported data destination format".into()),
        }

        {
            let mut state_guard = state.lock().await;
            state_guard.infer_schema = true;
        }

        info!("Infer schema setting applied");
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
