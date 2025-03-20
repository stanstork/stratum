use crate::{
    context::MigrationContext, destination::data_dest::DataDestination,
    source::data_source::DataSource,
};
use async_trait::async_trait;
use smql::{plan::MigrationPlan, statements::connection::DataFormat};
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
        let (source, source_format, destination, dest_format, state) = {
            let ctx = context.lock().await;
            (
                ctx.source.clone(),
                ctx.source_data_format,
                ctx.destination.clone(),
                ctx.destination_data_format,
                ctx.state.clone(),
            )
        };

        let mut metadata = match (source, source_format) {
            (DataSource::Database(source), format)
                if format.intersects(DataFormat::sql_databases()) =>
            {
                source.get_metadata().await?
            }
            _ => return Err("Unsupported data source format".into()),
        };

        match (&destination, dest_format) {
            (DataDestination::Database(destination), format)
                if format.intersects(DataFormat::sql_databases()) =>
            {
                // Set the metadata name to the target table name
                metadata.name = plan.migration.target.clone();

                let mut dest = destination.lock().await;
                dest.infer_schema(&metadata).await?;
                dest.set_metadata(metadata);
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
