use crate::{
    context::MigrationContext, destination::data_dest::DataDestination,
    source::data_source::DataSource,
};
use async_trait::async_trait;
use smql::statements::connection::DataFormat;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

#[async_trait]
pub trait MigrationSetting {
    async fn apply(
        &self,
        context: Arc<Mutex<MigrationContext>>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

pub struct InferSchemaSetting;
pub struct BatchSizeSetting(pub i64);

#[async_trait]
impl MigrationSetting for InferSchemaSetting {
    async fn apply(
        &self,
        context: Arc<Mutex<MigrationContext>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let context = context.lock().await;
        let metadata = match (&context.source, &context.source_data_format) {
            (DataSource::Database(source), format)
                if format.intersects(DataFormat::sql_databases()) =>
            {
                source.get_metadata().await?
            }
            _ => return Err("Unsupported data source format".into()),
        };

        match (&context.destination, context.destination_data_format) {
            (DataDestination::Database(destination), format)
                if format.intersects(DataFormat::sql_databases()) =>
            {
                destination.infer_schema(&metadata).await?;
            }
            _ => unimplemented!("Unsupported data destination"),
        }

        info!("Infer schema setting applied");
        Ok(())
    }
}

#[async_trait]
impl MigrationSetting for BatchSizeSetting {
    async fn apply(
        &self,
        context: Arc<Mutex<MigrationContext>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let context = context.lock().await;
        let mut state = context.state.lock().await;
        state.batch_size = self.0 as usize;
        info!("Batch size setting applied");
        Ok(())
    }
}
