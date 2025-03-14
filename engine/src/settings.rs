use crate::{
    context::MigrationContext,
    destination::{data_dest::DbDataDestination, postgres::PgDestination},
    source::{
        data_source::{DataSource, DbDataSource},
        providers::mysql::MySqlDataSource,
    },
};
use async_trait::async_trait;
use smql::statements::connection::DataFormat;
use std::sync::Arc;
use tokio::sync::Mutex;

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
        let metadata = match context.source_data_format {
            DataFormat::MySql => {
                context
                    .source
                    .as_any()
                    .downcast_ref::<MySqlDataSource>()
                    .ok_or_else(|| "Failed to downcast source to MySqlDataSource")?
                    .get_metadata()
                    .await?
            }
            _ => return Err("Unsupported data source format".into()),
        };

        match context.destination_data_format {
            DataFormat::Postgres => {
                let destination = context
                    .destination
                    .as_any()
                    .downcast_ref::<PgDestination>()
                    .ok_or_else(|| "Failed to downcast destination to PgDestination")?;

                if !destination.table_exists(&metadata.name).await? {
                    println!("Table does not exist, creating table");
                    destination.infer_schema(&metadata).await?;
                }
            }
            _ => unimplemented!("Unsupported data destination"),
        }

        println!("Infer schema setting applied");
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
        println!("Batch size setting applied");
        Ok(())
    }
}
