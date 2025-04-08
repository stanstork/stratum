use crate::{
    context::MigrationContext, destination::data_dest::DataDestination,
    source::data_source::DataSource,
};
use async_trait::async_trait;
use batch_size::BatchSizeSetting;
use infer_schema::InferSchemaSetting;
use smql::{
    plan::MigrationPlan,
    statements::setting::{Setting, SettingValue},
};
use sql_adapter::metadata::provider::MetadataProvider;
use std::sync::Arc;
use tokio::sync::Mutex;

pub mod batch_size;
pub mod create_cols;
pub mod infer_schema;

#[async_trait]
pub trait MigrationSetting {
    async fn apply(
        &self,
        plan: &MigrationPlan,
        context: Arc<Mutex<MigrationContext>>,
    ) -> Result<(), Box<dyn std::error::Error>>;
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
            ("create_missing_columns", SettingValue::Boolean(true)) => migration_settings
                .push(Box::new(create_cols::CreateMissingColumnsSetting::new())
                    as Box<dyn MigrationSetting>),
            _ => (), // Ignore unknown settings
        }
    }

    migration_settings
}

pub async fn set_source_metadata(
    context: &Arc<Mutex<MigrationContext>>,
    source_tables: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let context = context.lock().await;
    if let DataSource::Database(src) = &context.source {
        let mut src_guard = src.lock().await;
        let metadata =
            MetadataProvider::build_metadata_graph(src_guard.adapter().as_ref(), source_tables)
                .await?;
        src_guard.set_metadata(metadata);
    }
    Ok(())
}

pub async fn set_destination_metadata(
    context: &Arc<Mutex<MigrationContext>>,
    destination_tables: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let context = context.lock().await;
    if let DataDestination::Database(dest) = &context.destination {
        let mut dest_guard = dest.lock().await;
        let metadata = MetadataProvider::build_metadata_graph(
            dest_guard.adapter().as_ref(),
            destination_tables,
        )
        .await?;
        dest_guard.set_metadata(metadata);
    }
    Ok(())
}
