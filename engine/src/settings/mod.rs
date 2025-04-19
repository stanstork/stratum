use crate::context::MigrationContext;
use async_trait::async_trait;
use batch_size::BatchSizeSetting;
use constraints::IgnoreConstraintsSettings;
use create_tables::CreateMissingTablesSetting;
use infer_schema::InferSchemaSetting;
use phase::MigrationSettingsPhase;
use smql::{
    plan::MigrationPlan,
    statements::setting::{Setting, SettingValue},
};
use std::sync::Arc;
use tokio::sync::Mutex;

pub mod batch_size;
pub mod constraints;
pub mod create_cols;
pub mod create_tables;
pub mod infer_schema;
pub mod phase;

#[async_trait]
pub trait MigrationSetting {
    fn phase(&self) -> MigrationSettingsPhase;
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
            ("create_missing_tables", SettingValue::Boolean(true)) => migration_settings
                .push(Box::new(CreateMissingTablesSetting::new(context).await)
                    as Box<dyn MigrationSetting>),
            ("ignore_constraints", SettingValue::Boolean(true)) => migration_settings
                .push(Box::new(IgnoreConstraintsSettings(true)) as Box<dyn MigrationSetting>),
            _ => (), // Ignore unknown settings
        }
    }

    migration_settings.sort_by_key(|s| s.phase());
    migration_settings
}
