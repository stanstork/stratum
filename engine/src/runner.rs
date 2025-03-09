use crate::{
    settings::{BatchSizeSetting, InferSchemaSetting, MigrationSetting},
    state::MigrationState,
};
use smql::{
    plan::MigrationPlan,
    statements::setting::{Setting, SettingValue},
};
use sql_adapter::{get_db_adapter, DbEngine};
use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn run(plan: MigrationPlan) -> Result<(), Box<dyn std::error::Error>> {
    let state = Arc::new(Mutex::new(MigrationState::new()));

    let source_adapter = get_db_adapter(
        DbEngine::from_data_format(plan.connections.source.data_format),
        &plan.connections.source.con_str,
    )
    .await?;
    let settings = parse_settings(&plan.migration.settings);

    for setting in settings.iter() {
        setting.apply(state.clone()).await;
    }

    todo!()
}

fn parse_settings(settings: &Vec<Setting>) -> Vec<Box<dyn MigrationSetting>> {
    settings
        .into_iter()
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
