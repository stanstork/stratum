use crate::{
    buffer::RecordBuffer,
    context::MigrationContext,
    destination::data_dest::create_data_destination,
    settings::{BatchSizeSetting, InferSchemaSetting, MigrationSetting},
    source::data_source::create_data_source,
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
    let destination_adapter = get_db_adapter(
        DbEngine::from_data_format(plan.connections.destination.data_format),
        &plan.connections.destination.con_str,
    )
    .await?;

    let table = plan.migration.source.first().unwrap().clone();

    let data_source = Arc::new(
        create_data_source(table, plan.connections.source.data_format, source_adapter).await?,
    );
    let data_destination = Arc::new(
        create_data_destination(
            plan.connections.destination.data_format,
            destination_adapter,
        )
        .await?,
    );

    let buffer = Arc::new(RecordBuffer::new("migration_buffer"));

    let context = Arc::new(Mutex::new(MigrationContext {
        state: Arc::clone(&state),
        source: Arc::clone(&data_source),
        destination: Arc::clone(&data_destination),
        buffer: Arc::clone(&buffer),
        source_data_format: plan.connections.source.data_format,
        destination_data_format: plan.connections.destination.data_format,
    }));

    let settings = parse_settings(&plan.migration.settings);
    for setting in settings.iter() {
        setting.apply(Arc::clone(&context)).await?;
    }

    let buffer_clone = Arc::clone(&buffer);
    let data_source_clone = Arc::clone(&data_source);

    // Spawn producer task
    let producer_task = tokio::spawn(async move {
        let mut offset = 0;
        loop {
            let records = data_source_clone
                .fetch_data(100, Some(offset))
                .await
                .unwrap();

            println!("Fetched {} records", records.len());

            if records.is_empty() {
                break;
            }

            for record in records {
                if let Err(e) = buffer_clone.store(record.serialize()) {
                    eprintln!("Failed to store record: {}", e);
                    return;
                }
            }

            offset += state.lock().await.batch_size;
        }
    });

    producer_task.await?;

    // Consumer code will go here

    Ok(())
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
