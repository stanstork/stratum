use crate::{
    adapter::get_adapter,
    consumer::spawn_consumer,
    context::MigrationContext,
    destination::data_dest::{create_data_destination, DataDestination},
    producer::spawn_producer,
    record::register_data_record,
    settings::{BatchSizeSetting, InferSchemaSetting, MigrationSetting},
    source::data_source::{create_data_source, DataSource},
};
use smql::{
    plan::MigrationPlan,
    statements::{
        connection::DataFormat,
        setting::{Setting, SettingValue},
    },
};
use sql_adapter::{metadata::utils::build_table_metadata, row::row::RowData};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

pub async fn run(plan: MigrationPlan) -> Result<(), Box<dyn std::error::Error>> {
    info!("Running migration");

    // Register RowData type for deserialization
    register_data_record::<RowData>("RowData").await;

    let (data_source, data_destination) = setup_connections(&plan).await?;
    let context = MigrationContext::init(data_source, data_destination, &plan);

    apply_settings(&plan, Arc::clone(&context)).await?;
    validate_data_destination(&plan, &context.lock().await.destination).await?;

    let producer = spawn_producer(Arc::clone(&context)).await;
    let consumer = spawn_consumer(Arc::clone(&context)).await;

    // Wait for both producer and consumer to finish
    tokio::try_join!(producer, consumer)?;

    Ok(())
}

async fn setup_connections(
    plan: &MigrationPlan,
) -> Result<(DataSource, DataDestination), Box<dyn std::error::Error>> {
    info!("Setting up connections");

    let source_adapter = get_adapter(
        plan.connections.source.data_format,
        &plan.connections.source.con_str,
    )
    .await?;
    let destination_adapter = get_adapter(
        plan.connections.destination.data_format,
        &plan.connections.destination.con_str,
    )
    .await?;

    let data_source = match plan.connections.source.data_format {
        DataFormat::MySql => DataSource::Database(create_data_source(&plan, source_adapter).await?),
        _ => unimplemented!("Unsupported data source"),
    };
    let data_destination = match plan.connections.destination.data_format {
        DataFormat::Postgres => {
            DataDestination::Database(create_data_destination(&plan, destination_adapter).await?)
        }
        _ => unimplemented!("Unsupported data destination"),
    };

    Ok((data_source, data_destination))
}

async fn apply_settings(
    plan: &MigrationPlan,
    context: Arc<Mutex<MigrationContext>>,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Applying migration settings");

    let settings = parse_settings(&plan.migration.settings);
    for setting in settings.iter() {
        setting.apply(Arc::clone(&context)).await?;
    }

    context.lock().await.debug_state().await;

    Ok(())
}

async fn validate_data_destination(
    plan: &MigrationPlan,
    destination: &DataDestination,
) -> Result<(), Box<dyn std::error::Error>> {
    match destination {
        DataDestination::Database(destination) => {
            let dest_table = plan.migration.target.clone();
            let metadata = build_table_metadata(&destination.adapter(), &dest_table).await?;
            println!("Dest metadata: {:#?}", metadata);
            // destination.validate_schema(&metadata).await?
        }
    }

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
