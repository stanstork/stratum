use crate::{
    adapter::get_adapter,
    consumer::spawn_consumer,
    context::MigrationContext,
    destination::data_dest::{create_data_destination, DataDestination},
    producer::spawn_producer,
    settings::{BatchSizeSetting, InferSchemaSetting, MigrationSetting},
    source::data_source::{create_data_source, DataSource},
    validate::schema_validator::{SchemaValidationMode, SchemaValidator},
};
use smql::{
    plan::MigrationPlan,
    statements::{
        connection::DataFormat,
        setting::{Setting, SettingValue},
    },
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info};

pub async fn run(plan: MigrationPlan) -> Result<(), Box<dyn std::error::Error>> {
    info!("Running migration");

    let (data_source, data_destination) = setup_connections(&plan).await?;
    let context = MigrationContext::init(data_source, data_destination, &plan);

    apply_settings(&plan, Arc::clone(&context)).await?;
    validate_destination(&plan, Arc::clone(&context)).await?;

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
        DataFormat::MySql => DataSource::Database(create_data_source(plan, source_adapter).await?),
        _ => unimplemented!("Unsupported data source"),
    };
    let data_destination = match plan.connections.destination.data_format {
        DataFormat::Postgres => {
            DataDestination::Database(create_data_destination(plan, destination_adapter).await?)
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
        setting.apply(plan, Arc::clone(&context)).await?;
    }

    context.lock().await.debug_state().await;

    Ok(())
}

async fn validate_destination(
    plan: &MigrationPlan,
    context: Arc<Mutex<MigrationContext>>,
) -> Result<(), Box<dyn std::error::Error>> {
    let context = context.lock().await;
    let source_metadata = context.get_source_metadata().await?;
    let destination_metadata = context
        .get_destination_metadata(&plan.migration.target)
        .await?;

    let validator = SchemaValidator::new(&source_metadata, &destination_metadata);

    if context.state.lock().await.infer_schema {
        if let Err(err) = validator.validate(SchemaValidationMode::OneToOne) {
            error!("Schema validation failed: {:?}", err);
            return Err(err);
        } else {
            info!("Schema validation passed");
        }
    }

    Ok(())
}

fn parse_settings(settings: &[Setting]) -> Vec<Box<dyn MigrationSetting>> {
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
