use crate::{
    adapter::{get_adapter, Adapter},
    consumer::Consumer,
    context::MigrationContext,
    destination::data_dest::{create_data_destination, DataDestination},
    mapping::field::FieldMapping,
    producer::Producer,
    settings::parse_settings,
    source::data_source::{create_data_source, DataSource},
    validate::schema_validator::{SchemaValidationMode, SchemaValidator},
};
use smql::{plan::MigrationPlan, statements::connection::DataFormat};
use sql_adapter::{
    metadata::{provider::MetadataProvider, table::TableMetadata},
    schema::mapping::NameMap,
};
use std::sync::Arc;
use tokio::sync::{watch, Mutex};
use tracing::{error, info};

pub async fn run(plan: MigrationPlan) -> Result<(), Box<dyn std::error::Error>> {
    info!("Running migration");

    let (data_source, data_destination) = setup_connections(&plan).await?;
    let context = MigrationContext::init(data_source, data_destination, &plan);

    apply_settings(&plan, Arc::clone(&context)).await?;
    validate_destination(&plan, Arc::clone(&context)).await?;

    let (shutdown_sender, shutdown_receiver) = watch::channel(false);

    let producer = Producer::new(Arc::clone(&context), shutdown_sender)
        .await
        .spawn();
    let consumer = Consumer::new(Arc::clone(&context), shutdown_receiver)
        .await
        .spawn();

    // Wait for both producer and consumer to finish
    tokio::try_join!(producer, consumer)?;

    Ok(())
}

pub async fn load_src_metadata(
    plan: &MigrationPlan,
) -> Result<TableMetadata, Box<dyn std::error::Error>> {
    let source_adapter = get_adapter(
        plan.connections.source.data_format,
        &plan.connections.source.con_str,
    )
    .await?;

    match source_adapter {
        Adapter::MySql(my_sql_adapter) => {
            let source_table = plan.migration.source.first().unwrap();
            let metadata =
                MetadataProvider::build_metadata_with_deps(&my_sql_adapter, source_table).await?;
            Ok(metadata)
        }
        Adapter::Postgres(_pg_adapter) => unimplemented!("Postgres metadata loading"),
    }
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

    let settings = parse_settings(&plan.migration.settings, &context).await;
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
    let destination_metadata = context.get_dest_metadata().await?;
    let tbls_name_map = context.name_mapping.clone();

    let validator = SchemaValidator::new(&source_metadata, &destination_metadata);

    if context.state.lock().await.infer_schema {
        let col_mapping = NameMap::new(FieldMapping::extract_field_map(&plan.mapping));
        let table_mapping = NameMap::new(tbls_name_map.clone());
        if let Err(err) =
            validator.validate(SchemaValidationMode::OneToOne, table_mapping, col_mapping)
        {
            error!("Schema validation failed: {:?}", err);
            return Err(err);
        } else {
            info!("Schema validation passed");
        }
    }

    Ok(())
}
