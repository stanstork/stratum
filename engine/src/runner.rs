use crate::{
    adapter::{get_adapter, Adapter},
    consumer::Consumer,
    context::MigrationContext,
    destination::data_dest::{create_data_destination, DataDestination},
    producer::Producer,
    settings::parse_settings,
    source::data_source::{create_data_source, DataSource},
    validate::schema_validator::{SchemaValidationMode, SchemaValidator},
};
use smql::{plan::MigrationPlan, statements::connection::DataFormat};
use sql_adapter::metadata::{provider::MetadataProvider, table::TableMetadata};
use std::sync::Arc;
use tokio::sync::{watch, Mutex};
use tracing::{error, info};

pub async fn run(plan: MigrationPlan) -> Result<(), Box<dyn std::error::Error>> {
    info!("Running migration");

    let source = create_source(&plan).await?;
    let destination = create_destination(&plan).await?;
    let context = MigrationContext::init(source, destination, &plan);

    apply_settings(&plan, Arc::clone(&context)).await?;
    // validate_destination(&plan, Arc::clone(&context)).await?;

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
) -> Result<Vec<TableMetadata>, Box<dyn std::error::Error>> {
    let source_adapter = get_adapter(
        plan.connections.source.data_format,
        &plan.connections.source.con_str,
    )
    .await?;
    let mut metadata = Vec::new();

    match source_adapter {
        Adapter::MySql(my_sql_adapter) => {
            for migration in plan.migration.migrations.iter() {
                let metadata_entry = MetadataProvider::build_metadata(
                    &my_sql_adapter,
                    migration.sources.first().unwrap(),
                )
                .await?;
                metadata.push(metadata_entry);
            }

            Ok(metadata)
        }
        Adapter::Postgres(_pg_adapter) => unimplemented!("Postgres metadata loading"),
    }
}

async fn create_source(plan: &MigrationPlan) -> Result<DataSource, Box<dyn std::error::Error>> {
    let adapter = get_adapter(
        plan.connections.source.data_format,
        &plan.connections.source.con_str,
    )
    .await?;

    match plan.connections.source.data_format {
        DataFormat::MySql => Ok(DataSource::Database(
            create_data_source(plan, adapter).await?,
        )),
        _ => unimplemented!("Unsupported data source"),
    }
}

async fn create_destination(
    plan: &MigrationPlan,
) -> Result<DataDestination, Box<dyn std::error::Error>> {
    let adapter = get_adapter(
        plan.connections.destination.data_format,
        &plan.connections.destination.con_str,
    )
    .await?;

    match plan.connections.destination.data_format {
        DataFormat::Postgres => Ok(DataDestination::Database(
            create_data_destination(plan, adapter).await?,
        )),
        _ => unimplemented!("Unsupported data destination"),
    }
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

// async fn validate_destination(
//     plan: &MigrationPlan,
//     context: Arc<Mutex<MigrationContext>>,
// ) -> Result<(), Box<dyn std::error::Error>> {
//     let context = context.lock().await;
//     let source_metadata = context.get_source_metadata().await?;
//     let destination_metadata = context.get_dest_metadata().await?;
//     let tbls_name_map = context.entity_name_map.clone();

//     let validator = SchemaValidator::new(&source_metadata, &destination_metadata);

//     // if context.state.lock().await.infer_schema {
//     //     let col_mapping = FieldMapping::extract_field_map(&plan.mapping);
//     //     let table_mapping = tbls_name_map.clone();
//     //     if let Err(err) =
//     //         validator.validate(SchemaValidationMode::OneToOne, table_mapping, col_mapping)
//     //     {
//     //         error!("Schema validation failed: {:?}", err);
//     //         return Err(err);
//     //     } else {
//     //         info!("Schema validation passed");
//     //     }
//     // }

//     Ok(())
// }
