use crate::{
    adapter::Adapter,
    consumer::Consumer,
    context::MigrationContext,
    destination::{data_dest::DataDestination, destination::Destination},
    metadata::{set_destination_metadata, set_source_metadata},
    producer::Producer,
    settings::parse_settings,
    source::{data_source::DataSource, linked_source::LinkedSource, source::Source},
};
use common::mapping::EntityMappingContext;
use smql::plan::MigrationPlan;
use sql_adapter::metadata::{provider::MetadataProvider, table::TableMetadata};
use std::{collections::HashMap, sync::Arc, vec};
use tokio::sync::{watch, Mutex};
use tracing::info;

pub async fn run(plan: MigrationPlan) -> Result<(), Box<dyn std::error::Error>> {
    info!("Running migration");

    let mapping = EntityMappingContext::new(&plan);
    let source = create_source(&plan, &mapping).await?;
    let destination = create_destination(&plan).await?;
    let context = MigrationContext::init(source, destination, mapping);

    apply_settings(&plan, Arc::clone(&context)).await?;
    // validate_destination(&plan, Arc::clone(&context)).await?;
    set_metadata(&context, &plan).await?;

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
) -> Result<HashMap<String, TableMetadata>, Box<dyn std::error::Error>> {
    let source_adapter = Adapter::new(
        plan.connections.source.data_format,
        &plan.connections.source.con_str,
    )
    .await?;

    match source_adapter {
        Adapter::MySql(my_sql_adapter) => {
            let tables = plan.migration.sources();
            let metadata = MetadataProvider::build_metadata_graph(&my_sql_adapter, &tables).await?;
            Ok(metadata)
        }
        Adapter::Postgres(_pg_adapter) => unimplemented!("Postgres metadata loading"),
    }
}

async fn create_source(
    plan: &MigrationPlan,
    mapping: &EntityMappingContext,
) -> Result<Source, Box<dyn std::error::Error>> {
    let format = plan.connections.source.data_format;
    let adapter = Adapter::new(format, &plan.connections.source.con_str).await?;
    let data_source = DataSource::from_adapter(format, &adapter)?;

    let mut load_sources = vec![];
    for load in plan.loads.iter() {
        let load_source = LinkedSource::new(&adapter, &format, mapping, &load).await?;
        load_sources.push(load_source);
    }

    Ok(Source::new(format, data_source, load_sources))
}

async fn create_destination(
    plan: &MigrationPlan,
) -> Result<Destination, Box<dyn std::error::Error>> {
    let format = plan.connections.destination.data_format;
    let adapter = Adapter::new(format, &plan.connections.destination.con_str).await?;
    let data_dest =
        DataDestination::from_adapter(plan.connections.destination.data_format, adapter)?;

    Ok(Destination::new(format, data_dest))
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

async fn set_metadata(
    context: &Arc<Mutex<MigrationContext>>,
    plan: &MigrationPlan,
) -> Result<(), Box<dyn std::error::Error>> {
    let source_tables = plan.migration.sources();
    let destination_tables = plan.migration.targets();

    set_source_metadata(context, &source_tables).await?;
    set_destination_metadata(context, &destination_tables).await?;

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
