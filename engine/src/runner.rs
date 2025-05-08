use std::{sync::Arc, vec};

use crate::{
    context::{global::GlobalContext, item::ItemContext},
    destination::{data_dest::DataDestination, destination::Destination},
    filter::{compiler::FilterCompiler, filter::Filter, sql::SqlFilterCompiler},
    settings::collect_settings,
    source::{data_source::DataSource, linked_source::LinkedSource, source::Source},
    state::MigrationState,
};
use common::mapping::EntityMapping;
use smql_v02::{
    plan::MigrationPlan,
    statements::{connection::DataFormat, migrate::MigrateItem, setting::Settings},
};
use tokio::sync::Mutex;
use tracing::{error, info};

pub async fn run(plan: MigrationPlan) -> Result<(), Box<dyn std::error::Error>> {
    info!("Running migration");

    // let mapping = EntityMapping::new(&plan);
    // let source = create_source(&plan, &mapping).await?;
    // let destination = create_destination(&plan).await?;
    // let context = MigrationContext::init(source, destination, mapping);

    // apply_settings(&plan, Arc::clone(&context)).await?;
    // // validate_destination(&plan, Arc::clone(&context)).await?;
    // set_metadata(&context, &plan).await?;

    // let (shutdown_sender, shutdown_receiver) = watch::channel(false);

    // let producer = Producer::new(Arc::clone(&context), shutdown_sender)
    //     .await
    //     .spawn();

    // let consumer = Consumer::new(Arc::clone(&context), shutdown_receiver)
    //     .await
    //     .spawn();

    // // Wait for both producer and consumer to finish
    // tokio::try_join!(producer, consumer)?;

    Ok(())
}

pub async fn run_v2(plan: MigrationPlan) -> Result<(), Box<dyn std::error::Error>> {
    info!("Running migration v2");

    let mut migration_tasks = vec![];

    let global_context = GlobalContext::new(&plan).await?;
    for mi in plan.migration.migrate_items {
        let mapping = EntityMapping::new(&mi);
        let source = create_source(&global_context, &mapping, &mi).await?;
        let destination = create_destination(&global_context, &mi).await?;
        let mi_task = tokio::spawn(async move {
            let state = MigrationState::from_settings(&mi.settings);
            let item_context = ItemContext::new(source, destination, mapping, state);

            let apply_settings = apply_settings(&item_context, &mi.settings).await;
            if let Err(e) = apply_settings {
                error!("Failed to apply settings: {:?}", e);
                return;
            }

            // let producer = Producer::new(item_context.clone()).await.spawn();
            // let consumer = Consumer::new(item_context.clone()).await.spawn();
            // tokio::try_join!(producer, consumer).unwrap();
        });
        migration_tasks.push(mi_task);
    }

    // Wait for all migration tasks to finish
    for task in migration_tasks {
        if let Err(e) = task.await {
            error!("Migration task failed: {:?}", e);
        }
    }

    Ok(())

    // todo!("Implement v2 migration");
}

// pub async fn load_src_metadata(
//     plan: &MigrationPlan,
// ) -> Result<HashMap<String, TableMetadata>, Box<dyn std::error::Error>> {
//     let source_adapter = Adapter::new(
//         plan.connections.source.data_format,
//         &plan.connections.source.con_str,
//     )
//     .await?;

//     match source_adapter {
//         Adapter::MySql(my_sql_adapter) => {
//             let tables = plan.migration.sources();
//             let metadata = MetadataProvider::build_metadata_graph(&my_sql_adapter, &tables).await?;
//             Ok(metadata)
//         }
//         Adapter::Postgres(_pg_adapter) => unimplemented!("Postgres metadata loading"),
//     }
// }

async fn create_source(
    ctx: &GlobalContext,
    mapping: &EntityMapping,
    migrate_item: &MigrateItem,
) -> Result<Source, Box<dyn std::error::Error>> {
    let linked = if let Some(load) = migrate_item.load.as_ref() {
        Some(LinkedSource::new(ctx, load, mapping).await?)
    } else {
        None
    };

    let filter = create_filter(migrate_item, ctx.src_format)?;
    let primary = DataSource::from_adapter(ctx.src_format, &ctx.src_adapter, &linked, &filter)?;

    Ok(Source::new(
        migrate_item.source.name(),
        ctx.src_format,
        primary,
        linked,
        filter,
    ))
}

fn create_filter(
    migrate_item: &MigrateItem,
    format: DataFormat,
) -> Result<Option<Filter>, Box<dyn std::error::Error>> {
    match format {
        // If the format is SQL, try to build a SQL filter.
        DataFormat::MySql | DataFormat::Postgres => {
            // Create a new SQL filter
            let filter = migrate_item
                .filter
                .as_ref()
                .map(|ast| Filter::Sql(SqlFilterCompiler::compile(&ast.expression)));
            Ok(filter)
        }
        _ => {
            // Unsupported format
            Ok(None)
        }
    }
}

async fn create_destination(
    ctx: &GlobalContext,
    migrate_item: &MigrateItem,
) -> Result<Destination, Box<dyn std::error::Error>> {
    let data_dest = DataDestination::from_adapter(ctx.dest_format, &ctx.dest_adapter)?;
    Ok(Destination::new(
        migrate_item.destination.name(),
        ctx.dest_format,
        data_dest,
    ))
}

async fn apply_settings(
    ctx: &Arc<Mutex<ItemContext>>,
    settings: &Settings,
) -> Result<(), Box<dyn std::error::Error>> {
    info!("Applying migration settings");

    let settings = collect_settings(&settings, &ctx).await;
    for setting in settings.iter() {
        setting.apply(ctx.clone()).await?;
    }

    ctx.lock().await.debug_state().await;

    Ok(())
}

// async fn set_metadata(
//     context: &Arc<Mutex<MigrationContext>>,
//     plan: &MigrationPlan,
// ) -> Result<(), Box<dyn std::error::Error>> {
//     let source_tables = plan.migration.sources();
//     let destination_tables = plan.migration.targets();

//     set_source_metadata(context, &source_tables).await?;
//     set_destination_metadata(context, &destination_tables).await?;

//     Ok(())
// }

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
