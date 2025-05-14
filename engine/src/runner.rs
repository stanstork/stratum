use crate::{
    consumer::Consumer,
    context::{global::GlobalContext, item::ItemContext},
    destination::{data::DataDestination, Destination},
    error::MigrationError,
    filter::{compiler::FilterCompiler, sql::SqlFilterCompiler, Filter},
    producer::Producer,
    settings::collect_settings,
    source::{data::DataSource, linked::LinkedSource, Source},
    state::MigrationState,
};
use common::mapping::EntityMapping;
use futures::{stream::FuturesUnordered, StreamExt};
use smql::{
    plan::MigrationPlan,
    statements::{connection::DataFormat, migrate::MigrateItem, setting::Settings},
};
use std::sync::Arc;
use tokio::{
    sync::{watch, Mutex},
    task::JoinHandle,
};
use tracing::{error, info};

pub async fn run(plan: MigrationPlan) -> Result<(), MigrationError> {
    info!("Running migration v2");

    // Build the shared context
    let global_ctx = GlobalContext::new(&plan).await?;

    // Spawn one task per MigrateItem, collecting JoinHandles
    let mut handles: FuturesUnordered<JoinHandle<Result<(), MigrationError>>> =
        FuturesUnordered::new();

    for mi in plan.migration.migrate_items {
        let gc = global_ctx.clone();
        handles.push(tokio::spawn(async move {
            // Prepare per-item state
            let mapping = EntityMapping::new(&mi);
            let source = create_source(&gc, &mapping, &mi).await?;
            let destination = create_destination(&gc, &mi).await?;
            let mut item_ctx = ItemContext::new(
                source,
                destination,
                mapping.clone(),
                MigrationState::from_settings(&mi.settings),
            );

            // Apply all settings
            apply_settings(&mut item_ctx, &mi.settings).await?;
            set_meta(&mut item_ctx).await?;

            // Wire up producer and consumer
            let (shutdown_tx, shutdown_rx) = watch::channel(false);
            let ctx = Arc::new(Mutex::new(item_ctx));

            let prod = Producer::new(ctx.clone(), shutdown_tx).await.spawn();
            let cons = Consumer::new(ctx.clone(), shutdown_rx).await.spawn();

            // Run both sides in parallel, propagate any error
            tokio::try_join!(prod, cons)?;
            Ok(())
        }));
    }

    // Await all item‐tasks, logging any failures
    while let Some(join_res) = handles.next().await {
        match join_res {
            Ok(Ok(())) => (), // task completed successfully
            Ok(Err(e)) => error!("Migration item error: {}", e),
            Err(join_err) => error!("Task panicked: {}", join_err),
        }
    }

    info!("Migration completed");

    Ok(())
}

async fn create_source(
    ctx: &GlobalContext,
    mapping: &EntityMapping,
    migrate_item: &MigrateItem,
) -> Result<Source, MigrationError> {
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
) -> Result<Option<Filter>, MigrationError> {
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
) -> Result<Destination, MigrationError> {
    let data_dest = DataDestination::from_adapter(ctx.dest_format, &ctx.dest_adapter)?;
    Ok(Destination::new(
        migrate_item.destination.name(),
        ctx.dest_format,
        data_dest,
    ))
}

async fn apply_settings(ctx: &mut ItemContext, settings: &Settings) -> Result<(), MigrationError> {
    info!("Applying migration settings");

    let settings = collect_settings(settings, ctx);
    for setting in settings.iter() {
        setting.apply(ctx).await?;
    }

    ctx.debug_state().await;

    Ok(())
}

async fn set_meta(ctx: &mut ItemContext) -> Result<(), MigrationError> {
    ctx.set_src_meta().await?;
    ctx.set_dest_meta().await?;

    Ok(())
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
