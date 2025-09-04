use crate::{
    adapter::Adapter,
    consumer::Consumer,
    context::{global::GlobalContext, item::ItemContext},
    destination::{data::DataDestination, Destination},
    error::MigrationError,
    filter::{compiler::FilterCompiler, csv::CsvFilterCompiler, sql::SqlFilterCompiler, Filter},
    metadata::entity::EntityMetadata,
    producer::Producer,
    settings::collect_settings,
    source::{data::DataSource, linked::LinkedSource, Source},
    state::MigrationState,
};
use common::mapping::EntityMapping;
use smql::{
    plan::MigrationPlan,
    statements::{
        connection::{Connection, ConnectionPair, DataFormat},
        migrate::{MigrateItem, SpecKind},
        setting::Settings,
    },
};
use sql_adapter::metadata::provider::MetadataProvider;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{watch, Mutex};
use tracing::{error, info};

pub async fn run(plan: MigrationPlan, dry_run: bool) -> Result<(), MigrationError> {
    info!("Running migration v2");

    // Build the shared context
    let global_ctx = GlobalContext::new(&plan).await?;

    // Run each migration item sequentially
    for mi in plan.migration.migrate_items {
        let gc = global_ctx.clone();
        let conn = plan.connections.clone();

        // Prepare per-item state
        let mapping = EntityMapping::new(&mi);
        let source = create_source(&gc, &conn, &mapping, &mi).await?;
        let destination = create_destination(&gc, &conn, &mi).await?;
        let state = MigrationState::new(&mi.settings, dry_run);
        let mut item_ctx = ItemContext::new(source, destination, mapping.clone(), state);

        // Apply all settings
        apply_settings(&mut item_ctx, &mi.settings).await?;
        set_meta(&mut item_ctx).await?;

        // Wire up producer and consumer
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let ctx = Arc::new(Mutex::new(item_ctx));

        let prod = Producer::new(ctx.clone(), shutdown_tx).await.spawn();
        let cons = Consumer::new(ctx.clone(), shutdown_rx).await.spawn();

        // await both before moving on
        match tokio::try_join!(prod, cons) {
            Ok(((), ())) => info!("Item {} migrated successfully", mi.destination.name()),
            Err(e) => {
                error!("Migration item error ({}): {}", mi.destination.name(), e);
                // decide: return Err(e)?  or continue to next?
            }
        }

        info!("Migration item {} completed", mi.destination.name());
    }

    info!("Migration completed");
    Ok(())
}

pub async fn load_src_metadata(
    conn_str: &str,
    format: DataFormat,
) -> Result<HashMap<String, EntityMetadata>, MigrationError> {
    info!("Loading source metadata");

    let adapter = Adapter::sql(format, conn_str).await?;
    let names = adapter.get_sql().list_tables().await?;

    info!("Found {} source tables: {:?}", names.len(), names);

    let meta_graph = MetadataProvider::build_metadata_graph(adapter.get_sql(), &names).await?;

    info!(
        "Source metadata graph built with {} tables",
        meta_graph.len()
    );

    return Ok(meta_graph
        .iter()
        .map(|(name, meta)| (name.clone(), EntityMetadata::Table(meta.clone())))
        .collect());
}

async fn create_source(
    ctx: &GlobalContext,
    conn: &Connection,
    mapping: &EntityMapping,
    migrate_item: &MigrateItem,
) -> Result<Source, MigrationError> {
    let name = migrate_item.source.name();
    let format = get_data_format(migrate_item, conn).0;

    // build the optional LinkedSource
    let linked = if let Some(load) = migrate_item.load.as_ref() {
        Some(LinkedSource::new(ctx, format, load, mapping).await?)
    } else {
        None
    };

    let adapter = get_adapter(ctx, &format, &name).await?;
    let filter = create_filter(migrate_item, format)?;
    let primary = DataSource::from_adapter(format, &adapter, &linked, &filter)?;

    Ok(Source::new(name, format, primary, linked, filter))
}

async fn get_adapter(
    ctx: &GlobalContext,
    format: &DataFormat,
    name: &str,
) -> Result<Option<Adapter>, MigrationError> {
    match format {
        f if f.is_sql() => {
            // for SQL just clone the existing connection handle
            Ok(ctx.src_conn.clone())
        }
        f if f.is_file() => {
            // for file-based sources instantiate a new adapter
            let file_adapter = ctx.get_file_adapter(name).await?;
            Ok(Some(file_adapter))
        }
        _ => Err(MigrationError::UnsupportedFormat(format.to_string())),
    }
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
        DataFormat::Csv => {
            let filter = migrate_item
                .filter
                .as_ref()
                .map(|ast| Filter::Csv(CsvFilterCompiler::compile(&ast.expression)));
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
    conn: &Connection,
    migrate_item: &MigrateItem,
) -> Result<Destination, MigrationError> {
    let name = migrate_item.destination.name();
    let format = get_data_format(migrate_item, conn).1;
    let data_dest = DataDestination::from_adapter(format, &ctx.dst_conn)?;
    Ok(Destination::new(name, format, data_dest))
}

async fn apply_settings(ctx: &mut ItemContext, settings: &Settings) -> Result<(), MigrationError> {
    info!("Applying migration settings");

    let mut settings = collect_settings(settings, ctx).await;
    for setting in settings.iter_mut() {
        if setting.can_apply(ctx) {
            setting.apply(ctx).await?;
        }
    }

    let state = ctx.state.lock().await;
    println!("Report: {:#?}", state.validation_report);

    todo!("Implement validation report handling");

    ctx.debug_state().await;

    Ok(())
}

async fn set_meta(ctx: &mut ItemContext) -> Result<(), MigrationError> {
    ctx.set_src_meta().await?;
    ctx.set_dest_meta().await?;

    Ok(())
}

fn get_data_format(item: &MigrateItem, conn: &Connection) -> (DataFormat, DataFormat) {
    // helper for one side (source or destination)
    fn format_for(kind: &SpecKind, conn: &Option<ConnectionPair>, label: &str) -> DataFormat {
        match kind {
            SpecKind::Table => {
                conn.as_ref()
                    .unwrap_or_else(|| panic!("Connection {} is required", label))
                    .format
            }
            SpecKind::Api => DataFormat::Api,
            SpecKind::Csv => DataFormat::Csv,
        }
    }

    let source_format = format_for(&item.source.kind, &conn.source, "source");
    let dest_format = format_for(&item.destination.kind, &conn.dest, "destination");
    (source_format, dest_format)
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
