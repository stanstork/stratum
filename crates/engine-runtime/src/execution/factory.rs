use crate::error::MigrationError;
use connectors::adapter::Adapter;
use engine_core::{
    connectors::{
        destination::{DataDestination, Destination},
        filter::Filter,
        linked::LinkedSource,
        source::{DataSource, Source},
    },
    context::global::GlobalContext,
};
use engine_processing::filter::{
    compiler::FilterCompiler, csv::CsvFilterCompiler, sql::SqlFilterCompiler,
};
use model::transform::mapping::EntityMapping;
use planner::query::offsets::OffsetStrategy;
use smql_syntax::ast_v2::{
    connection::{Connection, ConnectionPair, DataFormat},
    migrate::{MigrateItem, SpecKind},
};
use std::sync::Arc;

pub async fn create_source(
    ctx: &GlobalContext,
    conn: &Connection,
    mapping: &EntityMapping,
    migrate_item: &MigrateItem,
    offset_strategy: Arc<dyn OffsetStrategy>,
) -> Result<Source, MigrationError> {
    let name = migrate_item.source.name();
    let format = get_data_format(migrate_item, conn).0;

    let linked = if let Some(load) = migrate_item.load.as_ref() {
        Some(LinkedSource::new(ctx, format, load, mapping).await?)
    } else {
        None
    };

    let adapter = get_adapter(ctx, &format, &name).await?;
    let filter = create_filter(migrate_item, format)?;
    let primary = DataSource::from_adapter(format, &adapter, &linked, &filter, offset_strategy)?;

    Ok(Source::new(name, format, primary, linked, filter))
}

pub async fn create_destination(
    ctx: &GlobalContext,
    conn: &Connection,
    migrate_item: &MigrateItem,
) -> Result<Destination, MigrationError> {
    let name = migrate_item.destination.name();
    let format = get_data_format(migrate_item, conn).1;
    let data_dest = DataDestination::from_adapter(format, &ctx.dst_conn)?;
    Ok(Destination::new(name, format, data_dest))
}

fn create_filter(
    migrate_item: &MigrateItem,
    format: DataFormat,
) -> Result<Option<Filter>, MigrationError> {
    match format {
        DataFormat::MySql | DataFormat::Postgres => Ok(migrate_item
            .filter
            .as_ref()
            .map(|ast| Filter::Sql(SqlFilterCompiler::compile(&ast.expression)))),
        DataFormat::Csv => Ok(migrate_item
            .filter
            .as_ref()
            .map(|ast| Filter::Csv(CsvFilterCompiler::compile(&ast.expression)))),
        _ => Ok(None),
    }
}

async fn get_adapter(
    ctx: &GlobalContext,
    format: &DataFormat,
    name: &str,
) -> Result<Option<Adapter>, MigrationError> {
    match format {
        f if f.is_sql() => Ok(ctx.src_conn.clone()),
        f if f.is_file() => {
            let file_adapter = ctx.get_file_adapter(name).await?;
            Ok(Some(file_adapter))
        }
        _ => Err(MigrationError::UnsupportedFormat(format.to_string())),
    }
}

fn get_data_format(item: &MigrateItem, conn: &Connection) -> (DataFormat, DataFormat) {
    fn format_for(kind: &SpecKind, conn: &Option<ConnectionPair>, label: &str) -> DataFormat {
        match kind {
            SpecKind::Table => {
                conn.as_ref()
                    .unwrap_or_else(|| panic!("Connection {label} is required"))
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
