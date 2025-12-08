use crate::error::MigrationError;
use engine_core::{
    connectors::{
        destination::{DataDestination, Destination},
        filter::Filter,
        linked::LinkedSource,
        source::{DataFormat, DataSource, Source},
    },
    context::exec::ExecutionContext,
};
use engine_processing::filter::{
    compiler::FilterCompiler, csv::CsvFilterCompiler, sql::SqlFilterCompiler,
};
use model::{
    execution::{connection::Connection, pipeline::Pipeline},
    transform::mapping::TransformationMetadata,
};
use planner::query::offsets::OffsetStrategy;
use std::sync::Arc;

pub async fn create_source(
    ctx: &ExecutionContext,
    pipeline: &Pipeline,
    mapping: &TransformationMetadata,
    offset_strategy: Arc<dyn OffsetStrategy>,
) -> Result<Source, MigrationError> {
    let name = pipeline.source.table.clone();
    let format = get_data_format(&pipeline.source.connection)?;
    let adapter = ctx.get_adapter(&pipeline.source.connection).await?;
    let linked = LinkedSource::new(&adapter, &format, &pipeline.source.joins, mapping).await?;
    let filter = create_filter(pipeline, &format)?;
    let primary = DataSource::from_adapter(format, &adapter, &linked, &filter, offset_strategy)?;

    Ok(Source::new(name, format, primary, linked, filter))
}

pub async fn create_destination(
    ctx: &ExecutionContext,
    pipeline: &Pipeline,
) -> Result<Destination, MigrationError> {
    let name = pipeline.destination.table.clone();
    let format = get_data_format(&pipeline.destination.connection)?;
    let adapter = ctx.get_adapter(&pipeline.destination.connection).await?;
    let data_dest = DataDestination::from_adapter(format, &adapter)?;

    Ok(Destination::new(name, format, data_dest))
}

fn create_filter(
    pipeline: &Pipeline,
    format: &DataFormat,
) -> Result<Option<Filter>, MigrationError> {
    let filter = &pipeline.source.filters[0];
    match format {
        DataFormat::MySql | DataFormat::Postgres => Ok(Some(Filter::Sql(
            SqlFilterCompiler::compile(&filter.condition),
        ))),
        DataFormat::Csv => Ok(Some(Filter::Csv(CsvFilterCompiler::compile(
            &filter.condition,
        )))),
    }
}

fn get_data_format(conn: &Connection) -> Result<DataFormat, MigrationError> {
    match conn.driver.as_str() {
        "mysql" => Ok(DataFormat::MySql),
        "postgres" => Ok(DataFormat::Postgres),
        "csv" => Ok(DataFormat::Csv),
        _ => Err(MigrationError::UnsupportedFormat(conn.driver.clone())),
    }
}
