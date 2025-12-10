use crate::error::MigrationError;
use engine_core::{
    connectors::{
        destination::{DataDestination, Destination},
        filter::Filter,
        format::DataFormat,
        linked::LinkedSource,
        source::{DataSource, Source},
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
    if pipeline.source.filters.is_empty() {
        return Ok(None);
    }

    // Combine all filter conditions with AND logic.
    // Multiple where blocks or conditions in SMQL are semantically joined with AND,
    // meaning ALL conditions must be satisfied (standard SQL WHERE clause behavior).
    //
    // Example: where { age > 18 } where { status == "active" }
    // Results in: (age > 18) AND (status == "active")
    let combined_condition = if pipeline.source.filters.len() == 1 {
        pipeline.source.filters[0].condition.clone()
    } else {
        use model::execution::expr::{BinaryOp, CompiledExpression};

        // Start with the first filter condition
        let mut combined = pipeline.source.filters[0].condition.clone();

        // AND all subsequent filter conditions together
        for filter in &pipeline.source.filters[1..] {
            combined = CompiledExpression::Binary {
                left: Box::new(combined),
                op: BinaryOp::And,
                right: Box::new(filter.condition.clone()),
            };
        }

        combined
    };

    match format {
        DataFormat::MySql | DataFormat::Postgres => Ok(Some(Filter::Sql(
            SqlFilterCompiler::compile(&combined_condition),
        ))),
        DataFormat::Csv => Ok(Some(Filter::Csv(CsvFilterCompiler::compile(
            &combined_condition,
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
