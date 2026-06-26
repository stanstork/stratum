use crate::{
    builder::errors::{ReportBuilderError, SourceAnalyzerError},
    plan::{
        connection::plan::DatabaseDriver,
        pipeline::{
            destination::DestinationPlan,
            source::{ColumnInfo, SourcePlan},
        },
    },
};
pub use destination::{DbPlanDestinationEndpoint, WasmPlanDestinationEndpoint};
use engine_core::{
    context::exec::ConnectionPool, drivers::DriverRef, schema::type_registry::Dialect,
};
use engine_processing::io::format::DataFormat;
use engine_wasm::{registry::PluginRegistry, schema::PluginField};
use model::{
    core::types::Type,
    execution::{connection::Connection, pipeline::Pipeline},
};
pub use source::{DbPlanSourceEndpoint, WasmPlanSourceEndpoint};
use std::{collections::HashMap, sync::Arc};

mod destination;
mod source;

/// A resolved source endpoint with all its plan-side data pre-computed.
pub trait PlanSourceEndpoint: Send + Sync {
    fn source_plan(&self) -> &SourcePlan;
    fn column_types(&self) -> &HashMap<String, Type>;
    fn db_driver(&self) -> Option<&DriverRef>;
    fn plugin_name(&self) -> Option<&str>;
}

/// A resolved destination endpoint, symmetric to `PlanSourceEndpoint`.
pub trait PlanDestinationEndpoint: Send + Sync {
    fn destination_plan(&self) -> &DestinationPlan;
    fn column_types(&self) -> &HashMap<String, Type>;
    fn db_driver(&self) -> Option<&DriverRef>;
    fn plugin_name(&self) -> Option<&str>;
}

fn wasm_plugin_name(conn: &Connection) -> Result<String, ReportBuilderError> {
    conn.properties.get_string("plugin").ok_or_else(|| {
        ReportBuilderError::SourceAnalyzer(SourceAnalyzerError::QueryFailed(format!(
            "wasm connection '{}' is missing required property `plugin`",
            conn.name
        )))
    })
}

pub async fn resolve_source(
    pipeline: &Pipeline,
    connections: &mut ConnectionPool,
    registry: &Arc<PluginRegistry>,
) -> Result<Box<dyn PlanSourceEndpoint>, ReportBuilderError> {
    let conn = &pipeline.source.connection;
    match DataFormat::parse(&conn.driver) {
        Some(DataFormat::Wasm) => {
            let plugin = wasm_plugin_name(conn)?;
            Ok(Box::new(WasmPlanSourceEndpoint::new(
                pipeline,
                registry.clone(),
                plugin,
            )?))
        }
        _ => {
            let driver = DriverRef::resolve(&conn.driver, conn, connections).await?;
            Ok(Box::new(DbPlanSourceEndpoint::new(pipeline, driver).await?))
        }
    }
}

pub async fn resolve_destination(
    pipeline: &Pipeline,
    connections: &mut ConnectionPool,
    registry: &Arc<PluginRegistry>,
) -> Result<Box<dyn PlanDestinationEndpoint>, ReportBuilderError> {
    let conn = &pipeline.destination.connection;
    match DataFormat::parse(&conn.driver) {
        Some(DataFormat::Wasm) => {
            let plugin = wasm_plugin_name(conn)?;
            Ok(Box::new(WasmPlanDestinationEndpoint::new(
                pipeline,
                registry.clone(),
                plugin,
            )?))
        }
        _ => {
            let driver = DriverRef::resolve(&conn.driver, conn, connections).await?;
            Ok(Box::new(
                DbPlanDestinationEndpoint::new(pipeline, driver).await?,
            ))
        }
    }
}

/// True when either side is a WASM endpoint - i.e., the wasm-aware path must
/// be used instead of the DB<->DB analyzer chain.
pub fn is_wasm_pipeline(pipeline: &Pipeline) -> bool {
    DataFormat::parse(&pipeline.source.connection.driver) == Some(DataFormat::Wasm)
        || DataFormat::parse(&pipeline.destination.connection.driver) == Some(DataFormat::Wasm)
}

pub(super) fn dialect_driver(d: Dialect) -> DatabaseDriver {
    match d {
        Dialect::Postgres => DatabaseDriver::Postgres,
        Dialect::MySql => DatabaseDriver::MySql,
    }
}

pub(super) fn plugin_column_info((ordinal, field): (usize, &PluginField)) -> ColumnInfo {
    let _ = ordinal;
    ColumnInfo {
        name: field.name.clone(),
        data_type: field.field_type.clone(),
        nullable: field.nullable,
        default: None,
        max_length: None,
        is_primary_key: false,
        is_auto_increment: false,
    }
}
