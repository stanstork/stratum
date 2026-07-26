use crate::error::MigrationError;
use async_trait::async_trait;
use connectors::{sql::metadata::table::TableMetadata, traits::introspector::SchemaIntrospector};
pub use destination::{DbDestinationEndpoint, WasmDestinationEndpoint};
use engine_config::settings::ValidatedSettings;
use engine_core::{
    context::exec::ExecutionContext,
    schema::{
        schema_ops::{SchemaOp, SchemaOps},
        type_registry::Dialect,
    },
};
use engine_processing::{
    context::PipelineContext,
    io::{destination::Destination, format::DataFormat, source::Source},
};
use engine_wasm::registry::PluginRegistry;
use model::{
    execution::{
        connection::Connection,
        flags::IntegrityMode,
        pipeline::{LifecycleHooks, Pipeline},
    },
    transform::mapping::TransformationMetadata,
};
use query_builder::offsets::OffsetStrategy;
pub use source::{CsvSourceEndpoint, DbSourceEndpoint, WasmSourceEndpoint};
use std::sync::Arc;

mod destination;
mod source;

#[derive(Debug, Clone, Copy)]
pub enum HookPhase {
    Before,
    After,
}

/// What building a source produces: the runtime reader plus any schema ops /
/// cascade tables that source-side graph expansion implied.
pub struct SourceArtifacts {
    pub source: Source,
    pub schema_ops: Option<SchemaOps>,
    pub cascade_tables: Vec<String>,
}

#[async_trait]
pub trait SourceEndpoint: Send + Sync {
    async fn build(
        &self,
        pipeline: &Pipeline,
        mapping: &TransformationMetadata,
        offset_strategy: Arc<dyn OffsetStrategy>,
    ) -> Result<SourceArtifacts, MigrationError>;

    /// SQL dialect, if this source has one. `None` for plugin sources.
    fn dialect(&self) -> Option<Dialect>;

    /// If this source is a DB table with exactly one integer primary key,
    /// returns `(pk_column, min, max)` so the scan can be split into parallel
    /// range lanes. `None` (the default) disables lanes - the pipeline runs as a
    /// single scan.
    async fn int_key_range(&self, _table: &str) -> Option<(String, u64, u64)> {
        None
    }

    /// A `SchemaIntrospector` for this source plus the dialect its metadata
    /// should be interpreted in, if the source can describe its own schema.
    fn schema_introspector(
        &self,
        dest_dialect: Dialect,
    ) -> Option<(Arc<dyn SchemaIntrospector>, Dialect)>;
}

#[async_trait]
pub trait DestinationEndpoint: Send + Sync {
    async fn build(
        &self,
        pipeline: &Pipeline,
        source_dialect: Option<Dialect>,
    ) -> Result<Destination, MigrationError>;

    /// Like [`build`](Self::build), but backed by its own connection so a
    /// parallel lane writes independently. Defaults to [`build`](Self::build)
    /// (a shared handle) - only DB destinations with a single write connection
    /// override it.
    async fn build_lane(
        &self,
        pipeline: &Pipeline,
        source_dialect: Option<Dialect>,
    ) -> Result<Destination, MigrationError> {
        self.build(pipeline, source_dialect).await
    }

    /// Validate settings + plan DDL. No-op (empty ops) for plugin destinations
    /// or when the source is a plugin.
    async fn plan_settings(
        &self,
        ctx: &mut PipelineContext,
        source: &dyn SourceEndpoint,
        pipeline: &Pipeline,
        dry_run: bool,
        integrity: IntegrityMode,
    ) -> Result<(ValidatedSettings, SchemaOps), MigrationError>;

    /// Execute already-deduped DDL for one phase.
    async fn apply_schema_ops(&self, ops: &[SchemaOp], phase: &str) -> Result<(), MigrationError>;

    /// Run lifecycle SQL hooks for one phase.
    async fn run_hooks(
        &self,
        phase: HookPhase,
        hooks: &LifecycleHooks,
    ) -> Result<(), MigrationError>;

    /// Destination table metadata for the consumer (introspected for DB,
    /// synthesized for plugin).
    async fn destination_metadata(
        &self,
        ctx: &PipelineContext,
        cascade_tables: &[String],
    ) -> Result<Vec<TableMetadata>, MigrationError>;
}

fn wasm_plugin_name(conn: &Connection) -> Result<String, MigrationError> {
    conn.properties.get_string("plugin").ok_or_else(|| {
        MigrationError::PipelineFailed(format!(
            "wasm connection '{}' is missing required property `plugin`",
            conn.name
        ))
    })
}

pub async fn resolve_source(
    conn: &Connection,
    exec: &ExecutionContext,
    registry: &Arc<PluginRegistry>,
) -> Result<Box<dyn SourceEndpoint>, MigrationError> {
    match DataFormat::parse(&conn.driver) {
        Some(DataFormat::Wasm) => Ok(Box::new(WasmSourceEndpoint {
            registry: registry.clone(),
            plugin: wasm_plugin_name(conn)?,
        })),
        Some(DataFormat::Csv) => Ok(Box::new(CsvSourceEndpoint::new(conn).await?)),
        _ => Ok(Box::new(DbSourceEndpoint(exec.resolve_driver(conn).await?))),
    }
}

pub async fn resolve_destination(
    conn: &Connection,
    exec: &ExecutionContext,
    registry: &Arc<PluginRegistry>,
) -> Result<Box<dyn DestinationEndpoint>, MigrationError> {
    match DataFormat::parse(&conn.driver) {
        Some(DataFormat::Wasm) => Ok(Box::new(WasmDestinationEndpoint::new(
            registry.clone(),
            wasm_plugin_name(conn)?,
        )?)),
        _ => Ok(Box::new(DbDestinationEndpoint(
            exec.resolve_driver(conn).await?,
        ))),
    }
}
