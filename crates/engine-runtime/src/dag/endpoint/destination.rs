use super::{DestinationEndpoint, HookPhase, SourceEndpoint};
use crate::error::MigrationError;
use async_trait::async_trait;
use connectors::sql::metadata::{column::ColumnMetadata, table::TableMetadata};
use engine_config::settings::{self, ValidatedSettings};
use engine_core::{
    dispatch_driver,
    drivers::DriverRef,
    schema::{
        schema_ops::{SchemaOp, SchemaOps},
        type_registry::Dialect,
    },
};
use engine_processing::{
    context::PipelineContext,
    hooks::executor::HookExecutor,
    io::destination::{Destination, IntoDestination},
};
use engine_wasm::{
    registry::PluginRegistry, runtime::instance::PluginInstance, schema::PluginField,
};
use model::execution::{
    flags::IntegrityMode,
    pipeline::{LifecycleHooks, Pipeline},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub struct DbDestinationEndpoint(pub DriverRef);

/// A WASM-plugin destination.
pub struct WasmDestinationEndpoint {
    plugin: String,
    instance: Mutex<Option<PluginInstance>>,
    input_schema: Vec<PluginField>,
}

impl WasmDestinationEndpoint {
    /// Instantiate the plugin and cache its declared input schema.
    pub fn new(registry: Arc<PluginRegistry>, plugin: String) -> Result<Self, MigrationError> {
        let instance = registry.instantiate(&plugin)?;
        let input_schema = instance.metadata().input_schema.clone();
        Ok(Self {
            plugin,
            instance: Mutex::new(Some(instance)),
            input_schema,
        })
    }

    fn take_instance(&self) -> Result<PluginInstance, MigrationError> {
        self.instance.lock().unwrap().take().ok_or_else(|| {
            MigrationError::PipelineFailed(format!(
                "wasm plugin '{}' already consumed; build() can only be called once",
                self.plugin
            ))
        })
    }
}

#[async_trait]
impl DestinationEndpoint for DbDestinationEndpoint {
    async fn build(
        &self,
        pipeline: &Pipeline,
        source_dialect: Option<Dialect>,
    ) -> Result<Destination, MigrationError> {
        let src_dialect = source_dialect.unwrap_or_else(|| self.0.dialect());
        let dest = dispatch_driver!(&self.0, |d| {
            d.clone()
                .into_destination(&pipeline.destination.table, src_dialect)
        });
        Ok(dest)
    }

    async fn plan_settings(
        &self,
        ctx: &mut PipelineContext,
        source: &dyn SourceEndpoint,
        pipeline: &Pipeline,
        dry_run: bool,
        integrity: IntegrityMode,
    ) -> Result<(ValidatedSettings, SchemaOps), MigrationError> {
        let Some((src_introspector, src_dialect)) = source.schema_introspector(self.0.dialect())
        else {
            return Ok((
                ValidatedSettings::from_pipeline(&pipeline.settings, dry_run, integrity),
                SchemaOps::empty(),
            ));
        };
        let result = dispatch_driver!(&self.0, |d| {
            settings::validate_and_plan(
                ctx,
                src_introspector.clone(),
                src_dialect,
                d.clone(),
                &pipeline.settings,
                dry_run,
                integrity,
            )
            .await?
        });
        Ok(result)
    }

    async fn apply_schema_ops(&self, ops: &[SchemaOp], phase: &str) -> Result<(), MigrationError> {
        dispatch_driver!(&self.0, |d| {
            settings::apply_schema_ops(d.as_ref(), ops)
                .await
                .map_err(|e| {
                    MigrationError::PipelineFailed(format!("{phase} schema operation failed: {e}"))
                })?
        });
        Ok(())
    }

    async fn run_hooks(
        &self,
        phase: HookPhase,
        hooks: &LifecycleHooks,
    ) -> Result<(), MigrationError> {
        dispatch_driver!(&self.0, |d| {
            let mut ex = HookExecutor::new(d.clone(), hooks.clone());
            let r = match phase {
                HookPhase::Before => ex.execute_before().await,
                HookPhase::After => ex.execute_after().await,
            };
            r.map_err(|e| {
                MigrationError::HookExecutionFailed(format!("{phase:?} hooks failed: {e}"))
            })?;
        });
        Ok(())
    }

    async fn destination_metadata(
        &self,
        ctx: &PipelineContext,
        cascade_tables: &[String],
    ) -> Result<Vec<TableMetadata>, MigrationError> {
        if cascade_tables.is_empty() {
            let meta = self
                .0
                .table_metadata(&ctx.destination.name)
                .await
                .map_err(|e| {
                    MigrationError::PipelineFailed(format!(
                        "Failed to get destination metadata: {e}"
                    ))
                })?;
            return Ok(vec![meta]);
        }
        let mut metas = Vec::with_capacity(cascade_tables.len());
        for table in cascade_tables {
            metas.push(self.0.table_metadata(table).await.map_err(|e| {
                MigrationError::PipelineFailed(format!("cascade dest metadata '{table}': {e}"))
            })?);
        }
        Ok(metas)
    }
}

#[async_trait]
impl DestinationEndpoint for WasmDestinationEndpoint {
    async fn build(
        &self,
        pipeline: &Pipeline,
        _source_dialect: Option<Dialect>,
    ) -> Result<Destination, MigrationError> {
        let instance = self.take_instance()?;
        Ok(Destination::wasm(instance, &pipeline.destination.table))
    }

    async fn plan_settings(
        &self,
        _ctx: &mut PipelineContext,
        _source: &dyn SourceEndpoint,
        pipeline: &Pipeline,
        dry_run: bool,
        integrity: IntegrityMode,
    ) -> Result<(ValidatedSettings, SchemaOps), MigrationError> {
        // Plugin manages its own schema; no DDL.
        Ok((
            ValidatedSettings::from_pipeline(&pipeline.settings, dry_run, integrity),
            SchemaOps::empty(),
        ))
    }

    async fn apply_schema_ops(
        &self,
        _ops: &[SchemaOp],
        _phase: &str,
    ) -> Result<(), MigrationError> {
        // plan_settings returns empty ops, so the orchestrator never calls this
        // with non-empty input. Guard loudly in case that invariant breaks.
        Err(MigrationError::PipelineFailed(
            "schema operations are not supported on a WASM destination".into(),
        ))
    }

    async fn run_hooks(
        &self,
        _phase: HookPhase,
        _hooks: &LifecycleHooks,
    ) -> Result<(), MigrationError> {
        Err(MigrationError::HookExecutionFailed(
            "lifecycle SQL hooks are not supported on a WASM destination".into(),
        ))
    }

    async fn destination_metadata(
        &self,
        ctx: &PipelineContext,
        _cascade: &[String],
    ) -> Result<Vec<TableMetadata>, MigrationError> {
        // Authoritative metadata from the plugin's declared `input` schema
        // (set via `#[stratum_sink(input = [...])]` and round-tripped through
        // `__stratum_metadata`). The destination table name comes from ctx.
        Ok(vec![dest_meta_from_schema(
            &ctx.destination.name,
            &self.input_schema,
        )])
    }
}

/// Build a TableMetadata from a sink plugin's declared input schema. Column
/// names *and* types are authoritative - the plugin tells us exactly which
/// columns it expects and what type tag each one has.
fn dest_meta_from_schema(table: &str, schema: &[PluginField]) -> TableMetadata {
    let columns = schema
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let name = f.name.clone();
            (
                name.clone(),
                ColumnMetadata {
                    ordinal: i,
                    name,
                    data_type: f.field_type.clone(),
                    is_nullable: f.nullable,
                    ..Default::default()
                },
            )
        })
        .collect();
    TableMetadata {
        name: table.to_string(),
        schema: None,
        columns,
        primary_keys: Vec::new(),
        foreign_keys: Vec::new(),
        referenced_tables: HashMap::new(),
        referencing_tables: HashMap::new(),
    }
}
