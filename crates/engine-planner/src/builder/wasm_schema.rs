use crate::{
    builder::{
        ReportBuilder,
        endpoint::{PlanDestinationEndpoint, PlanSourceEndpoint},
        errors::{ReportBuilderError, ReportBuilderResult, SourceAnalyzerError},
    },
    plan::schema::{change::SchemaChange, types::SchemaChangeType},
};
use connectors::{
    sql::{metadata::table::TableMetadata, query::generator::QueryGenerator},
    traits::introspector::SchemaIntrospector,
};
use engine_config::settings::validated::ValidatedSettings;
use engine_core::{
    dispatch_driver,
    schema::{plan::SchemaPlan, type_registry::Dialect},
};
use engine_processing::io::source::plugin_introspector::PluginIntrospector;
use engine_wasm::registry::{PluginRegistry, plugin_columns};
use model::{
    execution::{flags::IntegrityMode, pipeline::Pipeline},
    transform::mapping::TransformationMetadata,
};
use std::sync::Arc;

/// Preview the schema changes for a WASM-source -> DB-destination pipeline.
pub(crate) async fn wasm_source_schema_changes(
    builder: &ReportBuilder,
    pipeline: &Pipeline,
    src_ep: &dyn PlanSourceEndpoint,
    dst_ep: &dyn PlanDestinationEndpoint,
    plugin_registry: &Arc<PluginRegistry>,
) -> ReportBuilderResult<Vec<SchemaChange>> {
    let (Some(plugin), Some(dest_driver)) = (src_ep.plugin_name(), dst_ep.db_driver()) else {
        return Ok(Vec::new());
    };

    let meta = plugin_registry.metadata(plugin).map_err(|e| {
        ReportBuilderError::SourceAnalyzer(SourceAnalyzerError::QueryFailed(format!(
            "could not load source plugin '{}': {}",
            plugin, e
        )))
    })?;
    if meta.output_schema.is_empty() {
        return Ok(Vec::new());
    }

    let dest_dialect = dest_driver.dialect();
    let introspector: Arc<dyn SchemaIntrospector> =
        Arc::new(PluginIntrospector::new(&meta.output_schema, dest_dialect));

    let mut mapping = TransformationMetadata::new(pipeline);
    mapping.set_plugin_columns(plugin_columns(pipeline, plugin_registry));

    let settings = ValidatedSettings::from_pipeline(&pipeline.settings, true, IntegrityMode::Off);
    let schema_plan = builder
        .build_schema_plan(pipeline, introspector, dest_dialect, &mapping, &settings)
        .await?;

    let dest_table = &pipeline.destination.table;
    let dest_exists =
        dispatch_driver!(dest_driver, |d| d.table_exists(dest_table).await).map_err(|e| {
            ReportBuilderError::SourceAnalyzer(SourceAnalyzerError::QueryFailed(format!(
                "could not check destination table '{}': {}",
                dest_table, e
            )))
        })?;

    if dest_exists {
        let dest_meta = dispatch_driver!(dest_driver, |d| d.table_metadata(dest_table).await)
            .map_err(|e| {
                ReportBuilderError::SourceAnalyzer(SourceAnalyzerError::QueryFailed(format!(
                    "could not introspect destination table '{}': {}",
                    dest_table, e
                )))
            })?;
        Ok(add_column_changes(dest_table, &schema_plan, &dest_meta, dest_dialect).await)
    } else {
        create_table_changes(dest_table, &schema_plan).await
    }
}

/// Build CREATE TABLE (+ enum/FK) changes from a fresh schema plan.
async fn create_table_changes(
    dest_table: &str,
    plan: &SchemaPlan,
) -> ReportBuilderResult<Vec<SchemaChange>> {
    let mut changes = Vec::new();

    let table_queries = plan.table_queries().await;
    let (create_sql, _) = table_queries
        .iter()
        .find(|q| q.1.eq_ignore_ascii_case(dest_table))
        .ok_or_else(|| {
            ReportBuilderError::SourceAnalyzer(SourceAnalyzerError::QueryFailed(format!(
                "No table query generated for target: {}",
                dest_table
            )))
        })?;

    changes.push(SchemaChange {
        change_type: SchemaChangeType::CreateTable,
        entity: dest_table.to_string(),
        description: format!("Create new table '{}'", dest_table),
        ddl: Some(create_sql.clone()),
        is_breaking: false,
        is_reversible: true,
    });

    for (sql, column_name) in plan.enum_queries() {
        changes.push(SchemaChange {
            change_type: SchemaChangeType::CreateEnum,
            entity: format!("{}.{}", dest_table, column_name),
            description: format!("Create custom enum type for column '{}'", column_name),
            ddl: Some(sql),
            is_breaking: false,
            is_reversible: true,
        });
    }

    for (sql, column_name) in plan.fk_queries() {
        changes.push(SchemaChange {
            change_type: SchemaChangeType::AddConstraint,
            entity: format!("{}.{}", dest_table, column_name),
            description: format!("Add foreign key constraint to column '{}'", column_name),
            ddl: Some(sql),
            is_breaking: false,
            is_reversible: true,
        });
    }

    Ok(changes)
}

/// Build ADD COLUMN changes for columns the existing destination is missing.
async fn add_column_changes(
    dest_table: &str,
    plan: &SchemaPlan,
    dest_meta: &TableMetadata,
    dest_dialect: Dialect,
) -> Vec<SchemaChange> {
    let mut changes = Vec::new();
    let planned_columns = plan.resolved_column_defs().await;
    let existing_columns = dest_meta.columns();

    let dialect = dest_dialect.as_query_dialect();
    let generator = QueryGenerator::new(dialect.as_ref());

    for planned_col in planned_columns {
        let col_name = planned_col.name();
        let exists = existing_columns
            .iter()
            .any(|c| c.name.eq_ignore_ascii_case(col_name));
        if exists {
            continue;
        }
        let (sql, _) = generator.add_column(dest_table, planned_col.clone());
        changes.push(SchemaChange {
            change_type: SchemaChangeType::AddColumn,
            entity: format!("{}.{}", dest_table, col_name),
            description: format!(
                "Add missing column '{}' to table '{}'",
                col_name, dest_table
            ),
            ddl: Some(sql),
            is_breaking: false,
            is_reversible: true,
        });
    }

    changes
}
