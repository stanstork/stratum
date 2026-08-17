use super::ReportBuilder;
use super::endpoint::resolve_source;
use super::errors::{ReportBuilderError, ReportBuilderResult, SourceAnalyzerError};
use crate::plan::{
    pipeline::{cascade::CascadeTablePlan, plan::PipelinePlan},
    schema::{change::SchemaChange, types::SchemaChangeType},
};
use connectors::traits::introspector::SchemaIntrospector;
use engine_core::{context::exec::ConnectionPool, dispatch_driver};
use engine_runtime::dag::Dag;
use engine_schema::{
    graph_expander::{GraphExpander, GraphExpansionResult},
    plan::SchemaObjectFlags,
    schema_ops::{SchemaOp, SchemaOps},
    type_registry::{Dialect, TypeRegistry},
};
use engine_wasm::registry::PluginRegistry;
use model::execution::pipeline::Pipeline;
use model::execution::row_count::RowCount;
use model::transform::mapping::TransformationMetadata;
use std::sync::Arc;

/// Analyzes a single graph/cascade pipeline into one parent [`PipelinePlan`] whose
/// `cascade_tables` carry the per-table breakdown.
pub(super) struct GraphAnalyzer<'a> {
    builder: &'a ReportBuilder,
}

impl<'a> GraphAnalyzer<'a> {
    pub(super) fn new(builder: &'a ReportBuilder) -> Self {
        Self { builder }
    }

    /// Expand the FK closure and analyze every discovered table, then fold the
    /// results onto one parent plan (the whole closure runs as a single pipeline).
    pub(super) async fn analyze(
        &self,
        pipeline: &Pipeline,
        dag: &Dag,
        connections: &mut ConnectionPool,
        plugin_registry: &Arc<PluginRegistry>,
    ) -> ReportBuilderResult<PipelinePlan> {
        let expansion = expand_graph(pipeline, connections, plugin_registry).await?;
        let schema_changes = schema_ops_to_changes(&expansion.schema_ops);

        // Analyze every discovered table as its own single-table pipeline.
        let mut tables: Vec<&String> = expansion.discovered_tables.keys().collect();
        tables.sort();

        let mut cascade_tables = Vec::with_capacity(tables.len());
        let mut counts = Vec::with_capacity(tables.len());
        let mut diagnostics = Vec::new();
        let (mut conversions, mut unsafe_conversions) = (0usize, 0usize);

        for table in tables {
            let single = cascade_table_pipeline(pipeline, table);
            let sub = self
                .builder
                .analyze_db_pipeline(&single, dag, connections, plugin_registry, None)
                .await?;

            let row_count = sub.source.effective_row_count().clone();
            counts.push(row_count.clone());

            conversions += sub.data_flow_summary.type_conversions;
            unsafe_conversions += sub.data_flow_summary.unsafe_conversions;

            diagnostics.extend(sub.diagnostics.clone());
            cascade_tables.push(CascadeTablePlan {
                source_table: table.clone(),
                dest_table: sub.destination.table.clone(),
                row_count,
                columns: sub.source.columns.len(),
                primary_key: sub.source.primary_key.clone(),
                dest_exists: sub.destination.exists,
                mappings: sub.mappings.clone(),
                sample: sub.sample.clone(),
            });
        }

        // Collapse identical diagnostics.
        diagnostics
            .sort_unstable_by(|a, b| a.code.cmp(&b.code).then_with(|| a.message.cmp(&b.message)));
        diagnostics.dedup_by(|a, b| a.code == b.code && a.message == b.message);

        // The parent plan drives the summary/estimates/verdict. Analyze the root
        // table for its shape, then override the row total with the cascade sum.
        let root_single = cascade_table_pipeline(pipeline, &pipeline.source.table);
        let total = (!counts.is_empty()).then(|| RowCount::sum(counts.iter()));

        let mut plan = self
            .builder
            .analyze_db_pipeline(&root_single, dag, connections, plugin_registry, total)
            .await?;

        plan.name = pipeline.name.clone();
        plan.description = pipeline.description.clone();
        plan.schema_changes = schema_changes;
        plan.sample = None;
        plan.diagnostics = diagnostics;
        plan.data_flow_summary.type_conversions = conversions;
        plan.data_flow_summary.unsafe_conversions = unsafe_conversions;
        plan.cascade_tables = cascade_tables;

        Ok(plan)
    }
}

/// Synthesize the single-table pipeline for one discovered table of a cascade:
/// its source/destination tables, the destination rename from `map { }`, and the
/// field mappings that apply to it.
fn cascade_table_pipeline(pipeline: &Pipeline, table: &str) -> Pipeline {
    let mut single = pipeline.clone();

    single.source.table = table.to_string();
    single.source.graph_references = None;
    single.source.filters.clear();
    single.source.joins.clear();

    single.destination.table = single
        .destination
        .table_map
        .get(table)
        .cloned()
        .unwrap_or_else(|| table.to_string());
    single.destination.table_map.clear();

    // Pick the transforms that apply to this table.
    single.transformations = if table == pipeline.source.table {
        pipeline.transformations.clone()
    } else {
        pipeline
            .named_transformations
            .get(table)
            .cloned()
            .unwrap_or_default()
    };
    single.named_transformations.clear();

    // Validations are authored against the root table; don't apply them to the
    // other discovered tables.
    if table != pipeline.source.table {
        single.validations.clear();
    }

    single
}

/// Run the FK-graph expansion for a `with references` pipeline, mirroring the
/// expansion `apply` performs (same discovered closure and DDL).
async fn expand_graph(
    pipeline: &Pipeline,
    connections: &mut ConnectionPool,
    plugin_registry: &Arc<PluginRegistry>,
) -> ReportBuilderResult<GraphExpansionResult> {
    let src_ep = resolve_source(pipeline, connections, plugin_registry).await?;
    let src_driver = src_ep.db_driver().ok_or_else(|| {
        ReportBuilderError::Config("graph migrations require a database source".into())
    })?;
    let source_dialect = src_driver.dialect();

    let dest_driver = &pipeline.destination.connection.driver;
    let dest_dialect = Dialect::parse(dest_driver).ok_or_else(|| {
        ReportBuilderError::Config(format!(
            "destination driver '{dest_driver}' is not a SQL dialect"
        ))
    })?;

    let refs = pipeline
        .source
        .graph_references
        .as_ref()
        .expect("graph_references present");

    let mapping = TransformationMetadata::new(pipeline);
    let introspector: Arc<dyn SchemaIntrospector> =
        dispatch_driver!(src_driver, |d| d.clone() as Arc<dyn SchemaIntrospector>);
    let type_registry = Arc::new(TypeRegistry::new(source_dialect, dest_dialect));
    let expander = GraphExpander::new(introspector, type_registry, source_dialect);

    expander
        .expand(
            &pipeline.source.table,
            refs,
            &mapping,
            SchemaObjectFlags::from_pipeline(pipeline),
            false,
        )
        .await
        .map_err(|e| {
            ReportBuilderError::SourceAnalyzer(SourceAnalyzerError::QueryFailed(format!(
                "graph expansion failed for {}: {e}",
                pipeline.name
            )))
        })
}

/// Convert an ordered set of DDL ops into report schema changes.
pub(crate) fn schema_ops_to_changes(ops: &SchemaOps) -> Vec<SchemaChange> {
    ops.pre
        .iter()
        .chain(ops.post.iter())
        .map(op_to_change)
        .collect()
}

pub(crate) fn op_to_change(op: &SchemaOp) -> SchemaChange {
    let upper = op.sql.trim_start().to_uppercase();

    let change_type = if upper.starts_with("CREATE TABLE") {
        SchemaChangeType::CreateTable
    } else if upper.starts_with("CREATE TYPE") {
        SchemaChangeType::CreateEnum
    } else if upper.contains("INDEX") {
        SchemaChangeType::AddIndex
    } else {
        SchemaChangeType::AddConstraint
    };

    SchemaChange {
        change_type,
        entity: quoted_ident(&op.description),
        description: op.description.clone(),
        ddl: Some(op.sql.clone()),
        is_breaking: false,
        is_reversible: true,
    }
}

/// Extract the first quoted identifier from a schema-op description, e.g.
/// `Create table 'city'` -> `city`.
fn quoted_ident(desc: &str) -> String {
    for q in ['\'', '`', '"'] {
        if let Some(start) = desc.find(q)
            && let Some(len) = desc[start + 1..].find(q)
        {
            return desc[start + 1..start + 1 + len].to_string();
        }
    }
    String::new()
}
