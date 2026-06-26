use super::{
    MigrationSetting, context::SchemaSettingContext, driver::SchemaDriver, error::SettingsError,
    phase::MigrationSettingsPhase,
};
use async_trait::async_trait;
use connectors::{
    sql::metadata::provider::MetadataProvider, traits::introspector::SchemaIntrospector,
};
use engine_core::schema::{
    plan::SchemaPlan,
    schema_ops::{SchemaOp, SchemaOps},
};
use engine_processing::{context::PipelineContext, io::format::DataFormat};
use std::{slice, sync::Arc};
use tracing::info;

pub struct InferSchemaSetting<D: SchemaDriver> {
    context: SchemaSettingContext<D>,
}

#[async_trait]
impl<D: SchemaDriver> MigrationSetting for InferSchemaSetting<D> {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::InferSchema
    }

    fn can_apply(&self, ctx: &PipelineContext) -> bool {
        matches!(
            (ctx.source.format, ctx.destination.format),
            (DataFormat::MySql, DataFormat::Postgres)
        )
    }

    async fn plan(&mut self, ctx: &PipelineContext) -> Result<SchemaOps, SettingsError> {
        if !self.can_apply(ctx) {
            return Ok(SchemaOps::empty());
        }
        self.build_schema_ops().await
    }
}

impl<D: SchemaDriver> InferSchemaSetting<D> {
    pub async fn new(ctx: SchemaSettingContext<D>) -> Self {
        Self { context: ctx }
    }

    async fn build_schema_ops(&mut self) -> Result<SchemaOps, SettingsError> {
        let ctx = &self.context;

        // Check for existing destination
        if ctx.destination_exists().await? {
            info!("Skipping schema inference: destination table already exists");
            return Ok(SchemaOps::empty());
        }
        info!("Destination table not found—planning schema inference");

        let mut schema_plan = ctx.build_schema_plan().await?;

        // Build metadata graph for source tables
        let sources = slice::from_ref(&ctx.source.name);
        let introspector = ctx.destination.driver.clone() as Arc<dyn SchemaIntrospector>;
        let meta_graph =
            MetadataProvider::build_metadata_graph(introspector.as_ref(), sources).await?;

        // Add only those metadata entries that aren't already in schema plan
        for meta in meta_graph.values() {
            if !schema_plan.metadata_exists(&meta.name) {
                SchemaPlan::collect_schema_deps(meta, &mut schema_plan);
                schema_plan.add_metadata(&meta.name, meta.clone());
            }
        }

        Self::schema_plan_to_ops(&schema_plan).await
    }

    async fn schema_plan_to_ops(plan: &SchemaPlan) -> Result<SchemaOps, SettingsError> {
        let mut ops = SchemaOps::empty();

        // Enum queries -> pre (idempotent - safe to skip "already exists")
        for (sql, name) in plan.enum_queries() {
            ops.pre.push(SchemaOp {
                sql,
                description: format!("Create enum type '{}'", name),
                idempotent: true,
                skip_if_missing_ref: false,
            });
        }

        // Table queries -> pre
        for (sql, name) in plan.table_queries().await {
            ops.pre.push(SchemaOp {
                sql,
                description: format!("Create table '{}'", name),
                idempotent: false,
                skip_if_missing_ref: false,
            });
        }

        // FK queries -> post (created after data migration)
        for (sql, name) in plan.fk_queries() {
            ops.post.push(SchemaOp {
                sql,
                description: format!("Add foreign key constraint on '{}'", name),
                idempotent: false,
                skip_if_missing_ref: true,
            });
        }

        Ok(ops)
    }
}
