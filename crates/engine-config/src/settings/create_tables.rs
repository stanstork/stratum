use super::{
    MigrationSetting, context::SchemaSettingContext, driver::SchemaDriver,
    phase::MigrationSettingsPhase,
};
use crate::settings::error::SettingsError;
use async_trait::async_trait;
use engine_core::schema::schema_ops::{SchemaOp, SchemaOps};
use engine_processing::context::PipelineContext;
use tracing::info;

pub struct CreateMissingTablesSetting<S: SchemaDriver, D: SchemaDriver> {
    context: SchemaSettingContext<S, D>,
}

#[async_trait]
impl<S: SchemaDriver, D: SchemaDriver> MigrationSetting for CreateMissingTablesSetting<S, D> {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::CreateMissingTables
    }

    async fn plan(&mut self, _ctx: &PipelineContext) -> Result<SchemaOps, SettingsError> {
        self.build_schema_ops().await
    }
}

impl<S: SchemaDriver, D: SchemaDriver> CreateMissingTablesSetting<S, D> {
    pub async fn new(ctx: SchemaSettingContext<S, D>) -> Self {
        Self { context: ctx }
    }

    async fn build_schema_ops(&self) -> Result<SchemaOps, SettingsError> {
        // If the table already exists, bail out
        if self.context.destination_exists().await? {
            info!("Destination table already exists; skipping schema creation.");
            return Ok(SchemaOps::empty());
        }

        // Resolve source name from the destination
        let dest_name = &self.context.destination.name;
        let src_name = self.context.mapping.entities.reverse_resolve(dest_name);

        let schema_planner = self.context.init_schema_planner().await?;
        let plan = schema_planner.plan_schema(&src_name).await?;

        let mut ops = SchemaOps::empty();

        // Enum queries -> pre (idempotent)
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

        // FK queries -> post
        for (sql, name) in plan.fk_queries() {
            ops.post.push(SchemaOp {
                sql,
                description: format!("Add foreign key constraint on '{}'", name),
                idempotent: false,
                skip_if_missing_ref: true,
            });
        }

        info!("Create missing tables setting planned");
        Ok(ops)
    }
}
