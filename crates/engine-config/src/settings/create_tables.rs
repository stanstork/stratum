use super::{
    MigrationSetting, context::SchemaSettingContext, driver::SchemaDriver,
    phase::MigrationSettingsPhase,
};
use crate::settings::error::SettingsError;
use async_trait::async_trait;
use connectors::drivers::postgres::config::PkCreation;
use engine_core::schema::schema_ops::SchemaOps;
use engine_processing::context::PipelineContext;
use tracing::{info, warn};

pub struct CreateMissingTablesSetting<D: SchemaDriver> {
    context: SchemaSettingContext<D>,
}

#[async_trait]
impl<D: SchemaDriver> MigrationSetting for CreateMissingTablesSetting<D> {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::CreateMissingTables
    }

    async fn plan(&mut self, _ctx: &PipelineContext) -> Result<SchemaOps, SettingsError> {
        self.build_schema_ops().await
    }
}

impl<D: SchemaDriver> CreateMissingTablesSetting<D> {
    pub fn new(ctx: SchemaSettingContext<D>) -> Self {
        Self { context: ctx }
    }

    async fn build_schema_ops(&self) -> Result<SchemaOps, SettingsError> {
        let settings = &self.context.settings;
        let defer_pk = settings.pk_creation() == PkCreation::Post && !settings.skip_pk();

        // If the table already exists, bail out. `pk_creation = "post"` only
        // applies to tables we create, so warn and leave an existing PK as-is.
        if self.context.destination_exists().await? {
            if defer_pk {
                warn!(
                    table = %self.context.destination.name,
                    "pk_creation=\"post\" ignored: destination table already exists; leaving its primary key in place"
                );
            }
            info!("destination table already exists, skipping schema creation");
            return Ok(SchemaOps::empty());
        }

        // Resolve source name from the destination
        let dest_name = &self.context.destination.name;
        let src_name = self.context.mapping.entities.reverse_resolve(dest_name);

        let schema_planner = self.context.init_schema_planner().await?;
        let mut plan = schema_planner.plan_schema(&src_name).await?;

        // With `pk_creation = "post"` the tables are created without their primary
        // key and it is added back after the load (in the post phase, before FKs),
        // so per-row index maintenance doesn't slow the bulk COPY.
        plan.defer_pk(defer_pk);

        info!("planned create-missing-tables");

        Ok(plan.build_ops())
    }
}
