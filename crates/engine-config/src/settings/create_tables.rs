use super::{MigrationSetting, context::SchemaSettingContext, phase::MigrationSettingsPhase};
use crate::settings::{error::SettingsError, validated::ValidatedSettings};
use async_trait::async_trait;
use engine_core::{
    connectors::{destination::Destination, source::Source},
    context::item::ItemContext,
};
use model::transform::mapping::TransformationMetadata;
use tracing::info;

pub struct CreateMissingTablesSetting {
    context: SchemaSettingContext,
}

#[async_trait]
impl MigrationSetting for CreateMissingTablesSetting {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::CreateMissingTables
    }

    async fn apply(&mut self, _ctx: &mut ItemContext) -> Result<(), SettingsError> {
        // If the table already exists, bail out
        if self.context.destination_exists().await? {
            info!("Destination table already exists; skipping schema creation.");
            return Ok(());
        }

        // Resolve source name from the destination
        let dest_name = &self.context.destination.name;
        let src_name = self.context.mapping.entities.reverse_resolve(dest_name);

        let schema_planner = self.context.init_schema_planner().await?;
        let plan = schema_planner.plan_schema(&src_name).await?;

        self.context.apply_to_destination(plan).await?;

        info!("Create missing tables setting applied");
        Ok(())
    }
}

impl CreateMissingTablesSetting {
    pub async fn new(
        src: &Source,
        dest: &Destination,
        mapping: &TransformationMetadata,
        settings: &ValidatedSettings,
    ) -> Self {
        Self {
            context: SchemaSettingContext::new(src, dest, mapping, settings).await,
        }
    }
}
