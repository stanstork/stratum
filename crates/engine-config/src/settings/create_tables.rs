use super::{MigrationSetting, context::SchemaSettingContext, phase::MigrationSettingsPhase};
use crate::{report::dry_run::DryRunReport, settings::error::SettingsError};
use async_trait::async_trait;
use connectors::metadata::entity::EntityMetadata;
use engine_core::{
    connectors::{destination::Destination, source::Source},
    context::item::ItemContext,
    migration_state::MigrationSettings,
};
use futures::lock::Mutex;
use model::transform::mapping::EntityMapping;
use std::sync::Arc;
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
        let src_name = self
            .context
            .mapping
            .entity_name_map
            .reverse_resolve(dest_name);

        // Fetch metadata and build an empty plan
        let meta = self
            .context
            .source
            .primary
            .fetch_meta(src_name.clone())
            .await?;
        let mut plan = self.context.build_schema_plan().await?;

        // Add all columns
        plan.add_column_defs(&meta.name(), plan.column_defs(&meta));

        // If this is SQL table, wire up FKs and enums
        if let EntityMetadata::Table(table_meta) = &meta {
            for fk in table_meta.fk_defs() {
                if self
                    .context
                    .mapping
                    .entity_name_map
                    .contains_key(&fk.referenced_table)
                {
                    plan.add_fk_def(&table_meta.name, fk.clone());
                }
            }

            let extract_enums = plan.type_engine().enum_extractor();
            for col in extract_enums(table_meta) {
                plan.add_enum_def(&table_meta.name, &col.name);
            }
        }

        // Stamp in the metadata and apply the plan
        plan.add_metadata(&src_name, meta.clone());
        self.context.apply_to_destination(plan).await?;

        // Set the create missing tables flag to global settings
        {
            let mut settings = self.context.settings.lock().await;
            settings.set_create_missing_tables(true);
        }

        info!("Create missing tables setting applied");
        Ok(())
    }
}

impl CreateMissingTablesSetting {
    pub async fn new(
        src: &Source,
        dest: &Destination,
        mapping: &EntityMapping,
        settings: &Arc<Mutex<MigrationSettings>>,
        dry_run_report: &Arc<Mutex<Option<DryRunReport>>>,
    ) -> Self {
        Self {
            context: SchemaSettingContext::new(src, dest, mapping, settings, dry_run_report).await,
        }
    }
}
