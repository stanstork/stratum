use super::{context::SchemaSettingContext, phase::MigrationSettingsPhase, MigrationSetting};
use crate::{
    context::item::ItemContext, destination::Destination, error::MigrationError,
    metadata::entity::EntityMetadata, schema::plan::SchemaPlan, source::Source,
    state::MigrationState,
};
use async_trait::async_trait;
use common::mapping::EntityMapping;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

pub struct CreateMissingTablesSetting {
    context: SchemaSettingContext,
}

#[async_trait]
impl MigrationSetting for CreateMissingTablesSetting {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::CreateMissingTables
    }

    async fn apply(&self, _ctx: &mut ItemContext) -> Result<(), MigrationError> {
        if self.context.destination_exists().await? {
            info!("Destination table already exists. Create missing tables setting will not be applied");
            return Ok(());
        }

        // Target table name
        let dest = self.context.destination.name.clone();

        // reverse‐map destination -> source
        let src = self.context.mapping.entity_name_map.reverse_resolve(&dest);
        let meta = self.context.source.primary.fetch_meta(src.clone()).await?;

        let mut schema_plan = self.context.build_schema_plan().await?;

        // add columns, FKs, enums into plan
        schema_plan.add_column_defs(&meta.name(), SchemaPlan::column_defs(&schema_plan, &meta));

        let meta = match meta {
            EntityMetadata::Table(meta) => meta,
            _ => panic!("Expected table metadata"),
        };

        // add foreign keys
        for fk in meta.fk_defs() {
            if self
                .context
                .mapping
                .entity_name_map
                .contains_key(&fk.referenced_table)
            {
                schema_plan.add_fk_def(&meta.name, fk.clone());
            }
        }

        // add enums
        for col in (schema_plan.type_engine().type_extractor())(&meta) {
            schema_plan.add_enum_def(&meta.name, &col.name);
        }

        schema_plan.add_metadata(&src, meta);

        // apply the schema plan to the destination
        self.context.apply_to_destination(schema_plan).await?;

        // Set the create missing tables flag to global state
        {
            let mut state = self.context.state.lock().await;
            state.create_missing_tables = true;
        }

        info!("Create missing tables setting applied");
        Ok(())
    }
}

impl CreateMissingTablesSetting {
    pub fn new(
        src: &Source,
        dest: &Destination,
        mapping: &EntityMapping,
        state: &Arc<Mutex<MigrationState>>,
    ) -> Self {
        Self {
            context: SchemaSettingContext::new(src, dest, mapping, state),
        }
    }
}
