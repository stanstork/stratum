use super::{
    context::SchemaSettingContext, error::SettingsError, phase::MigrationSettingsPhase,
    MigrationSetting,
};
use crate::{
    context::item::ItemContext, destination::Destination, error::MigrationError,
    metadata::entity::EntityMetadata, schema::plan::SchemaPlan, source::Source,
    state::MigrationState,
};
use async_trait::async_trait;
use common::mapping::EntityMapping;
use sql_adapter::metadata::provider::MetadataProvider;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

pub struct InferSchemaSetting {
    context: SchemaSettingContext,
}

#[async_trait]
impl MigrationSetting for InferSchemaSetting {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::InferSchema
    }

    async fn apply(&self, _ctx: &mut ItemContext) -> Result<(), MigrationError> {
        self.apply_schema().await?;

        // Set the infer schema flag to global state
        {
            let mut state = self.context.state.lock().await;
            state.infer_schema = true;
        }

        info!("Infer schema setting applied");
        Ok(())
    }
}

impl InferSchemaSetting {
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

    async fn apply_schema(&self) -> Result<(), SettingsError> {
        let ctx = &self.context;

        let adapter = ctx.source_adapter().await?;
        let mut schema_plan = ctx.build_schema_plan().await?;

        // Check for existing destination
        if ctx.destination_exists().await? {
            info!("Skipping schema inference: destination table already exists");
            return Ok(());
        }
        info!("Destination table not found—applying schema inference");

        // Build metadata graph for source tables
        let sources = &[ctx.source.name.clone()];
        let meta_graph = MetadataProvider::build_metadata_graph(&*adapter, sources).await?;

        // Add only those metadata entries that aren't already in schema plan
        for meta in meta_graph.values() {
            if !schema_plan.metadata_exists(&meta.name) {
                SchemaPlan::collect_schema_deps(meta, &mut schema_plan);
                schema_plan.add_metadata(&meta.name, EntityMetadata::Table(meta.clone()));
            }
        }

        self.context.apply_to_destination(schema_plan).await
    }
}
