use super::{
    MigrationSetting, context::SchemaSettingContext, error::SettingsError,
    phase::MigrationSettingsPhase,
};
use crate::{report::dry_run::DryRunReport, settings::validated::ValidatedSettings};
use async_trait::async_trait;
use connectors::{
    metadata::entity::EntityMetadata, sql::base::metadata::provider::MetadataProvider,
};
use engine_core::{
    connectors::{destination::Destination, source::Source},
    context::item::ItemContext,
    schema::plan::SchemaPlan,
};
use futures::lock::Mutex;
use model::transform::mapping::EntityMapping;
use smql_syntax::ast::connection::DataFormat;
use std::{slice, sync::Arc};
use tracing::info;

pub struct InferSchemaSetting {
    context: SchemaSettingContext,
}

#[async_trait]
impl MigrationSetting for InferSchemaSetting {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::InferSchema
    }

    fn can_apply(&self, ctx: &ItemContext) -> bool {
        matches!(
            (ctx.source.format, ctx.destination.format),
            (DataFormat::MySql, DataFormat::Postgres)
        )
    }

    async fn apply(&mut self, _ctx: &mut ItemContext) -> Result<(), SettingsError> {
        self.apply_schema().await?;

        info!("Infer schema setting applied");
        Ok(())
    }
}

impl InferSchemaSetting {
    pub async fn new(
        src: &Source,
        dest: &Destination,
        mapping: &EntityMapping,
        settings: &ValidatedSettings,
        dry_run_report: &Arc<Mutex<DryRunReport>>,
    ) -> Self {
        Self {
            context: SchemaSettingContext::new(src, dest, mapping, settings, dry_run_report).await,
        }
    }

    async fn apply_schema(&mut self) -> Result<(), SettingsError> {
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
        let sources = slice::from_ref(&ctx.source.name);
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
