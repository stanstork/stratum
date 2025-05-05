// use super::{context::SchemaSettingContext, phase::MigrationSettingsPhase, MigrationSetting};
// use crate::context::MigrationContext;
// use async_trait::async_trait;
// use smql::plan::MigrationPlan;
// use sql_adapter::metadata::provider::MetadataProvider;
// use std::sync::Arc;
// use tokio::sync::Mutex;
// use tracing::info;

// pub struct InferSchemaSetting {
//     context: SchemaSettingContext,
// }

// #[async_trait]
// impl MigrationSetting for InferSchemaSetting {
//     fn phase(&self) -> MigrationSettingsPhase {
//         MigrationSettingsPhase::InferSchema
//     }

//     async fn apply(
//         &self,
//         plan: &MigrationPlan,
//         _context: Arc<Mutex<MigrationContext>>,
//     ) -> Result<(), Box<dyn std::error::Error>> {
//         self.apply_schema(plan).await?;

//         // Set the infer schema flag to global state
//         {
//             let mut state = self.context.state.lock().await;
//             state.infer_schema = true;
//         }

//         info!("Infer schema setting applied");
//         Ok(())
//     }
// }

// impl InferSchemaSetting {
//     pub async fn new(context: &Arc<Mutex<MigrationContext>>) -> Self {
//         Self {
//             context: SchemaSettingContext::new(context).await,
//         }
//     }

//     async fn apply_schema(&self, plan: &MigrationPlan) -> Result<(), Box<dyn std::error::Error>> {
//         let adapter = self.context.source_adapter().await?;
//         let mut schema_plan = self.context.build_schema_plan().await?;

//         for migration in plan.migration.migrations.iter() {
//             if !self.context.destination_exists(&migration.target).await? {
//                 info!("Destination table does not exist. Infer schema setting will be applied");

//                 let metadata_graph =
//                     MetadataProvider::build_metadata_graph(&*adapter, &migration.sources).await?;
//                 for meta in metadata_graph.values() {
//                     if !schema_plan.metadata_exists(&meta.name) {
//                         MetadataProvider::collect_schema_deps(meta, &mut schema_plan);
//                         schema_plan.add_metadata(&meta.name, meta.clone());
//                     }
//                 }
//             }
//         }

//         self.context.apply_to_destination(schema_plan).await
//     }
// }
