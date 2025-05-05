// use super::{context::SchemaSettingContext, phase::MigrationSettingsPhase, MigrationSetting};
// use crate::{context::MigrationContext, metadata::fetch_src_tbl_metadata};
// use async_trait::async_trait;
// use std::sync::Arc;
// use tokio::sync::Mutex;
// use tracing::info;

// pub struct CreateMissingTablesSetting {
//     context: SchemaSettingContext,
// }

// #[async_trait]
// impl MigrationSetting for CreateMissingTablesSetting {
//     fn phase(&self) -> MigrationSettingsPhase {
//         MigrationSettingsPhase::CreateMissingTables
//     }

//     async fn apply(
//         &self,
//         plan: &smql::plan::MigrationPlan,
//         _context: Arc<Mutex<MigrationContext>>,
//     ) -> Result<(), Box<dyn std::error::Error>> {
//         let mut schema_plan = self.context.build_schema_plan().await?;

//         for dest in plan.migration.targets() {
//             if self.context.destination_exists(&dest).await? {
//                 continue;
//             }

//             // reverse‐map destination → source
//             let src = self.context.mapping.entity_name_map.reverse_resolve(&dest);
//             let meta = fetch_src_tbl_metadata(&self.context.source.primary, &src).await?;

//             // add columns, FKs, enums into plan
//             schema_plan.add_column_defs(
//                 &meta.name,
//                 meta.column_defs(schema_plan.type_engine().type_converter()),
//             );
//             for fk in meta.fk_defs() {
//                 if self
//                     .context
//                     .mapping
//                     .entity_name_map
//                     .contains_key(&fk.referenced_table)
//                 {
//                     schema_plan.add_fk_def(&meta.name, fk.clone());
//                 }
//             }
//             for col in (schema_plan.type_engine().type_extractor())(&meta) {
//                 schema_plan.add_enum_def(&meta.name, &col.name);
//             }
//             schema_plan.add_metadata(&src, meta);
//         }

//         self.context.apply_to_destination(schema_plan).await?;

//         // Set the create missing tables flag to global state
//         {
//             let mut state = self.context.state.lock().await;
//             state.create_missing_tables = true;
//         }

//         info!("Create missing tables setting applied");
//         Ok(())
//     }
// }

// impl CreateMissingTablesSetting {
//     pub async fn new(context: &Arc<Mutex<MigrationContext>>) -> Self {
//         Self {
//             context: SchemaSettingContext::new(context).await,
//         }
//     }
// }
