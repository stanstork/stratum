use crate::context::item::ItemContext;
use async_trait::async_trait;
use batch_size::BatchSizeSetting;
use constraints::IgnoreConstraintsSettings;
use phase::MigrationSettingsPhase;
use smql_v02::statements::setting::Settings;
use std::sync::Arc;
use tokio::sync::Mutex;

pub mod batch_size;
pub mod constraints;
pub mod context;
// pub mod create_cols;
// pub mod create_tables;
// pub mod infer_schema;
pub mod phase;

#[async_trait]
pub trait MigrationSetting: Send + Sync {
    fn phase(&self) -> MigrationSettingsPhase;
    async fn apply(&self, ctx: Arc<Mutex<ItemContext>>) -> Result<(), Box<dyn std::error::Error>>;
}

pub async fn collect_settings(
    cfg: &Settings,
    ctx: &Arc<Mutex<ItemContext>>,
) -> Vec<Box<dyn MigrationSetting>> {
    // Collect all settings based on the configuration
    let mut all: Vec<Box<dyn MigrationSetting>> = [
        // batch size > 0?
        cfg.batch_size
            .gt(&0)
            .then(|| Box::new(BatchSizeSetting(cfg.batch_size as i64)) as _),
        // ignore constraints?
        cfg.ignore_constraints
            .then(|| Box::new(IgnoreConstraintsSettings(true)) as _),
    ]
    .into_iter()
    .flatten()
    .collect();

    // Sort settings by phase to ensure they are applied in the correct order
    all.sort_by_key(|s| s.phase());

    all
}
