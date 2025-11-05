use super::{MigrationSetting, phase::MigrationSettingsPhase};
use crate::settings::error::SettingsError;
use async_trait::async_trait;
use engine_core::context::item::ItemContext;
use smql_syntax::ast::setting::CopyColumns;
use tracing::info;

pub struct CopyColumnsSetting(pub CopyColumns);

#[async_trait]
impl MigrationSetting for CopyColumnsSetting {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::BatchSize
    }

    async fn apply(&mut self, ctx: &mut ItemContext) -> Result<(), SettingsError> {
        ctx.settings.set_copy_columns(self.0);
        info!("Copy columns setting applied");
        Ok(())
    }
}
