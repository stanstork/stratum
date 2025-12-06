use super::{MigrationSetting, phase::MigrationSettingsPhase};
use async_trait::async_trait;
use smql_syntax::ast_v2::setting::CopyColumns;

pub struct CopyColumnsSetting(pub CopyColumns);

#[async_trait]
impl MigrationSetting for CopyColumnsSetting {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::BatchSize
    }
}
