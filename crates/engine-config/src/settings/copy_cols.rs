use super::{MigrationSetting, phase::MigrationSettingsPhase};
use crate::settings::CopyColumns;
use async_trait::async_trait;

pub struct CopyColumnsSetting(pub CopyColumns);

#[async_trait]
impl MigrationSetting for CopyColumnsSetting {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::BatchSize
    }
}
