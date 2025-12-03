use super::{MigrationSetting, phase::MigrationSettingsPhase};
use async_trait::async_trait;

pub struct BatchSizeSetting(pub i64);

#[async_trait]
impl MigrationSetting for BatchSizeSetting {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::BatchSize
    }
}
