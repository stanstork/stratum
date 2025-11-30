use super::{MigrationSetting, phase::MigrationSettingsPhase};
use async_trait::async_trait;

pub struct IgnoreConstraintsSettings(pub bool);

#[async_trait]
impl MigrationSetting for IgnoreConstraintsSettings {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::IgnoreConstraints
    }
}
