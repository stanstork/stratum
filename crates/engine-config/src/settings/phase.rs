#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MigrationSettingsPhase {
    CreateMissingTables,
    CreateMissingColumns,
}
