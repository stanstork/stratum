#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MigrationSettingsPhase {
    BatchSize,
    IgnoreConstraints,
    CopyColumns,
    InferSchema,
    CreateMissingTables,
    CreateMissingColumns,
    CascadeSchema,
}
