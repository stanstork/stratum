/// Commands sent from the TUI to the migration engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationCommand {
    PauseAll,
    CancelAll,
    RetryPipeline(String),
}
