/// Commands sent from TUI to migration engine
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationCommand {
    PauseAll,
    ResumeAll,
    PausePipeline(String),
    ResumePipeline(String),
    CancelPipeline(String),
    CancelAll,
    RetryPipeline(String),
}
