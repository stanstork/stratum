use thiserror::Error;

#[derive(Error, Debug)]
pub enum DagError {
    #[error("Circular dependency detected: {0}")]
    CircularDependency(String),

    #[error("Pipeline '{pipeline}' depends on non-existent pipeline '{dependency}'")]
    MissingDependency {
        pipeline: String,
        dependency: String,
    },

    #[error("Pipeline '{0}' already exists")]
    DuplicatePipeline(String),

    #[error("Empty pipeline list")]
    EmptyPipelines,
}

#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error("Pipeline '{pipeline}' failed: {reason}")]
    PipelineFailed { pipeline: String, reason: String },

    #[error("Multiple pipelines failed: {0:?}")]
    MultiplePipelinesFailed(Vec<String>),

    #[error("Pipeline '{0}' not found")]
    PipelineNotFound(String),
}
