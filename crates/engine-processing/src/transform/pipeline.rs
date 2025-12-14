use crate::transform::{
    error::TransformError,
    validation::{ValidationAction, ValidationResult},
};
use model::records::row::RowData;
use std::sync::Arc;
use tracing::warn;

pub trait Transform: Send + Sync {
    fn apply(&self, row: &mut RowData) -> Result<(), TransformError>;
}

/// Trait for filter-like transforms that decide whether to keep a row.
pub trait Filter: Send + Sync {
    fn should_keep(&self, row: &RowData) -> bool;
}

pub trait Validator: Send + Sync {
    fn validate(&self, row: &RowData) -> Result<ValidationResult, TransformError>;
}

pub trait TransformPipelineExt {
    fn add_if<T, F>(self, condition: bool, factory: F) -> Self
    where
        T: Transform + 'static,
        F: FnOnce() -> T;

    fn add_filter_if<F, Factory>(self, condition: bool, factory: Factory) -> Self
    where
        F: Filter + 'static,
        Factory: FnOnce() -> F;

    fn add_validator_if<V, Factory>(self, condition: bool, factory: Factory) -> Self
    where
        V: Validator + 'static,
        Factory: FnOnce() -> V;
}

#[derive(Clone)]
enum PipelineStage {
    Transform(Arc<dyn Transform>),
    Filter(Arc<dyn Filter>),
    Validation(Arc<dyn Validator>),
}

#[derive(Clone)]
pub struct TransformPipeline {
    stages: Vec<PipelineStage>,
}

impl TransformPipeline {
    pub fn new() -> Self {
        Self { stages: Vec::new() }
    }

    /// Apply pipeline to a single row in-place.
    pub fn apply(&self, row: &mut RowData) -> Result<bool, TransformError> {
        for stage in &self.stages {
            match stage {
                PipelineStage::Transform(transform) => {
                    transform.apply(row)?;
                }
                PipelineStage::Filter(filter) => {
                    if !filter.should_keep(row) {
                        return Ok(false);
                    }
                }
                PipelineStage::Validation(validator) => match validator.validate(row) {
                    Ok(res) => match res {
                        ValidationResult::Pass => {
                            // Row is valid, continue processing
                        }
                        ValidationResult::Failed {
                            rule,
                            message,
                            action,
                        } => match action {
                            ValidationAction::Skip => {
                                return Err(TransformError::FilteredOut);
                            }
                            ValidationAction::Fail => {
                                return Err(TransformError::ValidationFailed { rule, message });
                            }
                            ValidationAction::Warn => {
                                warn!("Validation '{}' failed: {} (continuing)", rule, message);
                                return Err(TransformError::FilteredOut);
                            }
                        },
                    },
                    Err(e) => return Err(e),
                },
            }
        }
        Ok(true)
    }

    pub fn apply_batch(
        &self,
        mut rows: Vec<RowData>,
    ) -> (Vec<RowData>, Vec<RowData>, Vec<(RowData, TransformError)>) {
        let mut successful = Vec::new();
        let mut filtered = Vec::new();
        let mut failed = Vec::new();

        // Process entire batch - collect all failures
        for mut row in rows.drain(..) {
            match self.apply(&mut row) {
                Ok(true) => successful.push(row),
                Ok(false) => filtered.push(row),
                Err(e) => {
                    // Collect failed row but continue processing batch
                    failed.push((row, e));
                }
            }
        }

        (successful, filtered, failed)
    }

    pub fn add_transform<T: Transform + 'static>(mut self, transform: T) -> Self {
        self.stages
            .push(PipelineStage::Transform(Arc::new(transform)));
        self
    }

    pub fn add_filter<F: Filter + 'static>(mut self, filter: F) -> Self {
        self.stages.push(PipelineStage::Filter(Arc::new(filter)));
        self
    }

    pub fn add_validator<V: Validator + 'static>(mut self, validator: V) -> Self {
        self.stages
            .push(PipelineStage::Validation(Arc::new(validator)));
        self
    }
}

impl TransformPipelineExt for TransformPipeline {
    fn add_if<T, F>(mut self, condition: bool, factory: F) -> Self
    where
        T: Transform + 'static,
        F: FnOnce() -> T,
    {
        if condition {
            self = self.add_transform(factory());
        }
        self
    }

    fn add_filter_if<F, Factory>(mut self, condition: bool, factory: Factory) -> Self
    where
        F: Filter + 'static,
        Factory: FnOnce() -> F,
    {
        if condition {
            self = self.add_filter(factory());
        }
        self
    }

    fn add_validator_if<V, Factory>(mut self, condition: bool, factory: Factory) -> Self
    where
        V: Validator + 'static,
        Factory: FnOnce() -> V,
    {
        if condition {
            self = self.add_validator(factory());
        }
        self
    }
}

impl Default for TransformPipeline {
    fn default() -> Self {
        Self::new()
    }
}
