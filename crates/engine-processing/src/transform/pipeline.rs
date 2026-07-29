use crate::transform::{
    error::TransformError,
    validation::{ValidationAction, ValidationResult},
};
use model::records::Record;
use std::sync::Arc;
use tracing::warn;

/// Outcome of applying a transformation pipeline to a row
#[derive(Debug, Clone, PartialEq)]
pub enum ApplyOutcome {
    /// Row passed all transformations and validations
    Success,
    /// Row was filtered out (excluded from processing)
    Skipped {
        /// Optional reason why the row was filtered
        reason: Option<String>,
    },
    /// Row passed with one or more validation warnings (continue processing)
    Warning {
        /// List of validation warnings that occurred
        warnings: Vec<ValidationWarning>,
    },
}

/// Outcome of running the pipeline over a whole batch.
pub struct BatchOutput {
    pub successful: Vec<Record>,
    pub filtered: Vec<Record>,
    pub failed: Vec<(Record, TransformError)>,
}

/// Details about a validation warning
#[derive(Debug, Clone, PartialEq)]
pub struct ValidationWarning {
    /// Validation rule that triggered the warning
    pub rule: String,
    /// Warning message
    pub message: String,
}

/// How a row removed during [`TransformPipeline::run_batch`] is routed.
enum Removal {
    Filter,
    Fail(TransformError),
}

pub trait Transform: Send + Sync {
    /// Apply to a single row in place.
    fn apply(&self, row: &mut Record) -> Result<(), TransformError>;

    /// Apply to a whole batch.
    fn apply_batch(&self, rows: &mut [Record], failures: &mut Vec<(usize, TransformError)>) {
        for (i, row) in rows.iter_mut().enumerate() {
            if let Err(e) = self.apply(row) {
                failures.push((i, e));
            }
        }
    }
}

/// Trait for filter-like transforms that decide whether to keep a row.
pub trait Filter: Send + Sync {
    fn should_keep(&self, row: &Record) -> bool;
}

pub trait Validator: Send + Sync {
    fn validate(&self, row: &Record) -> Result<ValidationResult, TransformError>;
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
    pub fn apply(&self, row: &mut Record) -> Result<ApplyOutcome, TransformError> {
        let mut warnings = Vec::new();

        for stage in &self.stages {
            match stage {
                PipelineStage::Transform(transform) => {
                    transform.apply(row)?;
                }
                PipelineStage::Filter(filter) => {
                    if !filter.should_keep(row) {
                        return Ok(ApplyOutcome::Skipped { reason: None });
                    }
                }
                PipelineStage::Validation(validator) => {
                    if let Some(outcome) = self.validate(row, validator, &mut warnings)? {
                        return Ok(outcome);
                    }
                }
            }
        }

        if warnings.is_empty() {
            Ok(ApplyOutcome::Success)
        } else {
            Ok(ApplyOutcome::Warning { warnings })
        }
    }

    /// Run the whole pipeline over a batch, **stage by stage**.
    pub fn run_batch(&self, mut rows: Vec<Record>) -> BatchOutput {
        let mut filtered = Vec::new();
        let mut failed: Vec<(Record, TransformError)> = Vec::new();

        let mut warn_scratch = Vec::new();
        let mut scratch: Vec<Record> = Vec::new();

        let mut failures: Vec<(usize, TransformError)> = Vec::new();
        let mut removals: Vec<(usize, Removal)> = Vec::new();

        for stage in &self.stages {
            if rows.is_empty() {
                break;
            }

            match stage {
                PipelineStage::Transform(transform) => {
                    failures.clear();
                    transform.apply_batch(&mut rows, &mut failures);

                    if !failures.is_empty() {
                        removals.clear();

                        for (idx, err) in failures.drain(..) {
                            removals.push((idx, Removal::Fail(err)));
                        }

                        drain_removals(
                            &mut rows,
                            &mut scratch,
                            &mut removals,
                            &mut filtered,
                            &mut failed,
                        );
                    }
                }
                PipelineStage::Filter(filter) => {
                    // One O(N) pass: retained rows keep their order, rejected rows move straight into `filtered`.
                    filtered.extend(rows.extract_if(.., |row| !filter.should_keep(row)));
                }
                PipelineStage::Validation(validator) => {
                    // Validate in place, recording removals by index.
                    removals.clear();

                    for (i, row) in rows.iter_mut().enumerate() {
                        match self.validate(row, validator, &mut warn_scratch) {
                            // Passed or warned (non-fatal): keep the row.
                            Ok(None) => {}
                            // Skip action: route to filtered.
                            Ok(Some(_)) => removals.push((i, Removal::Filter)),
                            // Fail action: route to failed (fatal for the run).
                            Err(e) => removals.push((i, Removal::Fail(e))),
                        }
                        warn_scratch.clear();
                    }

                    if !removals.is_empty() {
                        drain_removals(
                            &mut rows,
                            &mut scratch,
                            &mut removals,
                            &mut filtered,
                            &mut failed,
                        );
                    }
                }
            }
        }

        BatchOutput {
            successful: rows,
            filtered,
            failed,
        }
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

    fn validate(
        &self,
        row: &mut Record,
        validator: &Arc<dyn Validator>,
        warnings: &mut Vec<ValidationWarning>,
    ) -> Result<Option<ApplyOutcome>, TransformError> {
        let res = validator.validate(row)?;

        if let ValidationResult::Failed {
            rule,
            message,
            action,
        } = res
        {
            match action {
                ValidationAction::Skip => {
                    warn!(rule = %rule, message = %message, "validation failed, skipping row");
                    return Ok(Some(ApplyOutcome::Skipped {
                        reason: Some(format!("Validation '{}' failed: {}", rule, message)),
                    }));
                }
                ValidationAction::Fail => {
                    return Err(TransformError::ValidationFailed { rule, message });
                }
                ValidationAction::Warn => {
                    warn!(rule = %rule, message = %message, "validation failed, continuing");
                    // `res` was destructured by value, so move the strings in
                    // rather than cloning.
                    warnings.push(ValidationWarning { rule, message });
                }
            }
        }

        Ok(None)
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

/// Move the rows at `removals`' (ascending) indices out of `rows` into their
/// bucket, keeping the rest in their original order.
fn drain_removals(
    rows: &mut Vec<Record>,
    scratch: &mut Vec<Record>,
    removals: &mut Vec<(usize, Removal)>,
    filtered: &mut Vec<Record>,
    failed: &mut Vec<(Record, TransformError)>,
) {
    std::mem::swap(rows, scratch);
    let mut rem = removals.drain(..).peekable();

    for (i, row) in scratch.drain(..).enumerate() {
        match rem.peek() {
            Some(&(idx, _)) if idx == i => match rem.next().unwrap().1 {
                Removal::Filter => filtered.push(row),
                Removal::Fail(e) => failed.push((row, e)),
            },
            _ => rows.push(row),
        }
    }
}
