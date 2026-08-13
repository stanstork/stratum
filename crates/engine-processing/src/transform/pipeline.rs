use crate::{
    profile,
    transform::{
        error::TransformError,
        validation::{Action, ValidationResult},
    },
};
use model::records::Record;
use std::{sync::Arc, time::Instant};
use tracing::warn;

/// Split `rows` into maximal contiguous runs that share one schema `Arc` (i.e.
/// one table) and call `f(offset, run)` for each.
pub(crate) fn for_each_table(rows: &mut [Record], mut f: impl FnMut(usize, &mut [Record])) {
    let mut offset = 0;

    while offset < rows.len() {
        let current_schema = rows[offset].schema();

        // Find the first row that has a different schema, or the end of the slice
        let len = rows[offset..]
            .iter()
            .position(|r| !Arc::ptr_eq(current_schema, r.schema()))
            .unwrap_or(rows.len() - offset);

        f(offset, &mut rows[offset..offset + len]);
        offset += len;
    }
}

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

    /// Stable label for profiling (which stage this is). Overridden per impl.
    fn kind(&self) -> &'static str {
        "transform"
    }

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

    /// Validate a whole batch at once, pushing one result per row.
    fn validate_batch(
        &self,
        rows: &[Record],
        out: &mut Vec<Result<ValidationResult, TransformError>>,
    ) {
        for row in rows {
            out.push(self.validate(row));
        }
    }
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
        let mut failed = Vec::new();

        let mut scratch = Vec::new();
        let mut val_results = Vec::new();

        let mut failures = Vec::new();
        let mut removals = Vec::new();

        for stage in &self.stages {
            if rows.is_empty() {
                break;
            }

            match stage {
                PipelineStage::Transform(transform) => {
                    failures.clear();

                    let t = Instant::now();
                    transform.apply_batch(&mut rows, &mut failures);
                    profile::record_stage(transform.kind(), t.elapsed());

                    if !failures.is_empty() {
                        removals.clear();
                        removals.extend(
                            failures
                                .drain(..)
                                .map(|(idx, err)| (idx, Removal::Fail(err))),
                        );

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
                    // Validate the whole batch at once, recording removals by index.
                    removals.clear();
                    val_results.clear();

                    let t = Instant::now();
                    validator.validate_batch(&rows, &mut val_results);

                    for (i, res) in val_results.drain(..).enumerate() {
                        match res {
                            // Passed: keep the row.
                            Ok(ValidationResult::Pass) => {}
                            Ok(ValidationResult::Failed {
                                rule,
                                message,
                                action,
                            }) => match action {
                                // Skip action: route to filtered.
                                Action::Skip => removals.push((i, Removal::Filter)),
                                // Fail action: route to failed (fatal for the run).
                                Action::Fail => removals.push((
                                    i,
                                    Removal::Fail(TransformError::ValidationFailed {
                                        rule,
                                        message,
                                    }),
                                )),
                                // Warn action: non-fatal, keep the row.
                                Action::Warn => {}
                            },
                            // Evaluation error (bad expr, plugin crossing failure).
                            Err(e) => removals.push((i, Removal::Fail(e))),
                        }
                    }

                    profile::record_stage("validation", t.elapsed());

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
        if let ValidationResult::Failed {
            rule,
            message,
            action,
        } = validator.validate(row)?
        {
            match action {
                Action::Skip => {
                    warn!(rule = %rule, message = %message, "validation failed, skipping row");
                    return Ok(Some(ApplyOutcome::Skipped {
                        reason: Some(format!("Validation '{rule}' failed: {message}")),
                    }));
                }
                Action::Fail => {
                    return Err(TransformError::ValidationFailed { rule, message });
                }
                Action::Warn => {
                    warn!(rule = %rule, message = %message, "validation failed, continuing");
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
            Some(&(idx, _)) if idx == i => match rem.next() {
                Some((_, Removal::Filter)) => filtered.push(row),
                Some((_, Removal::Fail(e))) => failed.push((row, e)),
                None => rows.push(row),
            },
            _ => rows.push(row),
        }
    }
}
