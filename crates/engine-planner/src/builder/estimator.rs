use crate::plan::{
    diagnostics::calibration::CalibrationData,
    estimation::{
        duration::DurationEstimate, pipeline::PipelineEstimations, resource::ResourceEstimations,
    },
    execution::{
        execution_settings::{ExecutionSettings, ExecutionStrategy},
        execution_stage::ExecutionStage,
    },
    pipeline::{
        destination::{DestinationPlan, WriteMode},
        plan::PipelinePlan,
        settings::{CheckpointStrategy, PipelineSettings},
        source::SourcePlan,
    },
    transform::{
        join::JoinPlan,
        mapping::{ColumnMapping, MappingType},
    },
};

mod throughput {
    pub const BASE_COPY: u64 = 40_000;
    pub const BASE_INSERT: u64 = 10_000;
    pub const MIN_SAFE: u64 = 100;

    pub fn join_factor(count: usize) -> f64 {
        match count {
            0 => 1.0, // No joins: full speed
            1 => 0.9, // 1 join: 10% slower
            2 => 0.8, // 2 joins: 20% slower
            3 => 0.7, // 3 joins: 30% slower
            _ => 0.5, // 4+ joins: 50% slower
        }
    }

    pub fn expr_factor(count: usize) -> f64 {
        match count {
            0..=2 => 1.0,    // 0-2 expressions: no penalty
            3..=5 => 0.95,   // 3-5 expressions: 5% slower
            6..=10 => 0.90,  // 6-10 expressions: 10% slower
            11..=20 => 0.80, // 11-20 expressions: 20% slower
            _ => 0.70,       // 20+ expressions: 30% slower
        }
    }

    pub fn batch_factor(size: usize) -> f64 {
        match size {
            0..=100 => 0.5,
            101..=500 => 0.8,
            501..=1000 => 0.9,
            1001..=5000 => 0.98,
            5001..=10000 => 1.0,    // Optimal range
            10001..=50000 => 1.10,  // Staging optimization benefit
            50001..=100000 => 1.15, // Peak efficiency for COPY
            _ => 1.10,              // Diminishing returns (memory overhead)
        }
    }
}

mod overhead {
    use crate::plan::pipeline::destination::WriteMode;

    pub const CONNECTION_SETUP: u64 = 1;
    pub const CHECKPOINT_PERIODIC: u64 = 1;
    pub const INDEX_REBUILD_LARGE: u64 = 10;

    pub const THRESHOLD_MEDIUM: u64 = 1_000_000;
    pub const THRESHOLD_LARGE: u64 = 10_000_000;

    pub fn write_mode_overhead(mode: &WriteMode, current_rows: u64) -> u64 {
        match mode {
            WriteMode::Replace => {
                if current_rows > THRESHOLD_MEDIUM {
                    2
                } else {
                    1
                }
            }
            WriteMode::Append => 1,
            WriteMode::Upsert => 2,
            WriteMode::Merge => 3,
        }
    }
}

/// Estimates pipeline execution duration based on complexity factors
pub struct DurationEstimator {
    /// Base throughput (rows/sec) for simple pipelines
    /// This is a conservative baseline that gets adjusted by complexity factors
    baseline_throughput: u64,

    /// Calibration data from previous runs
    calibration: Option<CalibrationData>,
}

impl DurationEstimator {
    pub fn new(is_fast_path: bool) -> Self {
        Self {
            baseline_throughput: if is_fast_path {
                throughput::BASE_COPY
            } else {
                throughput::BASE_INSERT
            },
            calibration: None,
        }
    }

    /// Estimates the full pipeline impact including throughput, duration, batches, and memory.
    pub fn estimate_pipeline(
        &self,
        source: &SourcePlan,
        destination: &DestinationPlan,
        mappings: &[ColumnMapping],
        joins: &[JoinPlan],
        settings: &PipelineSettings,
        is_fast_path: bool,
    ) -> PipelineEstimations {
        let tps = self.calculate_throughput(mappings, joins, settings, is_fast_path);

        // Base duration: rows / rows-per-second
        let rows = source.total_rows.value;
        let base_seconds = rows.checked_div(tps).unwrap_or(0);
        let overhead_seconds = self.calculate_total_overhead(settings, destination);

        let batch_size = if settings.batch_size == 0 {
            1000 // Default batch size
        } else {
            settings.batch_size as u64
        };
        let batches = (rows as f64 / batch_size as f64).ceil() as u64;

        PipelineEstimations {
            duration: DurationEstimate::from_seconds(base_seconds + overhead_seconds),
            rows_per_second: tps,
            batches: batches.max(1),
            memory_mb: self.estimate_memory(settings, mappings),
        }
    }

    /// Estimate duration for an execution stage (parallel pipelines)
    pub fn estimate_stage(pipelines: &[&PipelinePlan]) -> DurationEstimate {
        DurationEstimate::max_of(
            &pipelines
                .iter()
                .map(|p| p.estimations.duration.clone())
                .collect::<Vec<_>>(),
        )
    }

    fn calculate_throughput(
        &self,
        mappings: &[ColumnMapping],
        joins: &[JoinPlan],
        settings: &PipelineSettings,
        _is_fast_path: bool,
    ) -> u64 {
        // Base throughput already accounts for fast path (BASE_COPY) vs regular (BASE_INSERT)
        let mut tps = self
            .calibration
            .as_ref()
            .map(|c| c.avg_throughput as f64)
            .unwrap_or(self.baseline_throughput as f64);

        let computed_cols = mappings
            .iter()
            .filter(|m| {
                matches!(
                    m.mapping_type,
                    MappingType::Computed | MappingType::Conditional
                )
            })
            .count();

        let conversions = mappings
            .iter()
            .filter(|m| m.type_conversion.is_some())
            .count();

        // Apply scaling factors
        tps *= throughput::join_factor(joins.len());
        tps *= throughput::expr_factor(computed_cols);
        tps *= throughput::batch_factor(settings.batch_size);

        // Type conversion penalty: 0-5 = 1.0, 6-15 = 0.98, 15+ = 0.95
        tps *= match conversions {
            0..=5 => 1.0,
            6..=15 => 0.98,
            _ => 0.95,
        };

        // Worker scaling: square root indicates diminishing returns of parallelism
        tps *= (settings.workers as f64).sqrt();

        (tps as u64).max(throughput::MIN_SAFE)
    }

    fn calculate_total_overhead(
        &self,
        settings: &PipelineSettings,
        destination: &DestinationPlan,
    ) -> u64 {
        let mut total = overhead::CONNECTION_SETUP;

        // Final write operation overhead
        total += overhead::write_mode_overhead(&destination.mode, destination.current_rows.value);

        // Checkpoint logic
        total += match &settings.checkpoint {
            CheckpointStrategy::Never => 0,
            CheckpointStrategy::EveryBatch => 0, // In-memory/Async
            _ => overhead::CHECKPOINT_PERIODIC,
        };

        // Large table maintenance (index rebuilds for merge/upsert)
        if destination.current_rows.value > overhead::THRESHOLD_LARGE
            && matches!(destination.mode, WriteMode::Upsert | WriteMode::Merge)
        {
            total += overhead::INDEX_REBUILD_LARGE;
        }

        total
    }

    fn estimate_memory(&self, settings: &PipelineSettings, mappings: &[ColumnMapping]) -> u64 {
        const BYTES_PER_COL: u64 = 80;
        const FRAMEWORK_OVERHEAD: u64 = 32;
        const EXPR_MB_LIMIT: u64 = 128;

        let row_size = mappings.len() as u64 * BYTES_PER_COL;
        let batch_mb = (settings.batch_size as u64 * row_size) / (1024 * 1024);
        let pipeline_memory = batch_mb * settings.workers as u64;

        let computed_count = mappings
            .iter()
            .filter(|m| {
                matches!(
                    m.mapping_type,
                    MappingType::Computed | MappingType::Conditional
                )
            })
            .count();

        let expr_memory = (computed_count as u64 * 5).min(EXPR_MB_LIMIT);

        pipeline_memory + expr_memory + FRAMEWORK_OVERHEAD
    }
}

/// Estimates total resource requirements for an execution plan
pub struct ResourceEstimator;

impl ResourceEstimator {
    /// Estimate total resources for the entire plan
    pub fn estimate(
        pipelines: &[PipelinePlan],
        execution_order: &[ExecutionStage],
        settings: &ExecutionSettings,
    ) -> ResourceEstimations {
        ResourceEstimations {
            duration: Self::total_duration(pipelines, execution_order, settings),
            peak_memory_mb: Self::peak_memory(pipelines, execution_order, settings),
            network_transfer_mb: Self::total_network(pipelines),
            disk_usage_mb: Self::total_disk(pipelines),
            total_batches: pipelines.iter().map(|p| p.estimations.batches).sum(),
        }
    }

    fn total_duration(
        pipelines: &[PipelinePlan],
        execution_order: &[ExecutionStage],
        settings: &ExecutionSettings,
    ) -> DurationEstimate {
        match settings.strategy {
            ExecutionStrategy::Sequential => {
                let durations: Vec<_> = pipelines
                    .iter()
                    .map(|p| p.estimations.duration.clone())
                    .collect();
                DurationEstimate::combine(&durations)
            }
            ExecutionStrategy::Parallel => {
                let stage_durations: Vec<_> = execution_order
                    .iter()
                    .map(|s| s.estimated_duration.clone())
                    .collect();
                DurationEstimate::combine(&stage_durations)
            }
        }
    }

    fn peak_memory(
        pipelines: &[PipelinePlan],
        execution_order: &[ExecutionStage],
        settings: &ExecutionSettings,
    ) -> u64 {
        match settings.strategy {
            ExecutionStrategy::Sequential => pipelines
                .iter()
                .map(|p| p.estimations.memory_mb)
                .max()
                .unwrap_or(0),
            ExecutionStrategy::Parallel => execution_order
                .iter()
                .map(|stage| {
                    pipelines
                        .iter()
                        .filter(|p| stage.pipelines.contains(&p.name))
                        .map(|p| p.estimations.memory_mb)
                        .sum::<u64>()
                })
                .max()
                .unwrap_or(0),
        }
    }

    fn total_network(pipelines: &[PipelinePlan]) -> f64 {
        pipelines
            .iter()
            .map(|p| {
                let row_bytes =
                    p.mappings.len() as f64 * 100.0 * p.source.effective_row_count().value as f64;
                (row_bytes * 2.0) / (1024.0 * 1024.0) // Read + Write, convert to MB
            })
            .sum()
    }

    fn total_disk(pipelines: &[PipelinePlan]) -> u64 {
        pipelines
            .iter()
            .map(|p| {
                let checkpoint_size = match p.settings.checkpoint {
                    CheckpointStrategy::Never => 0,
                    CheckpointStrategy::EveryBatch => p.estimations.batches,
                    CheckpointStrategy::EveryN { n } => p.estimations.batches / (n as u64).max(1),
                    _ => p.estimations.duration.likely_seconds / 10,
                };
                (checkpoint_size / 1024) + 10 // + base failure log estimate
            })
            .sum()
    }
}
