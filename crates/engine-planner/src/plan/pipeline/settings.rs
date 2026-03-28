use engine_config::settings::{CopyColumns, validated::ValidatedSettings};
use model::execution::flags::IntegrityMode;
use serde::Serialize;

// Helper functions for skip_serializing_if
fn is_false(b: &bool) -> bool {
    !b
}

#[derive(Serialize, Debug, Clone)]
pub struct PipelineSettings {
    pub batch_size: usize,
    pub copy_columns: CopyColumns,

    #[serde(skip_serializing_if = "is_false")]
    pub infer_schema: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub create_missing_tables: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub create_missing_columns: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub ignore_constraints: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub dry_run: bool,

    pub workers: usize,
    pub checkpoint: CheckpointStrategy,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub memory_limit_mb: Option<usize>,
}

impl PipelineSettings {
    pub fn from_validated(settings: ValidatedSettings) -> Self {
        Self {
            batch_size: settings.batch_size,
            copy_columns: settings.copy_columns,
            infer_schema: settings.infer_schema,
            create_missing_tables: settings.create_missing_tables,
            create_missing_columns: settings.create_missing_columns,
            ignore_constraints: settings.ignore_constraints,
            dry_run: settings.dry_run,
            workers: 1,
            checkpoint: CheckpointStrategy::EveryBatch,
            timeout: None,
            memory_limit_mb: None,
        }
    }

    pub fn as_validated(&self) -> ValidatedSettings {
        ValidatedSettings {
            batch_size: self.batch_size,
            copy_columns: self.copy_columns,
            infer_schema: self.infer_schema,
            create_missing_tables: self.create_missing_tables,
            create_missing_columns: self.create_missing_columns,
            ignore_constraints: self.ignore_constraints,
            dry_run: self.dry_run,
            integrity: IntegrityMode::Off,
        }
    }
}

impl Default for PipelineSettings {
    fn default() -> Self {
        Self::from_validated(ValidatedSettings::default(true))
    }
}

#[derive(Serialize, Debug, Clone, PartialEq, Default)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointStrategy {
    Never,
    #[default]
    EveryBatch,
    EveryN {
        n: usize,
    },
    EverySeconds {
        seconds: usize,
    },
}
