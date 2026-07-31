use engine_config::settings::validated::ValidatedSettings;
use model::execution::flags::IntegrityMode;
use serde::Serialize;

// Helper functions for skip_serializing_if
fn is_false(b: &bool) -> bool {
    !b
}

#[derive(Serialize, Debug, Clone)]
pub struct PipelineSettings {
    pub batch_size: usize,

    #[serde(skip_serializing_if = "is_false")]
    pub create_missing_tables: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub create_missing_columns: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub skip_primary_keys: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub skip_foreign_keys: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub skip_indexes: bool,
    #[serde(skip_serializing_if = "is_false")]
    pub dry_run: bool,

    pub lanes: usize,
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
            create_missing_tables: settings.create_missing_tables,
            create_missing_columns: settings.create_missing_columns,
            skip_primary_keys: settings.skip_primary_keys,
            skip_foreign_keys: settings.skip_foreign_keys,
            skip_indexes: settings.skip_indexes,
            lanes: settings.lanes,
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
            create_missing_tables: self.create_missing_tables,
            create_missing_columns: self.create_missing_columns,
            skip_primary_keys: self.skip_primary_keys,
            skip_foreign_keys: self.skip_foreign_keys,
            skip_indexes: self.skip_indexes,
            lanes: self.lanes,
            pk_creation: Default::default(),
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
