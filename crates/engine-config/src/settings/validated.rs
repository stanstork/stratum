use std::collections::HashMap;

use crate::settings::CopyColumns;
use connectors::drivers::postgres::config::PkCreation;
use model::{core::value::Value, execution::flags::IntegrityMode};
use serde::Serialize;

/// Immutable, validated configuration used throughout the migration.
#[derive(Serialize, Debug, Clone)]
pub struct ValidatedSettings {
    /// Batch size for reading and writing data
    pub batch_size: usize,
    /// Which columns to copy from source to destination
    pub copy_columns: CopyColumns,
    /// Whether to create missing tables at destination
    pub create_missing_tables: bool,
    /// Whether to create missing columns at destination
    pub create_missing_columns: bool,
    /// Whether to ignore constraints during migration
    pub ignore_constraints: bool,
    /// Parallel range lanes for a single-table snapshot copy (>= 1).
    pub lanes: usize,
    /// When the destination primary key is created relative to the bulk load.
    pub pk_creation: PkCreation,
    /// Whether this is a dry run (no changes applied)
    pub dry_run: bool,
    /// Integrity hashing mode for this migration run.
    pub integrity: IntegrityMode,
}

impl ValidatedSettings {
    pub fn default(dry_run: bool) -> Self {
        Self {
            batch_size: 1000,
            copy_columns: CopyColumns::All,
            create_missing_tables: false,
            create_missing_columns: false,
            ignore_constraints: false,
            lanes: 1,
            pk_creation: PkCreation::Pre,
            dry_run,
            integrity: IntegrityMode::Off,
        }
    }

    pub fn from_pipeline(
        settings: &HashMap<String, Value>,
        dry_run: bool,
        integrity: IntegrityMode,
    ) -> Self {
        let mut s = Self::default(dry_run);
        s.integrity = integrity;
        if let Some(n) = read_usize(settings, "batch_size") {
            s.batch_size = n;
        }
        if let Some(n) = read_usize(settings, "lanes") {
            s.lanes = n.clamp(1, 32);
        }
        s
    }

    pub fn from_builder(builder: ValidatedSettingsBuilder) -> Self {
        Self {
            batch_size: builder.batch_size.unwrap_or(1000),
            copy_columns: builder.copy_columns.unwrap_or(CopyColumns::All),
            create_missing_tables: builder.create_missing_tables.unwrap_or(false),
            create_missing_columns: builder.create_missing_columns.unwrap_or(false),
            ignore_constraints: builder.ignore_constraints.unwrap_or(false),
            lanes: builder.lanes.unwrap_or(1),
            pk_creation: builder.pk_creation.unwrap_or_default(),
            dry_run: builder.dry_run,
            integrity: builder.integrity,
        }
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn copy_columns(&self) -> &CopyColumns {
        &self.copy_columns
    }

    pub fn create_missing_tables(&self) -> bool {
        self.create_missing_tables
    }

    pub fn create_missing_columns(&self) -> bool {
        self.create_missing_columns
    }

    pub fn ignore_constraints(&self) -> bool {
        self.ignore_constraints
    }

    pub fn lanes(&self) -> usize {
        self.lanes
    }

    pub fn pk_creation(&self) -> PkCreation {
        self.pk_creation
    }

    pub fn is_dry_run(&self) -> bool {
        self.dry_run
    }

    pub fn requires_schema_op(&self) -> bool {
        self.create_missing_tables || self.create_missing_columns
    }

    pub fn mapped_columns_only(&self) -> bool {
        matches!(self.copy_columns, CopyColumns::MapOnly)
    }

    pub fn integrity(&self) -> IntegrityMode {
        self.integrity
    }
}

/// Read a settings value as a positive integer, accepting either `UInt` or a
/// positive `Int` (SMQL literals can land as either). Returns `None` otherwise.
fn read_usize(settings: &HashMap<String, Value>, key: &str) -> Option<usize> {
    match settings.get(key) {
        Some(Value::UInt(n)) => Some(*n as usize),
        Some(Value::Int(n)) if *n > 0 => Some(*n as usize),
        _ => None,
    }
}

#[derive(Debug, Default)]
pub struct ValidatedSettingsBuilder {
    pub batch_size: Option<usize>,
    pub copy_columns: Option<CopyColumns>,
    pub create_missing_tables: Option<bool>,
    pub create_missing_columns: Option<bool>,
    pub ignore_constraints: Option<bool>,
    pub lanes: Option<usize>,
    pub pk_creation: Option<PkCreation>,
    pub dry_run: bool,
    pub integrity: IntegrityMode,
}

impl ValidatedSettingsBuilder {
    pub fn new(dry_run: bool, integrity: IntegrityMode) -> Self {
        Self {
            dry_run,
            integrity,
            ..Default::default()
        }
    }

    pub fn batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = Some(batch_size);
        self
    }

    pub fn copy_columns(mut self, copy_columns: CopyColumns) -> Self {
        self.copy_columns = Some(copy_columns);
        self
    }

    pub fn create_missing_tables(mut self, create_missing_tables: bool) -> Self {
        self.create_missing_tables = Some(create_missing_tables);
        self
    }

    pub fn create_missing_columns(mut self, create_missing_columns: bool) -> Self {
        self.create_missing_columns = Some(create_missing_columns);
        self
    }

    pub fn ignore_constraints(mut self, ignore_constraints: bool) -> Self {
        self.ignore_constraints = Some(ignore_constraints);
        self
    }

    pub fn lanes(mut self, lanes: usize) -> Self {
        self.lanes = Some(lanes);
        self
    }

    pub fn build(self) -> ValidatedSettings {
        ValidatedSettings::from_builder(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_settings() {
        let settings = ValidatedSettings::default(false);
        assert_eq!(settings.batch_size(), 1000);
        assert!(!settings.is_dry_run());
        assert!(!settings.requires_schema_op());
    }

    #[test]
    fn test_builder() {
        let settings = ValidatedSettingsBuilder::new(true, IntegrityMode::BatchHashes)
            .batch_size(500)
            .create_missing_tables(true)
            .build();

        assert_eq!(settings.batch_size(), 500);
        assert!(settings.is_dry_run());
        assert!(settings.requires_schema_op());
    }
}
