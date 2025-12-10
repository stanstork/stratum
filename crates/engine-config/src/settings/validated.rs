use crate::settings::CopyColumns;

/// Immutable, validated configuration used throughout the migration.
#[derive(Debug, Clone)]
pub struct ValidatedSettings {
    /// Batch size for reading and writing data
    pub batch_size: usize,
    /// Which columns to copy from source to destination
    pub copy_columns: CopyColumns,
    /// Whether to infer the entire schema from source
    pub infer_schema: bool,
    /// Whether to create missing tables at destination
    pub create_missing_tables: bool,
    /// Whether to create missing columns at destination
    pub create_missing_columns: bool,
    /// Whether to ignore constraints during migration
    pub ignore_constraints: bool,
    /// Whether this is a dry run (no changes applied)
    pub dry_run: bool,
}

impl ValidatedSettings {
    pub fn default(dry_run: bool) -> Self {
        Self {
            batch_size: 1000,
            copy_columns: CopyColumns::All,
            infer_schema: false,
            create_missing_tables: false,
            create_missing_columns: false,
            ignore_constraints: false,
            dry_run,
        }
    }

    pub fn from_builder(builder: ValidatedSettingsBuilder) -> Self {
        Self {
            batch_size: builder.batch_size.unwrap_or(1000),
            copy_columns: builder.copy_columns.unwrap_or(CopyColumns::All),
            infer_schema: builder.infer_schema.unwrap_or(false),
            create_missing_tables: builder.create_missing_tables.unwrap_or(false),
            create_missing_columns: builder.create_missing_columns.unwrap_or(false),
            ignore_constraints: builder.ignore_constraints.unwrap_or(false),
            dry_run: builder.dry_run,
        }
    }

    pub fn batch_size(&self) -> usize {
        self.batch_size
    }

    pub fn copy_columns(&self) -> &CopyColumns {
        &self.copy_columns
    }

    pub fn infer_schema(&self) -> bool {
        self.infer_schema
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

    pub fn is_dry_run(&self) -> bool {
        self.dry_run
    }

    pub fn requires_schema_op(&self) -> bool {
        self.infer_schema || self.create_missing_tables || self.create_missing_columns
    }

    pub fn mapped_columns_only(&self) -> bool {
        matches!(self.copy_columns, CopyColumns::MapOnly)
    }
}

#[derive(Debug, Default)]
pub struct ValidatedSettingsBuilder {
    pub batch_size: Option<usize>,
    pub copy_columns: Option<CopyColumns>,
    pub infer_schema: Option<bool>,
    pub create_missing_tables: Option<bool>,
    pub create_missing_columns: Option<bool>,
    pub ignore_constraints: Option<bool>,
    pub dry_run: bool,
}

impl ValidatedSettingsBuilder {
    pub fn new(dry_run: bool) -> Self {
        Self {
            dry_run,
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

    pub fn infer_schema(mut self, infer_schema: bool) -> Self {
        self.infer_schema = Some(infer_schema);
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
        let settings = ValidatedSettingsBuilder::new(true)
            .batch_size(500)
            .infer_schema(true)
            .create_missing_tables(true)
            .build();

        assert_eq!(settings.batch_size(), 500);
        assert!(settings.is_dry_run());
        assert!(settings.infer_schema());
        assert!(settings.requires_schema_op());
    }
}
