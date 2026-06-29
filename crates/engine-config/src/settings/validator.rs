use crate::settings::{
    Settings,
    error::SettingsError,
    validated::{ValidatedSettings, ValidatedSettingsBuilder},
};
use connectors::traits::introspector::SchemaIntrospector;
use engine_processing::io::{destination::Destination, format::DataFormat, source::Source};
use model::execution::flags::IntegrityMode;
use tracing::{debug, warn};

/// Validates migration settings before they are applied.
pub struct SettingsValidator<'a> {
    source: &'a Source,
    destination: &'a Destination,
    introspector: &'a dyn SchemaIntrospector,
    dry_run: bool,
    integrity: IntegrityMode,
}

impl<'a> SettingsValidator<'a> {
    pub fn new(
        source: &'a Source,
        destination: &'a Destination,
        introspector: &'a dyn SchemaIntrospector,
        dry_run: bool,
        integrity: IntegrityMode,
    ) -> Self {
        Self {
            source,
            destination,
            introspector,
            dry_run,
            integrity,
        }
    }

    pub async fn validate(&self, settings: &Settings) -> Result<ValidatedSettings, SettingsError> {
        debug!("validating settings: {settings:#?}");

        let mut builder = ValidatedSettingsBuilder::new(self.dry_run, self.integrity);
        let mut errors: Vec<String> = Vec::new();

        self.validate_batch_size(settings, &mut builder);
        self.validate_copy_columns(settings, &mut builder);
        self.validate_ignore_constraints(settings, &mut builder, &mut errors);
        self.validate_infer_schema(settings, &mut builder, &mut errors)
            .await?;
        self.validate_create_tables(settings, &mut builder, &mut errors)
            .await?;
        self.validate_create_columns(settings, &mut builder, &mut errors)
            .await?;

        if !errors.is_empty() {
            return Err(SettingsError::ValidationFailed(errors));
        }

        self.check_conflicts(&builder)?;

        let validated = builder.build();
        debug!("settings validation completed");
        self.log_validated_settings(&validated);

        Ok(validated)
    }

    fn validate_batch_size(&self, settings: &Settings, builder: &mut ValidatedSettingsBuilder) {
        if settings.batch_size > 0 {
            if settings.batch_size > 100_000 {
                warn!(
                    batch_size = settings.batch_size,
                    "batch size is very large, may cause memory issues"
                );
            }
            builder.batch_size = Some(settings.batch_size);
        }
    }

    fn validate_copy_columns(&self, settings: &Settings, builder: &mut ValidatedSettingsBuilder) {
        builder.copy_columns = Some(settings.copy_columns);
    }

    fn validate_ignore_constraints(
        &self,
        settings: &Settings,
        builder: &mut ValidatedSettingsBuilder,
        errors: &mut Vec<String>,
    ) {
        if settings.ignore_constraints {
            if !self.is_sql_destination() {
                errors
                    .push("ignore_constraints is only supported for SQL destinations".to_string());
                return;
            }
            builder.ignore_constraints = Some(true);
        }
    }

    async fn validate_infer_schema(
        &self,
        settings: &Settings,
        builder: &mut ValidatedSettingsBuilder,
        errors: &mut Vec<String>,
    ) -> Result<(), SettingsError> {
        if !settings.infer_schema {
            return Ok(());
        }

        // Check format compatibility
        if !self.is_supported_schema_inference() {
            errors.push(format!(
                "infer_schema is not supported for {} -> {} migration",
                self.source.format, self.destination.format
            ));
            return Ok(());
        }

        // Check if table already exists
        if self.destination_exists().await? {
            warn!("create_missing_tables enabled but destination already exists, will skip");
        }

        builder.infer_schema = Some(true);
        Ok(())
    }

    async fn validate_create_tables(
        &self,
        settings: &Settings,
        builder: &mut ValidatedSettingsBuilder,
        errors: &mut Vec<String>,
    ) -> Result<(), SettingsError> {
        if !settings.create_missing_tables {
            return Ok(());
        }

        if !self.is_sql_destination() {
            errors.push("create_missing_tables is only supported for SQL destinations".to_string());
            return Ok(());
        }

        // Check if table already exists
        if self.destination_exists().await? {
            warn!("create_missing_tables enabled but destination already exists, will skip");
        }

        builder.create_missing_tables = Some(true);
        Ok(())
    }

    async fn validate_create_columns(
        &self,
        settings: &Settings,
        builder: &mut ValidatedSettingsBuilder,
        errors: &mut Vec<String>,
    ) -> Result<(), SettingsError> {
        if !settings.create_missing_columns {
            return Ok(());
        }

        if !self.is_sql_destination() {
            errors
                .push("create_missing_columns is only supported for SQL destinations".to_string());
            return Ok(());
        }

        // Check if destination exists (required for column creation)
        if !self.destination_exists().await? {
            errors.push(
                "create_missing_columns requires destination table to exist (use create_missing_tables first)".to_string(),
            );
            return Ok(());
        }

        builder.create_missing_columns = Some(true);
        Ok(())
    }

    fn check_conflicts(&self, builder: &ValidatedSettingsBuilder) -> Result<(), SettingsError> {
        let mut conflicts = Vec::new();

        // Conflict: infer_schema + create_missing_tables
        if builder.infer_schema.unwrap_or(false) && builder.create_missing_tables.unwrap_or(false) {
            conflicts.push(
                "Cannot use both infer_schema and create_missing_tables (infer_schema includes table creation)"
                    .to_string(),
            );
        }

        // Conflict: infer_schema + create_missing_columns
        if builder.infer_schema.unwrap_or(false) && builder.create_missing_columns.unwrap_or(false)
        {
            warn!(
                "infer_schema and create_missing_columns both enabled; infer_schema takes precedence"
            );
        }

        if !conflicts.is_empty() {
            return Err(SettingsError::ConflictingSettings(conflicts));
        }

        Ok(())
    }

    fn is_sql_destination(&self) -> bool {
        matches!(
            self.destination.format,
            DataFormat::Postgres | DataFormat::MySql
        )
    }

    fn is_supported_schema_inference(&self) -> bool {
        matches!(
            (self.source.format, self.destination.format),
            (DataFormat::MySql, DataFormat::Postgres)
        )
    }

    async fn destination_exists(&self) -> Result<bool, SettingsError> {
        let table = &self.destination.name;
        let exists = self.introspector.table_exists(table).await?;
        Ok(exists)
    }

    fn log_validated_settings(&self, settings: &ValidatedSettings) {
        debug!(
            batch_size = settings.batch_size(),
            copy_columns = ?settings.copy_columns(),
            infer_schema = settings.infer_schema(),
            create_missing_tables = settings.create_missing_tables(),
            create_missing_columns = settings.create_missing_columns(),
            ignore_constraints = settings.ignore_constraints(),
            dry_run = settings.is_dry_run(),
            "validated settings"
        );
    }
}
