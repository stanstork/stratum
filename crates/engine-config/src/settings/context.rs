use super::error::SettingsError;
use crate::{
    report::dry_run::DryRunReport,
    settings::{
        schema_manager::{LiveSchemaManager, SchemaManager, ValidationSchemaManager},
        validated::ValidatedSettings,
    },
};
use connectors::{
    metadata::field::FieldMetadata,
    sql::base::{
        adapter::SqlAdapter,
        metadata::{column::ColumnMetadata, table::TableMetadata},
    },
};
use engine_core::{
    connectors::{
        destination::{DataDestination, Destination},
        source::{DataSource, Source},
    },
    context::item::ItemContext,
    schema::{plan::SchemaPlan, types::TypeEngine},
};
use futures::lock::Mutex;
use model::{core::data_type::DataType, transform::mapping::TransformationMetadata};
use crate::settings::CopyColumns;
use std::sync::Arc;

pub struct SchemaSettingContext {
    pub source: Source,
    pub destination: Destination,
    pub mapping: TransformationMetadata,
    pub settings: ValidatedSettings,
    pub dry_run_report: Arc<Mutex<DryRunReport>>,
    pub schema_manager: Box<dyn SchemaManager>,
}

impl SchemaSettingContext {
    pub async fn new(
        src: &Source,
        dest: &Destination,
        mapping: &TransformationMetadata,
        settings: &ValidatedSettings,
        dry_run_report: &Arc<Mutex<DryRunReport>>,
    ) -> Self {
        let is_dry_run = settings.is_dry_run();
        let schema_manager: Box<dyn SchemaManager + Send> = if is_dry_run {
            Box::new(ValidationSchemaManager {
                report: dry_run_report.clone(),
                settings: settings.clone(),
            })
        } else {
            // Create live manager for real migration
            Box::new(LiveSchemaManager {
                destination: Arc::new(Mutex::new(dest.clone())),
            })
        };

        Self {
            source: src.clone(),
            destination: dest.clone(),
            mapping: mapping.clone(),
            settings: settings.clone(),
            dry_run_report: dry_run_report.clone(),
            schema_manager,
        }
    }

    pub async fn source_adapter(&self) -> Result<Arc<dyn SqlAdapter + Send + Sync>, SettingsError> {
        match &self.source.primary {
            DataSource::Database(src) => Ok(src.lock().await.adapter()),
            DataSource::File(_) => Err(SettingsError::UnsupportedSource(
                self.source.format.to_string(),
            )),
        }
    }

    pub async fn destination_adapter(
        &self,
    ) -> Result<Arc<dyn SqlAdapter + Send + Sync>, SettingsError> {
        match &self.destination.data_dest {
            DataDestination::Database(dest) => Ok(dest.data.lock().await.adapter()),
        }
    }

    pub async fn destination_exists(&self) -> Result<bool, SettingsError> {
        match &self.destination.data_dest {
            DataDestination::Database(dest) => Ok(dest
                .data
                .lock()
                .await
                .table_exists(&self.destination.name)
                .await?),
        }
    }

    pub async fn apply_to_destination(
        &mut self,
        schema_plan: SchemaPlan,
    ) -> Result<(), SettingsError> {
        self.schema_manager.infer_schema(&schema_plan).await?;
        return Ok(());
    }

    pub async fn build_schema_plan(&self) -> Result<SchemaPlan, SettingsError> {
        let ignore_constraints = self.settings.ignore_constraints();
        let mapped_columns_only = *self.settings.copy_columns() == CopyColumns::MapOnly;
        let source = self.source.primary.clone();

        let type_engine = TypeEngine::new(
            source.clone(),
            // converter
            Box::new(|meta: &FieldMetadata| -> (DataType, Option<usize>) { meta.pg_type() }),
            // extractor
            Box::new(|meta: &TableMetadata| -> Vec<ColumnMetadata> { TableMetadata::enums(meta) }),
        );

        Ok(SchemaPlan::new(
            source,
            type_engine,
            ignore_constraints,
            mapped_columns_only,
            self.mapping.clone(),
        ))
    }
}
