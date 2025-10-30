use super::error::SettingsError;
use crate::{
    report::dry_run::DryRunReport,
    settings::schema_manager::{LiveSchemaManager, SchemaManager, ValidationSchemaManager},
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
    migration_state::MigrationState,
    schema::{plan::SchemaPlan, types::TypeEngine},
};
use futures::lock::Mutex;
use model::{core::data_type::DataType, transform::mapping::EntityMapping};
use smql_syntax::ast::setting::CopyColumns;
use std::sync::Arc;

pub struct SchemaSettingContext {
    pub source: Source,
    pub destination: Destination,
    pub mapping: EntityMapping,
    pub state: Arc<Mutex<MigrationState>>,
    pub dry_run_report: Arc<Mutex<Option<DryRunReport>>>,
    pub schema_manager: Box<dyn SchemaManager>,
}

impl SchemaSettingContext {
    pub async fn new(
        src: &Source,
        dest: &Destination,
        mapping: &EntityMapping,
        state: &Arc<Mutex<MigrationState>>,
        dry_run_report: &Arc<Mutex<Option<DryRunReport>>>,
    ) -> Self {
        let is_dry_run = state.lock().await.is_dry_run();
        let schema_manager: Box<dyn SchemaManager + Send> = if is_dry_run {
            // Create validation manager for dry-run mode
            let report = dry_run_report
                .lock()
                .await
                .as_ref()
                .cloned()
                .expect("Dry run report should be initialized in dry run mode");

            Box::new(ValidationSchemaManager {
                report: Arc::new(Mutex::new(report)),
                state: state.clone(),
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
            state: state.clone(),
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
            DataDestination::Database(dest) => Ok(dest.lock().await.adapter()),
        }
    }

    pub async fn destination_exists(&self) -> Result<bool, SettingsError> {
        match &self.destination.data_dest {
            DataDestination::Database(dest) => Ok(dest
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
        if self
            .destination
            .format
            .intersects(ItemContext::sql_databases())
        {
            self.schema_manager.infer_schema(&schema_plan).await?;
            return Ok(());
        }
        Err(SettingsError::UnsupportedDestinationFormat(
            self.destination.format.to_string(),
        ))
    }

    pub async fn build_schema_plan(&self) -> Result<SchemaPlan, SettingsError> {
        let ignore_constraints = self.state.lock().await.ignore_constraints();
        let mapped_columns_only = self.state.lock().await.copy_columns() == CopyColumns::MapOnly;
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
