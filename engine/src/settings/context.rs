use super::error::SettingsError;
use crate::{
    context::item::ItemContext,
    destination::{data::DataDestination, Destination},
    metadata::field::FieldMetadata,
    schema::{plan::SchemaPlan, types::TypeEngine},
    settings::schema_manager::{LiveSchemaManager, SchemaManager, ValidationSchemaManager},
    source::{data::DataSource, Source},
    state::MigrationState,
};
use common::{mapping::EntityMapping, types::DataType};
use smql::statements::setting::CopyColumns;
use sql_adapter::{
    adapter::SqlAdapter,
    metadata::{column::ColumnMetadata, table::TableMetadata},
};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct SchemaSettingContext {
    pub source: Source,
    pub destination: Destination,
    pub mapping: EntityMapping,
    pub state: Arc<Mutex<MigrationState>>,
    pub schema_manager: Box<dyn SchemaManager>,
}

impl SchemaSettingContext {
    pub async fn new(
        src: &Source,
        dest: &Destination,
        mapping: &EntityMapping,
        state: &Arc<Mutex<MigrationState>>,
    ) -> Self {
        let is_dry_run = state.lock().await.is_dry_run();

        let schema_manager: Box<dyn SchemaManager + Send> = if is_dry_run {
            // The validation manager is created for dry-run validation runs.
            Box::new(ValidationSchemaManager {
                report: state.lock().await.dry_run_report(),
            })
        } else {
            // The live manager is created for a real migration run.
            Box::new(LiveSchemaManager {
                destination: Arc::new(Mutex::new(dest.clone())),
            })
        };

        Self {
            source: src.clone(),
            destination: dest.clone(),
            mapping: mapping.clone(),
            state: state.clone(),
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
