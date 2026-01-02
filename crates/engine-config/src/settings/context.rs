use super::error::SettingsError;
use crate::settings::CopyColumns;
use crate::settings::{schema_manager::SchemaManager, validated::ValidatedSettings};
use connectors::{
    metadata::field::FieldMetadata,
    sql::base::{
        adapter::SqlAdapter,
        metadata::{column::ColumnMetadata, table::TableMetadata},
    },
};
use engine_core::schema::planner::SchemaPlanner;
use engine_core::{
    connectors::{
        destination::{DataDestination, Destination},
        source::{DataSource, Source},
    },
    schema::{plan::SchemaPlan, types::TypeEngine},
};
use futures::lock::Mutex;
use model::{core::data_type::DataType, transform::mapping::TransformationMetadata};
use std::sync::Arc;

pub struct SchemaSettingContext {
    pub source: Source,
    pub destination: Destination,
    pub mapping: TransformationMetadata,
    pub settings: ValidatedSettings,
    pub schema_manager: SchemaManager,
}

impl SchemaSettingContext {
    pub async fn new(
        src: &Source,
        dest: &Destination,
        mapping: &TransformationMetadata,
        settings: &ValidatedSettings,
    ) -> Self {
        Self {
            source: src.clone(),
            destination: dest.clone(),
            mapping: mapping.clone(),
            settings: settings.clone(),
            schema_manager: SchemaManager {
                destination: Arc::new(Mutex::new(dest.clone())),
            },
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
        Ok(())
    }

    pub async fn init_schema_planner(&self) -> Result<SchemaPlanner, SettingsError> {
        let ignore_constraints = self.settings.ignore_constraints();
        let mapped_columns_only = *self.settings.copy_columns() == CopyColumns::MapOnly;
        Ok(SchemaPlanner::new(
            self.source.clone(),
            self.mapping.clone(),
            ignore_constraints,
            mapped_columns_only,
        ))
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
