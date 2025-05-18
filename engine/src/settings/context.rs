use super::error::SettingsError;
use crate::{
    context::item::ItemContext,
    destination::{data::DataDestination, Destination},
    expr::types::boxed_infer_computed_type,
    source::{data::DataSource, Source},
    state::MigrationState,
};
use common::{mapping::EntityMapping, types::DataType};
use postgres::data_type::PgDataType;
use smql::statements::setting::CopyColumns;
use sql_adapter::{
    adapter::SqlAdapter,
    metadata::{column::ColumnMetadata, table::TableMetadata},
    schema::{plan::SchemaPlan, types::TypeEngine},
};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct SchemaSettingContext {
    pub source: Source,
    pub destination: Destination,
    pub mapping: EntityMapping,
    pub state: Arc<Mutex<MigrationState>>,
}

impl SchemaSettingContext {
    pub fn new(
        src: &Source,
        dest: &Destination,
        mapping: &EntityMapping,
        state: &Arc<Mutex<MigrationState>>,
    ) -> Self {
        Self {
            source: src.clone(),
            destination: dest.clone(),
            mapping: mapping.clone(),
            state: state.clone(),
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
        &self,
        schema_plan: SchemaPlan<'_>,
    ) -> Result<(), SettingsError> {
        if self
            .destination
            .format
            .intersects(ItemContext::sql_databases())
        {
            let DataDestination::Database(dest) = &self.destination.data_dest;
            dest.lock().await.infer_schema(&schema_plan).await?;
            return Ok(());
        }
        Err(SettingsError::UnsupportedDestinationFormat(
            self.destination.format.to_string(),
        ))
    }

    pub async fn build_schema_plan(&self) -> Result<SchemaPlan<'_>, SettingsError> {
        let adapter = self.source_adapter().await?;
        let ignore_constraints = self.state.lock().await.ignore_constraints;
        let mapped_columns_only = self.state.lock().await.copy_columns == CopyColumns::MapOnly;

        let type_engine = TypeEngine::new(
            adapter.clone(),
            // converter
            &|meta: &ColumnMetadata| -> (String, Option<usize>) { DataType::to_pg_type(meta) },
            // extractor
            &|meta: &TableMetadata| -> Vec<ColumnMetadata> { TableMetadata::enums(meta) },
            // INFERENCER → just the function pointer
            boxed_infer_computed_type,
        );

        Ok(SchemaPlan::new(
            adapter,
            type_engine,
            ignore_constraints,
            mapped_columns_only,
            self.mapping.clone(),
        ))
    }
}
