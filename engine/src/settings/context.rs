use super::error::SettingsError;
use crate::{
    context::item::ItemContext,
    destination::{data::DataDestination, Destination},
    metadata::field::FieldMetadata,
    schema::{plan::SchemaPlan, types::TypeEngine},
    source::{data::DataSource, Source},
    state::MigrationState,
};
use common::{mapping::EntityMapping, types::DataType};
use smql::statements::setting::CopyColumns;
use sql_adapter::{
    adapter::SqlAdapter,
    error::db::DbError,
    metadata::{column::ColumnMetadata, table::TableMetadata},
};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::{error, info};

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
            self.infer_schema(&schema_plan).await?;
            return Ok(());
        }
        Err(SettingsError::UnsupportedDestinationFormat(
            self.destination.format.to_string(),
        ))
    }

    pub async fn build_schema_plan(&self) -> Result<SchemaPlan<'_>, SettingsError> {
        let ignore_constraints = self.state.lock().await.ignore_constraints;
        let mapped_columns_only = self.state.lock().await.copy_columns == CopyColumns::MapOnly;
        let source = self.source.primary.clone();

        let type_engine = TypeEngine::new(
            source.clone(),
            // converter
            &|meta: &FieldMetadata| -> (DataType, Option<usize>) { meta.pg_type() },
            // extractor
            &|meta: &TableMetadata| -> Vec<ColumnMetadata> { TableMetadata::enums(meta) },
        );

        Ok(SchemaPlan::new(
            source,
            type_engine,
            ignore_constraints,
            mapped_columns_only,
            self.mapping.clone(),
        ))
    }

    async fn infer_schema(&self, schema_plan: &SchemaPlan<'_>) -> Result<(), DbError> {
        let mut state = self.state.lock().await;
        let enum_queries = schema_plan.enum_queries().await?;
        let table_queries = schema_plan.table_queries().await;
        let fk_queries = schema_plan.fk_queries();

        let all_queries = enum_queries
            .iter()
            .chain(&table_queries)
            .chain(&fk_queries)
            .cloned();

        for query in all_queries {
            if let Some(report) = state.validation_report.as_mut() {
                report.generated_queries.ddl.push((query.clone(), None));
            }

            if state.is_validation_run {
                info!("Validation run - skipping execution of query: {}", query);
                continue;
            }

            info!("Executing query: {}", query);
            if let Err(err) = self
                .destination
                .data_dest
                .adapter()
                .await
                .execute(&query)
                .await
            {
                error!("Failed to execute query: {}\nError: {:?}", query, err);
                return Err(err);
            }
        }

        info!("Schema inference completed");
        Ok(())
    }
}
