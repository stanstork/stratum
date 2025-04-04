use crate::state::MigrationState;
use crate::{
    context::MigrationContext, destination::data_dest::DataDestination,
    source::data_source::DataSource,
};
use async_trait::async_trait;
use common::mapping::{FieldNameMap, ScopedNameMap};
use postgres::data_type::PgColumnDataType;
use smql::statements::setting::{Setting, SettingValue};
use smql::{plan::MigrationPlan, statements::connection::DataFormat};
use sql_adapter::adapter::SqlAdapter;
use sql_adapter::metadata::column::data_type::ColumnDataType;
use sql_adapter::metadata::column::metadata::ColumnMetadata;
use sql_adapter::metadata::provider::MetadataProvider;
use sql_adapter::metadata::table::TableMetadata;
use sql_adapter::schema::plan::SchemaPlan;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

#[async_trait]
pub trait MigrationSetting {
    async fn apply(
        &self,
        plan: &MigrationPlan,
        context: Arc<Mutex<MigrationContext>>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

pub struct InferSchemaSetting {
    source: DataSource,
    source_format: DataFormat,
    destination: DataDestination,
    dest_format: DataFormat,
    table_name_map: FieldNameMap,
    column_name_map: ScopedNameMap,
    state: Arc<Mutex<MigrationState>>,
}

pub struct BatchSizeSetting(pub i64);

#[async_trait]
impl MigrationSetting for InferSchemaSetting {
    async fn apply(
        &self,
        plan: &MigrationPlan,
        _context: Arc<Mutex<MigrationContext>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.apply_schema(plan).await?;

        self.set_source_metadata(&plan.migration.sources()).await?;
        self.set_destination_metadata(&plan.migration.targets())
            .await?;

        // Set the infer schema flag to global state
        {
            let mut state = self.state.lock().await;
            state.infer_schema = true;
        }

        info!("Infer schema setting applied");
        Ok(())
    }
}

impl InferSchemaSetting {
    pub async fn new(context: &Arc<Mutex<MigrationContext>>) -> Self {
        let ctx = context.lock().await;
        InferSchemaSetting {
            source: ctx.source.clone(),
            source_format: ctx.source_format,
            destination: ctx.destination.clone(),
            dest_format: ctx.destination_format,
            table_name_map: ctx.entity_name_map.clone(),
            column_name_map: ctx.field_name_map.clone(),
            state: ctx.state.clone(),
        }
    }

    async fn apply_schema(&self, plan: &MigrationPlan) -> Result<(), Box<dyn std::error::Error>> {
        let type_converter = |meta: &ColumnMetadata| ColumnDataType::to_pg_type(meta);
        let type_extractor = |meta: &TableMetadata| TableMetadata::enums(meta);

        let source_adapter = self.source_adapter().await?;

        let mut schema_plan = SchemaPlan::new(
            source_adapter,
            &type_converter,
            &type_extractor,
            self.table_name_map.clone(),
            self.column_name_map.clone(),
        );

        for migration in plan.migration.migrations.iter() {
            if !self.destination_exists(&migration.target).await? {
                info!("Destination table does not exist. Infer schema setting will be applied");
                self.infer_schema(&migration.sources, &mut schema_plan)
                    .await?;
            }
        }

        self.apply_to_destination(schema_plan).await
    }

    async fn destination_exists(&self, table: &str) -> Result<bool, Box<dyn std::error::Error>> {
        match &self.destination {
            DataDestination::Database(dest) => Ok(dest.lock().await.table_exists(table).await?),
        }
    }

    async fn infer_schema(
        &self,
        tables: &Vec<String>,
        schema_plan: &mut SchemaPlan<'_>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let (DataSource::Database(_source), true) = (
            &self.source,
            self.source_format.intersects(DataFormat::sql_databases()),
        ) {
            let adapter = self.source_adapter().await?;

            // Build full metadata for the source
            let metadata_graph = MetadataProvider::build_metadata_graph(&*adapter, &tables).await?;
            for metadata in metadata_graph.values() {
                let table_name = metadata.name.clone();

                if schema_plan.metadata_exists(&table_name) {
                    continue;
                }

                MetadataProvider::collect_schema_deps(&metadata, schema_plan);
                schema_plan.add_metadata(&table_name, metadata.clone());
            }
            Ok(())
        } else {
            Err("Unsupported data source format".into())
        }
    }

    async fn apply_to_destination(
        &self,
        schema_plan: SchemaPlan<'_>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match (
            &self.destination,
            self.dest_format.intersects(DataFormat::sql_databases()),
        ) {
            (DataDestination::Database(destination), true) => {
                destination.lock().await.infer_schema(&schema_plan).await?;
                Ok(())
            }
            _ => Err("Unsupported data destination format".into()),
        }
    }

    pub async fn set_source_metadata(
        &self,
        source_tables: &[String],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let DataSource::Database(src) = &self.source {
            let mut src_guard = src.lock().await;
            let metadata =
                MetadataProvider::build_metadata_graph(src_guard.adapter().as_ref(), source_tables)
                    .await?;
            src_guard.set_metadata(metadata);
        }
        Ok(())
    }

    pub async fn set_destination_metadata(
        &self,
        destination_tables: &[String],
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let DataDestination::Database(dest) = &self.destination {
            let mut dest_guard = dest.lock().await;
            let metadata = MetadataProvider::build_metadata_graph(
                dest_guard.adapter().as_ref(),
                destination_tables,
            )
            .await?;
            dest_guard.set_metadata(metadata);
        }
        Ok(())
    }

    async fn source_adapter(
        &self,
    ) -> Result<Arc<dyn SqlAdapter + Send + Sync>, Box<dyn std::error::Error>> {
        match &self.source {
            DataSource::Database(source) => Ok(source.lock().await.adapter()),
        }
    }
}

#[async_trait]
impl MigrationSetting for BatchSizeSetting {
    async fn apply(
        &self,
        _plan: &MigrationPlan,
        context: Arc<Mutex<MigrationContext>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let context = context.lock().await;
        let mut state = context.state.lock().await;
        state.batch_size = self.0 as usize;
        info!("Batch size setting applied");
        Ok(())
    }
}

pub async fn parse_settings(
    settings: &[Setting],
    context: &Arc<Mutex<MigrationContext>>,
) -> Vec<Box<dyn MigrationSetting>> {
    let mut migration_settings = Vec::new();
    for setting in settings {
        match (setting.key.as_str(), setting.value.clone()) {
            ("infer_schema", SettingValue::Boolean(true)) => {
                migration_settings
                    .push(Box::new(InferSchemaSetting::new(context).await)
                        as Box<dyn MigrationSetting>)
            }
            ("batch_size", SettingValue::Integer(size)) => migration_settings
                .push(Box::new(BatchSizeSetting(size)) as Box<dyn MigrationSetting>),
            _ => (), // Ignore unknown settings
        }
    }

    migration_settings
}
