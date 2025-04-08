use super::MigrationSetting;
use crate::{context::MigrationContext, destination::data_dest::DataDestination};
use async_trait::async_trait;
use postgres::data_type::PgColumnDataType;
use smql::plan::MigrationPlan;
use sql_adapter::{
    metadata::{
        column::{data_type::ColumnDataType, metadata::ColumnMetadata},
        table::TableMetadata,
    },
    query::column::ColumnDef,
    schema::types::TypeInferencer,
};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct CreateMissingColumnsSetting;

impl CreateMissingColumnsSetting {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl MigrationSetting for CreateMissingColumnsSetting {
    async fn apply(
        &self,
        plan: &MigrationPlan,
        context: Arc<Mutex<MigrationContext>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let context = context.lock().await;
        for destination in plan.migration.targets() {
            let dest_name = destination.clone();
            let dest_metadata = context.destination.fetch_metadata(&dest_name).await?;

            let src_name = context.entity_name_map.reverse_resolve(&dest_name);
            let src_metadata = context.source.fetch_metadata(&src_name).await?;

            Self::add_columns(&context, &dest_name, &src_metadata, &dest_metadata).await?;
            Self::add_computed_columns(&context, &dest_name, &src_metadata, &dest_metadata).await?;
        }

        let mut state = context.state.lock().await;
        state.create_missing_columns = true;
        Ok(())
    }
}

impl CreateMissingColumnsSetting {
    async fn add_columns(
        context: &MigrationContext,
        table: &str,
        source_metadata: &TableMetadata,
        dest_metadata: &TableMetadata,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(columns) = context.field_name_map.get_scope(table) {
            for (source_col, dest_col) in columns.forward_map() {
                let source_col_meta = source_metadata
                    .get_column(&source_col)
                    .ok_or_else(|| format!("Column {} not found in source metadata", source_col))?;

                // Currently, we only support PostgreSQL as a destination
                let type_converter = |meta: &ColumnMetadata| ColumnDataType::to_pg_type(meta);

                if dest_metadata.get_column(&dest_col).is_none() {
                    let col_def =
                        ColumnDef::with_type_convertor(&dest_col, &type_converter, source_col_meta);
                    Self::add_column(context, table, &col_def).await?;
                }
            }
        }
        Ok(())
    }

    async fn add_computed_columns(
        context: &MigrationContext,
        table: &str,
        source_metadata: &TableMetadata,
        dest_metadata: &TableMetadata,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(computed) = context.field_name_map.get_computed(table) {
            for computed_col in computed.iter() {
                if dest_metadata.get_column(&computed_col.name).is_none() {
                    // Add the computed column to the destination table
                    let col_type = computed_col
                        .expression
                        .infer_type(&source_metadata.columns());
                    if let Some(col_type) = col_type {
                        let col_def =
                            ColumnDef::from_computed(&computed_col.name, &col_type.to_string());
                        Self::add_column(context, table, &col_def).await?;
                    } else {
                        return Err(format!(
                            "Failed to infer type for computed column {}",
                            computed_col.name
                        )
                        .into());
                    }
                }
            }
        }
        Ok(())
    }

    async fn add_column(
        context: &MigrationContext,
        table: &str,
        column: &ColumnDef,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match &context.destination {
            DataDestination::Database(db) => {
                let db = db.lock().await;
                db.add_column(table, column).await?;
                Ok(())
            }
            _ => Err("Unsupported data destination format".into()),
        }
    }
}
