use super::{context::SchemaSettingContext, phase::MigrationSettingsPhase, MigrationSetting};
use crate::{
    context::MigrationContext,
    destination::data_dest::DataDestination,
    expr::types::ExpressionWrapper,
    metadata::{fetch_dest_tbl_metadata, fetch_src_tbl_metadata},
};
use async_trait::async_trait;
use postgres::data_type::PgColumnDataType;
use smql::{plan::MigrationPlan, statements::expr::Expression};
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

pub struct CreateMissingColumnsSetting {
    context: SchemaSettingContext,
}

impl CreateMissingColumnsSetting {
    pub async fn new(global: &Arc<Mutex<MigrationContext>>) -> Self {
        Self {
            context: SchemaSettingContext::new(global).await,
        }
    }
}

#[async_trait]
impl MigrationSetting for CreateMissingColumnsSetting {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::CreateMissingColumns
    }

    async fn apply(
        &self,
        plan: &MigrationPlan,
        _context: Arc<Mutex<MigrationContext>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for destination in plan.migration.targets() {
            let dest_name = destination.clone();
            let dest_meta =
                fetch_dest_tbl_metadata(&self.context.destination.data_dest, &dest_name).await?;

            let src_name = self
                .context
                .mapping
                .entity_name_map
                .reverse_resolve(&dest_name);
            let src_meta = fetch_src_tbl_metadata(&self.context.source.primary, &src_name).await?;

            self.add_columns(&dest_name, &src_meta, &dest_meta).await?;
            self.add_computed_columns(&dest_name, &src_meta, &dest_meta)
                .await?;
        }

        {
            let mut state = self.context.state.lock().await;
            state.create_missing_columns = true;
        }

        Ok(())
    }
}

impl CreateMissingColumnsSetting {
    async fn add_columns(
        &self,
        table: &str,
        source_meta: &TableMetadata,
        dest_meta: &TableMetadata,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(columns) = self.context.mapping.field_mappings.get_entity(table) {
            let type_conv = |meta: &ColumnMetadata| ColumnDataType::to_pg_type(meta); // Currently only Postgres
            for (src_col, dst_col) in columns.forward_map() {
                if dest_meta.get_column(&dst_col).is_none() {
                    let meta = source_meta
                        .get_column(&src_col)
                        .ok_or_else(|| format!("{} not in source", src_col))?;
                    let def = ColumnDef::with_type_convertor(&dst_col, &type_conv, meta);
                    self.add_column(table, &def).await?;
                }
            }
        }
        Ok(())
    }

    async fn add_computed_columns(
        &self,
        table: &str,
        source_meta: &TableMetadata,
        dest_meta: &TableMetadata,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let adapter = self.context.source_adapter().await?;
        if let Some(computed) = self.context.mapping.field_mappings.get_computed(table) {
            for comp in computed.iter() {
                if dest_meta.get_column(&comp.name).is_none() {
                    // infer type (possibly lookup from another table)
                    let col_type = match &comp.expression {
                        Expression::Lookup { table: alias, .. } => {
                            let table = self.context.mapping.entity_name_map.resolve(alias);
                            let meta = fetch_src_tbl_metadata(&self.context.source.primary, &table)
                                .await?;
                            ExpressionWrapper(comp.expression.clone())
                                .infer_type(&meta.columns(), &self.context.mapping, &adapter)
                                .await
                        }
                        _ => {
                            ExpressionWrapper(comp.expression.clone())
                                .infer_type(&source_meta.columns(), &self.context.mapping, &adapter)
                                .await
                        }
                    };
                    let data_type =
                        col_type.ok_or_else(|| format!("Couldn’t infer type for {}", comp.name))?;
                    let def = ColumnDef::from_computed(&comp.name, &data_type.to_string());
                    self.add_column(table, &def).await?;
                }
            }
        }
        Ok(())
    }

    /// issue the ALTER TABLE … ADD COLUMN statement
    async fn add_column(
        &self,
        table: &str,
        column: &ColumnDef,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let DataDestination::Database(db) = &self.context.destination.data_dest {
            db.lock().await.add_column(table, column).await?;
            Ok(())
        } else {
            Err("Unsupported data destination".into())
        }
    }
}
