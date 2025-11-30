use super::{
    MigrationSetting, context::SchemaSettingContext, error::SettingsError,
    phase::MigrationSettingsPhase,
};
use crate::{report::dry_run::DryRunReport, settings::validated::ValidatedSettings};
use async_trait::async_trait;
use connectors::{
    metadata::{entity::EntityMetadata, field::FieldMetadata},
    sql::base::{metadata::table::TableMetadata, query::column::ColumnDef},
};
use engine_core::{
    connectors::{destination::Destination, source::Source},
    context::item::ItemContext,
    schema::{
        types::{ExpressionWrapper, TypeInferencer},
        utils::create_column_def,
    },
};
use futures::lock::Mutex;
use model::{core::data_type::DataType, transform::mapping::EntityMapping};
use smql_syntax::ast::expr::Expression;
use std::sync::Arc;

pub struct CreateMissingColumnsSetting {
    context: SchemaSettingContext,
}

#[async_trait]
impl MigrationSetting for CreateMissingColumnsSetting {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::CreateMissingColumns
    }

    async fn apply(&mut self, _ctx: &mut ItemContext) -> Result<(), SettingsError> {
        let dest_name = self.context.destination.name.clone();
        let dest_meta = self
            .context
            .destination
            .data_dest
            .fetch_meta(dest_name.clone())
            .await?;

        let src_name = self
            .context
            .mapping
            .entity_name_map
            .reverse_resolve(&dest_name);
        let src_meta = self
            .context
            .source
            .primary
            .fetch_meta(src_name.clone())
            .await?;

        self.add_columns(&dest_name, &src_meta, &dest_meta).await?;
        self.add_computed_columns(&dest_name, &src_meta, &dest_meta)
            .await?;

        Ok(())
    }
}

impl CreateMissingColumnsSetting {
    async fn add_columns(
        &mut self,
        table: &str,
        source_meta: &EntityMetadata,
        dest_meta: &TableMetadata,
    ) -> Result<(), SettingsError> {
        if let Some(columns) = self.context.mapping.field_mappings.get_entity(table) {
            let type_conv = |meta: &FieldMetadata| -> (DataType, Option<usize>) { meta.pg_type() }; // Currently only Postgres
            for (src_col, dst_col) in columns.forward_map() {
                if dest_meta.get_column(&dst_col).is_none() {
                    let meta = source_meta.column(&src_col).ok_or_else(|| {
                        SettingsError::MissingSourceColumn(format!("{src_col} not in source"))
                    })?;
                    let def = create_column_def(&dst_col, &type_conv, &meta);
                    self.context.schema_manager.add_column(table, &def).await?;
                }
            }
        }
        Ok(())
    }

    async fn add_computed_columns(
        &mut self,
        table: &str,
        source_meta: &EntityMetadata,
        dest_meta: &TableMetadata,
    ) -> Result<(), SettingsError> {
        let source = self.context.source.primary.clone();
        if let Some(computed) = self.context.mapping.field_mappings.get_computed(table) {
            for comp in computed.iter() {
                if dest_meta.get_column(&comp.name).is_none() {
                    // infer type (possibly lookup from another table)
                    let col_type = match &comp.expression {
                        Expression::Lookup { entity: alias, .. } => {
                            let table = self.context.mapping.entity_name_map.resolve(alias);
                            let meta = self.context.source.primary.fetch_meta(table).await?;
                            ExpressionWrapper(comp.expression.clone())
                                .infer_type(&meta.columns(), &self.context.mapping, &source)
                                .await
                        }
                        _ => {
                            ExpressionWrapper(comp.expression.clone())
                                .infer_type(&source_meta.columns(), &self.context.mapping, &source)
                                .await
                        }
                    };
                    let data_type = col_type.ok_or_else(|| {
                        SettingsError::DataTypeInference(format!(
                            "Couldn't infer type for {}",
                            comp.name
                        ))
                    })?;
                    let def = ColumnDef::from_computed(&comp.name, &data_type);
                    self.context.schema_manager.add_column(table, &def).await?;
                }
            }
        }
        Ok(())
    }
}

impl CreateMissingColumnsSetting {
    pub async fn new(
        src: &Source,
        dest: &Destination,
        mapping: &EntityMapping,
        settings: &ValidatedSettings,
        dry_run_report: &Arc<Mutex<DryRunReport>>,
    ) -> Self {
        Self {
            context: SchemaSettingContext::new(src, dest, mapping, settings, dry_run_report).await,
        }
    }
}
