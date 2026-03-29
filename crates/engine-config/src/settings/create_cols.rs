use super::{
    MigrationSetting, context::SchemaSettingContext, driver::SchemaDriver, error::SettingsError,
    phase::MigrationSettingsPhase,
};
use async_trait::async_trait;
use connectors::{
    sql::{
        metadata::{column::ColumnMetadata, table::TableMetadata},
        query::{column::ColumnDef, generator::QueryGenerator},
    },
    traits::introspector::SchemaIntrospector,
};
use engine_core::schema::{
    schema_ops::{SchemaOp, SchemaOps},
    types::{ExpressionWrapper, TypeInferencer},
    utils::create_column_def,
};
use engine_processing::context::PipelineContext;
use model::{core::types::Type, execution::expr::CompiledExpression};
use std::sync::Arc;

pub struct CreateMissingColumnsSetting<S: SchemaDriver, D: SchemaDriver> {
    context: SchemaSettingContext<S, D>,
}

#[async_trait]
impl<S: SchemaDriver, D: SchemaDriver> MigrationSetting for CreateMissingColumnsSetting<S, D> {
    fn phase(&self) -> MigrationSettingsPhase {
        MigrationSettingsPhase::CreateMissingColumns
    }

    async fn plan(&mut self, _ctx: &PipelineContext) -> Result<SchemaOps, SettingsError> {
        self.build_schema_ops().await
    }
}

impl<S: SchemaDriver, D: SchemaDriver> CreateMissingColumnsSetting<S, D> {
    pub async fn new(ctx: SchemaSettingContext<S, D>) -> Self {
        Self { context: ctx }
    }

    async fn build_schema_ops(&self) -> Result<SchemaOps, SettingsError> {
        let dest_name = self.context.destination.name.clone();
        let dest_meta = self
            .context
            .destination
            .driver
            .table_metadata(&dest_name)
            .await?;

        let src_name = self.context.mapping.entities.reverse_resolve(&dest_name);
        let src_meta = self.context.source.driver.table_metadata(&src_name).await?;

        let mut ops = SchemaOps::empty();
        self.plan_add_columns(&dest_name, &src_meta, &dest_meta, &mut ops)?;
        self.plan_add_computed_columns(&dest_name, &src_meta, &dest_meta, &mut ops)
            .await?;

        Ok(ops)
    }

    fn plan_add_columns(
        &self,
        table: &str,
        source_meta: &TableMetadata,
        dest_meta: &TableMetadata,
        ops: &mut SchemaOps,
    ) -> Result<(), SettingsError> {
        if let Some(columns) = self.context.mapping.field_mappings.get_entity(table) {
            let registry = Arc::new(self.context.type_registry());
            let source_dialect = &self.context.source.dialect;
            let type_conv = |meta: &ColumnMetadata| -> (Type, Option<usize>) {
                let source_type = source_dialect.to_canonical(meta);
                let target_type = registry.convert(&source_type).target_type();
                (target_type, meta.char_max_length)
            };

            let query_dialect = self.context.destination.dialect.as_query_dialect();
            let generator = QueryGenerator::new(query_dialect.as_ref());

            for (src_col, dst_col) in columns.forward_map() {
                if dest_meta.get_column(&dst_col).is_none() {
                    let meta = source_meta.column(&src_col).ok_or_else(|| {
                        SettingsError::MissingSourceColumn(format!("{src_col} not in source"))
                    })?;
                    let def = create_column_def(&dst_col, &type_conv, meta);
                    let (sql, _) = generator.add_column(table, def.clone());
                    ops.pre.push(SchemaOp {
                        sql,
                        description: format!("Add column '{}' to table '{}'", def.name, table),
                        idempotent: false,
                        skip_if_missing_ref: false,
                    });
                }
            }
        }
        Ok(())
    }

    async fn plan_add_computed_columns(
        &self,
        table: &str,
        source_meta: &TableMetadata,
        dest_meta: &TableMetadata,
        ops: &mut SchemaOps,
    ) -> Result<(), SettingsError> {
        let source = self.context.source.driver.clone() as Arc<dyn SchemaIntrospector>;
        if let Some(computed) = self.context.mapping.field_mappings.get_computed(table) {
            let query_dialect = self.context.destination.dialect.as_query_dialect();
            let generator = QueryGenerator::new(query_dialect.as_ref());

            for comp in computed.iter() {
                if dest_meta.get_column(&comp.name).is_none() {
                    // infer type (possibly from a cross-entity reference)
                    let col_type = match &comp.expression {
                        CompiledExpression::DotPath(segments) if segments.len() >= 2 => {
                            let alias = &segments[0];
                            let table = self.context.mapping.entities.resolve(alias);
                            let meta = self.context.source.driver.table_metadata(&table).await?;
                            ExpressionWrapper(comp.expression.clone())
                                .infer_type(
                                    &meta.columns(),
                                    &self.context.mapping,
                                    &source,
                                    self.context.source.dialect,
                                )
                                .await
                        }
                        _ => {
                            ExpressionWrapper(comp.expression.clone())
                                .infer_type(
                                    &source_meta.columns(),
                                    &self.context.mapping,
                                    &source,
                                    self.context.source.dialect,
                                )
                                .await
                        }
                    };
                    let data_type = col_type.ok_or_else(|| {
                        SettingsError::DataTypeInference(format!(
                            "Couldn't infer type for {}",
                            comp.name
                        ))
                    })?;
                    let def = ColumnDef::from_computed(&comp.name, &data_type.0);
                    let (sql, _) = generator.add_column(table, def.clone());
                    ops.pre.push(SchemaOp {
                        sql,
                        description: format!(
                            "Add computed column '{}' to table '{}'",
                            comp.name, table
                        ),
                        idempotent: false,
                        skip_if_missing_ref: false,
                    });
                }
            }
        }
        Ok(())
    }
}
