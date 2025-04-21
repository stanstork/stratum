use crate::{
    adapter::SqlAdapter,
    metadata::column::{data_type::ColumnDataType, metadata::ColumnMetadata},
};
use async_trait::async_trait;
use common::mapping::EntityMappingContext;
use smql::statements::expr::{Expression, Literal};
use std::sync::Arc;

// Alias for the SQL adapter reference
pub type AdapterRef = Arc<dyn SqlAdapter + Send + Sync>;

#[async_trait]
pub trait TypeInferencer {
    async fn infer_type(
        &self,
        columns: &[ColumnMetadata],
        mapping: &EntityMappingContext,
        adapter: &AdapterRef,
    ) -> Option<ColumnDataType>;
}

#[async_trait]
impl TypeInferencer for Expression {
    async fn infer_type(
        &self,
        columns: &[ColumnMetadata],
        mapping: &EntityMappingContext,
        adapter: &AdapterRef,
    ) -> Option<ColumnDataType> {
        match self {
            Expression::Identifier(identifier) => columns
                .iter()
                .find(|col| col.name.eq_ignore_ascii_case(identifier))
                .map(|col| col.data_type),

            Expression::Literal(literal) => Some(match literal {
                Literal::String(_) => ColumnDataType::String,
                Literal::Integer(_) => ColumnDataType::Int,
                Literal::Float(_) => ColumnDataType::Float,
                Literal::Boolean(_) => ColumnDataType::Boolean,
            }),

            Expression::Arithmetic { left, right, .. } => {
                let lt = left.infer_type(columns, mapping, adapter).await?;
                let rt = right.infer_type(columns, mapping, adapter).await?;
                Some(get_numeric_type(lt, rt))
            }

            Expression::FunctionCall { name, .. } => match name.to_ascii_lowercase().as_str() {
                "lower" | "upper" | "concat" => Some(ColumnDataType::VarChar),
                _ => None,
            },

            Expression::Lookup { table, key, .. } => {
                let table_name = mapping.entity_name_map.resolve(table);
                let meta = adapter.fetch_metadata(&table_name).await.ok()?;
                meta.columns()
                    .iter()
                    .find(|col| col.name.eq_ignore_ascii_case(key))
                    .map(|col| col.data_type)
            }
        }
    }
}

fn get_numeric_type(left: ColumnDataType, right: ColumnDataType) -> ColumnDataType {
    match (left, right) {
        (ColumnDataType::Int, ColumnDataType::Int) => ColumnDataType::Int,
        (ColumnDataType::Float, ColumnDataType::Float) => ColumnDataType::Float,
        (ColumnDataType::Int, ColumnDataType::Float) => ColumnDataType::Float,
        (ColumnDataType::Float, ColumnDataType::Int) => ColumnDataType::Float,
        (ColumnDataType::Decimal, ColumnDataType::Decimal) => ColumnDataType::Decimal,
        (ColumnDataType::Int, ColumnDataType::Decimal) => ColumnDataType::Decimal,
        (ColumnDataType::Decimal, ColumnDataType::Int) => ColumnDataType::Decimal,
        (ColumnDataType::Float, ColumnDataType::Decimal) => ColumnDataType::Decimal,
        (ColumnDataType::Decimal, ColumnDataType::Float) => ColumnDataType::Decimal,
        _ => {
            eprintln!(
                "Incompatible types for arithmetic operation: {:?} and {:?}",
                left, right
            );
            ColumnDataType::String // Fallback to String for unsupported types
        }
    }
}
