use crate::{
    metadata::{entity::EntityMetadata, field::FieldMetadata},
    schema::types::TypeInferencer,
    source::data::DataSource,
};
use async_trait::async_trait;
use common::{mapping::EntityMapping, types::DataType};
use smql::statements::expr::{Expression, Literal};
use tracing::warn;

/// A thin newtype wrapper around `Expression` to implement
/// `TypeInferencer` without touching the SMQL crate.
pub struct ExpressionWrapper(pub Expression);

#[async_trait]
impl TypeInferencer for ExpressionWrapper {
    /// Inspect the wrapped `Expression` and produce a SQL-like `DataType`.
    async fn infer_type(
        &self,
        columns: &[FieldMetadata],
        mapping: &EntityMapping,
        source: &DataSource,
    ) -> Option<DataType> {
        match &self.0 {
            Expression::Identifier(identifier) => columns
                .iter()
                .find(|col| col.name().eq_ignore_ascii_case(identifier))
                .map(|col| col.data_type()),

            Expression::Literal(literal) => Some(match literal {
                Literal::String(_) => DataType::String,
                Literal::Integer(_) => DataType::Int,
                Literal::Float(_) => DataType::Float,
                Literal::Boolean(_) => DataType::Boolean,
            }),

            Expression::Arithmetic { left, right, .. } => {
                let lt = ExpressionWrapper((**left).clone())
                    .infer_type(columns, mapping, source)
                    .await?;
                let rt = ExpressionWrapper((**right).clone())
                    .infer_type(columns, mapping, source)
                    .await?;
                Some(get_numeric_type(&lt, &rt))
            }

            Expression::FunctionCall { name, .. } => match name.to_ascii_lowercase().as_str() {
                "lower" | "upper" | "concat" => Some(DataType::VarChar),
                _ => None,
            },

            Expression::Lookup { entity, key, .. } => {
                let table_name = mapping.entity_name_map.resolve(entity);
                let meta = source.fetch_meta(table_name).await.ok()?;
                match meta {
                    EntityMetadata::Table(meta) => meta
                        .columns()
                        .iter()
                        .find(|col| col.name.eq_ignore_ascii_case(key))
                        .map(|col| col.data_type.clone()),
                    EntityMetadata::Csv(meta) => meta
                        .columns
                        .iter()
                        .find(|col| col.name.eq_ignore_ascii_case(key))
                        .map(|col| col.data_type.clone()),
                }
            }
        }
    }
}

fn get_numeric_type(left: &DataType, right: &DataType) -> DataType {
    match (left, right) {
        (DataType::Int, DataType::Int) => DataType::Int,
        (DataType::Float, DataType::Float) => DataType::Float,
        (DataType::Int, DataType::Float) => DataType::Float,
        (DataType::Float, DataType::Int) => DataType::Float,
        (DataType::Decimal, DataType::Decimal) => DataType::Decimal,
        (DataType::Int, DataType::Decimal) => DataType::Decimal,
        (DataType::Decimal, DataType::Int) => DataType::Decimal,
        (DataType::Float, DataType::Decimal) => DataType::Decimal,
        (DataType::Decimal, DataType::Float) => DataType::Decimal,
        _ => {
            warn!(
                "Incompatible types for arithmetic operation: {:?} and {:?}",
                left, right
            );
            DataType::String // Fallback to String for unsupported types
        }
    }
}
