use async_trait::async_trait;
use common::{computed::ComputedField, mapping::EntityMapping};
use smql::statements::expr::{Expression, Literal};
use sql_adapter::{
    metadata::column::{data_type::ColumnDataType, metadata::ColumnMetadata},
    schema::types::{AdapterRef, TypeInferencer},
};
use std::{future::Future, pin::Pin};

/// A thin newtype wrapper around `Expression` to implement
/// `TypeInferencer` without touching the SMQL crate.
pub struct ExpressionWrapper(pub Expression);

/// Helper function to infer the type of a computed field.
pub async fn infer_computed_type(
    computed: &ComputedField,
    columns: &[ColumnMetadata],
    mapping: &EntityMapping,
    adapter: &AdapterRef,
) -> Option<ColumnDataType> {
    // Clone the expression node into wrapper and run inference.
    let expr = ExpressionWrapper(computed.expression.clone());
    let data_type = expr.infer_type(columns, mapping, adapter).await;

    if let Some(data_type) = data_type {
        Some(data_type)
    } else {
        match computed.expression {
            Expression::Lookup { .. } => None,
            _ => {
                panic!(
                    "Failed to infer type for computed column `{}`.",
                    computed.name
                );
            }
        }
    }
}

/// Boxes the anonymous future returned by `infer_computed_type` into a
/// `Pin<Box<dyn Future<Output = Option<ColumnDataType>> + Send + 'a>>`. This:
/// 1. Erases the compiler-generated, unnamed `impl Future` type into a single,
///    heap-allocated trait object.
/// 2. Gives the function a concrete, nameable return type that exactly matches
///    `InferComputedTypeFn` alias.
///
/// Without this boxing shim, there’s no way to coerce the raw `async fn` into
/// a plain function-pointer signature, because its real future type is anonymous.
pub fn boxed_infer_computed_type<'a>(
    computed: &'a ComputedField,
    columns: &'a [ColumnMetadata],
    mapping: &'a EntityMapping,
    adapter: &'a AdapterRef,
) -> Pin<Box<dyn Future<Output = Option<ColumnDataType>> + Send + 'a>> {
    // Box the future returned by async fn
    Box::pin(infer_computed_type(computed, columns, mapping, adapter))
}

#[async_trait]
impl TypeInferencer for ExpressionWrapper {
    /// Inspect the wrapped `Expression` and produce a SQL-like `ColumnDataType`.
    async fn infer_type(
        &self,
        columns: &[ColumnMetadata],
        mapping: &EntityMapping,
        adapter: &AdapterRef,
    ) -> Option<ColumnDataType> {
        match &self.0 {
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
                let lt = ExpressionWrapper((**left).clone())
                    .infer_type(columns, mapping, adapter)
                    .await?;
                let rt = ExpressionWrapper((**right).clone())
                    .infer_type(columns, mapping, adapter)
                    .await?;
                Some(get_numeric_type(lt, rt))
            }

            Expression::FunctionCall { name, .. } => match name.to_ascii_lowercase().as_str() {
                "lower" | "upper" | "concat" => Some(ColumnDataType::VarChar),
                _ => None,
            },

            Expression::Lookup { entity, key, .. } => {
                let table_name = mapping.entity_name_map.resolve(entity);
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
