use crate::connectors::source::DataSource;
use async_trait::async_trait;
use connectors::{
    metadata::{entity::EntityMetadata, field::FieldMetadata},
    sql::base::{
        adapter::SqlAdapter,
        metadata::{column::ColumnMetadata, table::TableMetadata},
    },
};
use model::{
    core::{data_type::DataType, value::Value},
    execution::expr::CompiledExpression,
    transform::{computed_field::ComputedField, mapping::TransformationMetadata},
};
use std::sync::Arc;
use tracing::warn;

/// A thin newtype wrapper around `CompiledExpression` to implement
/// `TypeInferencer` without touching the model crate.
pub struct ExpressionWrapper(pub CompiledExpression);

// Alias for the SQL adapter reference
pub type AdapterRef = Arc<dyn SqlAdapter + Send + Sync>;

/// A function that converts a source database type to a target database type,
/// returning the target type name and optional size (e.g., MySQL `blob` → PostgreSQL `bytea`).
pub type TypeConverter = dyn Fn(&FieldMetadata) -> (DataType, Option<usize>) + Send + Sync;

/// A function that extracts enums from a table's metadata.
pub type EnumExtractor = dyn Fn(&TableMetadata) -> Vec<ColumnMetadata> + Send + Sync;

pub struct TypeEngine {
    source: DataSource,

    /// Function used to convert column types from source to target database format.
    type_converter: Box<TypeConverter>,

    /// Function used to extract enums from table metadata.
    enum_extractor: Box<EnumExtractor>,
}

#[async_trait]
pub trait TypeInferencer {
    async fn infer_type(
        &self,
        columns: &[FieldMetadata],
        mapping: &TransformationMetadata,
        source: &DataSource,
    ) -> Option<(DataType, Option<usize>)>;
}

impl TypeEngine {
    pub fn new(
        source: DataSource,
        type_converter: Box<TypeConverter>,
        enum_extractor: Box<EnumExtractor>,
    ) -> Self {
        Self {
            source,
            type_converter,
            enum_extractor,
        }
    }

    pub fn type_converter(&self) -> &TypeConverter {
        self.type_converter.as_ref()
    }

    pub fn enum_extractor(&self) -> &EnumExtractor {
        self.enum_extractor.as_ref()
    }

    pub async fn infer_computed_type(
        &self,
        computed: &ComputedField,
        columns: &[FieldMetadata],
        mapping: &TransformationMetadata,
    ) -> Option<(DataType, Option<usize>)> {
        // Clone the expression node into wrapper and run inference.
        let expr = ExpressionWrapper(computed.expression.clone());
        expr.infer_type(columns, mapping, &self.source).await
    }
}

#[async_trait]
impl TypeInferencer for ExpressionWrapper {
    /// Inspect the wrapped `CompiledExpression` and produce a SQL-like `DataType`.
    async fn infer_type(
        &self,
        columns: &[FieldMetadata],
        mapping: &TransformationMetadata,
        source: &DataSource,
    ) -> Option<(DataType, Option<usize>)> {
        match &self.0 {
            CompiledExpression::Identifier(identifier) => columns
                .iter()
                .find(|col| col.name().eq_ignore_ascii_case(identifier))
                .map(|col| (col.data_type(), col.char_max_length())),

            CompiledExpression::Literal(value) => Some(match value {
                Value::String(_) => (DataType::String, None),
                Value::Int(_) => (DataType::Int, None),
                Value::Float(_) => (DataType::Float, None),
                Value::Boolean(_) => (DataType::Boolean, None),
                Value::Null => (DataType::String, None), // Default for null
                _ => (DataType::String, None),           // Fallback for other types
            }),

            CompiledExpression::Binary { left, right, .. } => {
                let lt = ExpressionWrapper((**left).clone())
                    .infer_type(columns, mapping, source)
                    .await?;
                let rt = ExpressionWrapper((**right).clone())
                    .infer_type(columns, mapping, source)
                    .await?;
                Some(get_numeric_type(&lt.0, &rt.0))
            }

            CompiledExpression::FunctionCall { name, .. } => {
                match name.to_ascii_lowercase().as_str() {
                    "lower" | "upper" | "concat" => Some((DataType::VarChar, None)),
                    "env" => Some((DataType::VarChar, None)), // env() always returns string
                    _ => None,
                }
            }

            // DotPath with 2+ segments = cross-entity reference (table.column)
            CompiledExpression::DotPath(segments) if segments.len() >= 2 => {
                let entity = &segments[0];
                let key = &segments[1];
                let table_name = mapping.entities.reverse_resolve(entity);

                // For DotPath, the entity name IS the source table name, not a destination
                let meta = source.fetch_meta(table_name).await.ok()?;
                match meta {
                    EntityMetadata::Table(meta) => meta
                        .columns()
                        .iter()
                        .find(|col| col.name.eq_ignore_ascii_case(key))
                        .map(|col| (col.data_type.clone(), col.char_max_length)),
                    EntityMetadata::Csv(meta) => meta
                        .columns
                        .iter()
                        .find(|col| col.name.eq_ignore_ascii_case(key))
                        .map(|col| (col.data_type.clone(), None)),
                }
            }

            // Single-segment DotPath is just a field reference
            CompiledExpression::DotPath(segments) if segments.len() == 1 => columns
                .iter()
                .find(|col| col.name().eq_ignore_ascii_case(&segments[0]))
                .map(|col| (col.data_type(), col.char_max_length())),

            // Handle other expression types
            CompiledExpression::Unary { operand, .. } => {
                ExpressionWrapper((**operand).clone())
                    .infer_type(columns, mapping, source)
                    .await
            }

            CompiledExpression::Grouped(expr) => {
                ExpressionWrapper((**expr).clone())
                    .infer_type(columns, mapping, source)
                    .await
            }

            CompiledExpression::When {
                branches,
                else_expr,
            } => {
                // Try to infer from first branch value, fallback to else
                if let Some(branch) = branches.first() {
                    ExpressionWrapper(branch.value.clone())
                        .infer_type(columns, mapping, source)
                        .await
                } else if let Some(else_val) = else_expr {
                    ExpressionWrapper((**else_val).clone())
                        .infer_type(columns, mapping, source)
                        .await
                } else {
                    None
                }
            }

            CompiledExpression::IsNull(_) | CompiledExpression::IsNotNull(_) => {
                Some((DataType::Boolean, None))
            }

            CompiledExpression::Array(_) => None, // Arrays not yet supported
            CompiledExpression::DotPath(_) => None, // Empty DotPath
        }
    }
}

fn get_numeric_type(left: &DataType, right: &DataType) -> (DataType, Option<usize>) {
    match (left, right) {
        (DataType::Int, DataType::Int) => (DataType::Int, None),
        (DataType::Float, DataType::Float) => (DataType::Float, None),
        (DataType::Int, DataType::Float) => (DataType::Float, None),
        (DataType::Float, DataType::Int) => (DataType::Float, None),
        (DataType::Decimal, DataType::Decimal) => (DataType::Decimal, None),
        (DataType::Int, DataType::Decimal) => (DataType::Decimal, None),
        (DataType::Decimal, DataType::Int) => (DataType::Decimal, None),
        (DataType::Float, DataType::Decimal) => (DataType::Decimal, None),
        (DataType::Decimal, DataType::Float) => (DataType::Decimal, None),
        _ => {
            warn!(
                "Incompatible types for arithmetic operation: {:?} and {:?}",
                left, right
            );
            (DataType::String, None) // Fallback to String for unsupported types
        }
    }
}
