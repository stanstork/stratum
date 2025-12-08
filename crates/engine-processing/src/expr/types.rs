use async_trait::async_trait;
use connectors::{
    metadata::{entity::EntityMetadata, field::FieldMetadata},
    sql::base::{
        adapter::SqlAdapter,
        metadata::{column::ColumnMetadata, table::TableMetadata},
    },
};
use engine_core::connectors::source::DataSource;
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
    ) -> Option<DataType>;
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
    ) -> Option<DataType> {
        // Clone the expression node into wrapper and run inference.
        let expr = ExpressionWrapper(computed.expression.clone());
        let data_type = expr.infer_type(columns, mapping, &self.source).await;

        if let Some(data_type) = data_type {
            Some(data_type)
        } else {
            // DotPath with 2+ segments represents cross-entity references
            match &computed.expression {
                CompiledExpression::DotPath(segments) if segments.len() >= 2 => None,
                _ => {
                    panic!(
                        "Failed to infer type for computed column `{}`.",
                        computed.name
                    );
                }
            }
        }
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
    ) -> Option<DataType> {
        match &self.0 {
            CompiledExpression::Identifier(identifier) => columns
                .iter()
                .find(|col| col.name().eq_ignore_ascii_case(identifier))
                .map(|col| col.data_type()),

            CompiledExpression::Literal(value) => Some(match value {
                Value::String(_) => DataType::String,
                Value::Int(_) => DataType::Int,
                Value::Float(_) => DataType::Float,
                Value::Boolean(_) => DataType::Boolean,
                Value::Null => DataType::String, // Default for null
                _ => DataType::String,           // Fallback for other types
            }),

            CompiledExpression::Binary { left, right, .. } => {
                let lt = ExpressionWrapper((**left).clone())
                    .infer_type(columns, mapping, source)
                    .await?;
                let rt = ExpressionWrapper((**right).clone())
                    .infer_type(columns, mapping, source)
                    .await?;
                Some(get_numeric_type(&lt, &rt))
            }

            CompiledExpression::FunctionCall { name, .. } => {
                match name.to_ascii_lowercase().as_str() {
                    "lower" | "upper" | "concat" => Some(DataType::VarChar),
                    _ => None,
                }
            }

            // DotPath with 2+ segments = cross-entity reference (table.column)
            CompiledExpression::DotPath(segments) if segments.len() >= 2 => {
                let entity = &segments[0];
                let key = &segments[1];
                let table_name = mapping.entities.resolve(entity);
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

            // Single-segment DotPath is just a field reference
            CompiledExpression::DotPath(segments) if segments.len() == 1 => columns
                .iter()
                .find(|col| col.name().eq_ignore_ascii_case(&segments[0]))
                .map(|col| col.data_type()),

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
                Some(DataType::Boolean)
            }

            CompiledExpression::Array(_) => None, // Arrays not yet supported
            CompiledExpression::DotPath(_) => None, // Empty DotPath
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
