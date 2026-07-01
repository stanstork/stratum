use crate::type_registry::{Dialect, TypeRegistry};
use async_trait::async_trait;
use connectors::{
    sql::metadata::{column::ColumnMetadata, table::TableMetadata},
    traits::introspector::SchemaIntrospector,
};
use model::{
    core::{
        types::{FloatSize, IntSize, Type},
        value::Value,
    },
    execution::expr::CompiledExpression,
    transform::{computed_field::ComputedField, mapping::TransformationMetadata},
};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::warn;

/// Canonical types of computed columns already resolved earlier in the same
/// `select`, keyed by lowercased column name. Lets a computed column reference
/// an earlier computed column (which isn't in the source metadata).
pub type ComputedTypes = HashMap<String, (Type, Option<usize>)>;

/// A thin newtype wrapper around `CompiledExpression` to implement
/// `TypeInferencer` without touching the model crate.
pub struct ExpressionWrapper(pub CompiledExpression);

/// Handles type conversion and inference for schema migration.
///
/// Combines type registry (for cross-database conversion) with expression
/// type inference (for computed columns).
pub struct TypeEngine {
    introspector: Arc<dyn SchemaIntrospector>,
    type_registry: Arc<TypeRegistry>,
    source_dialect: Dialect,
}

#[async_trait]
pub trait TypeInferencer: Send + Sync {
    async fn infer_type(
        &self,
        columns: &[ColumnMetadata],
        computed_types: &ComputedTypes,
        mapping: &TransformationMetadata,
        introspector: &Arc<dyn SchemaIntrospector>,
        source_dialect: Dialect,
    ) -> Option<(Type, Option<usize>)>;
}

impl TypeEngine {
    pub fn new(
        introspector: Arc<dyn SchemaIntrospector>,
        type_registry: Arc<TypeRegistry>,
        source_dialect: Dialect,
    ) -> Self {
        Self {
            introspector,
            type_registry,
            source_dialect,
        }
    }

    /// Convert a column to the target database type using the type registry.
    pub fn convert_column(&self, col: &ColumnMetadata) -> (Type, Option<usize>) {
        let canonical = self.source_dialect.to_canonical(col);
        let target_type = self.type_registry.convert(&canonical).target_type();
        (target_type, col.char_max_length)
    }

    /// Extract enum columns from table metadata.
    pub fn extract_enums(&self, meta: &TableMetadata) -> Vec<ColumnMetadata> {
        TableMetadata::enums(meta)
    }

    pub fn source_dialect(&self) -> Dialect {
        self.source_dialect
    }

    /// Normalize a generated column expression from the source dialect's syntax to plain SQL.
    pub fn normalize_generated_expression(&self, expr: &str) -> String {
        self.source_dialect.normalize_generated_expression(expr)
    }

    pub fn type_registry(&self) -> &TypeRegistry {
        &self.type_registry
    }

    pub async fn infer_computed_type(
        &self,
        computed: &ComputedField,
        columns: &[ColumnMetadata],
        computed_types: &ComputedTypes,
        mapping: &TransformationMetadata,
    ) -> Option<(Type, Option<usize>)> {
        // Clone the expression node into wrapper and run inference.
        let expr = ExpressionWrapper(computed.expression.clone());
        expr.infer_type(
            columns,
            computed_types,
            mapping,
            &self.introspector,
            self.source_dialect,
        )
        .await
    }
}

#[async_trait]
impl TypeInferencer for ExpressionWrapper {
    /// Inspect the wrapped `CompiledExpression` and produce a canonical `Type`.
    async fn infer_type(
        &self,
        columns: &[ColumnMetadata],
        computed_types: &ComputedTypes,
        mapping: &TransformationMetadata,
        introspector: &Arc<dyn SchemaIntrospector>,
        source_dialect: Dialect,
    ) -> Option<(Type, Option<usize>)> {
        match &self.0 {
            CompiledExpression::Identifier(identifier) => computed_types
                .get(&identifier.to_ascii_lowercase())
                .cloned()
                .or_else(|| {
                    columns
                        .iter()
                        .find(|col| col.name.eq_ignore_ascii_case(identifier))
                        .map(|col| (source_dialect.to_canonical(col), col.char_max_length))
                }),

            CompiledExpression::Literal(value) => Some(match value {
                Value::String(_) => (Type::Text { charset: None }, None),
                Value::Int(_) => (
                    Type::Int {
                        bits: IntSize::I64,
                        unsigned: false,
                        auto_increment: false,
                    },
                    None,
                ),
                Value::UInt(_) => (
                    Type::Int {
                        bits: IntSize::I64,
                        unsigned: true,
                        auto_increment: false,
                    },
                    None,
                ),
                Value::Float(_) => (
                    Type::Float {
                        bits: FloatSize::F64,
                    },
                    None,
                ),
                Value::Decimal(_) => (
                    Type::Decimal {
                        precision: None,
                        scale: None,
                    },
                    None,
                ),
                Value::Boolean(_) => (Type::Boolean, None),
                Value::Null => (Type::Text { charset: None }, None), // Default for null
                Value::Date(_) => (Type::Date, None),
                Value::Time { .. } => (
                    Type::Time {
                        precision: None,
                        with_tz: false,
                    },
                    None,
                ),
                Value::Timestamp { .. } => (
                    Type::Timestamp {
                        precision: None,
                        with_tz: false,
                    },
                    None,
                ),
                Value::Uuid(_) => (Type::Uuid, None),
                Value::Json(_) => (Type::Json { binary: false }, None),
                _ => (Type::Text { charset: None }, None), // Fallback for other types
            }),

            CompiledExpression::Binary { left, right, .. } => {
                let lt = ExpressionWrapper((**left).clone())
                    .infer_type(
                        columns,
                        computed_types,
                        mapping,
                        introspector,
                        source_dialect,
                    )
                    .await?;
                let rt = ExpressionWrapper((**right).clone())
                    .infer_type(
                        columns,
                        computed_types,
                        mapping,
                        introspector,
                        source_dialect,
                    )
                    .await?;
                Some(get_numeric_type(&lt.0, &rt.0))
            }

            CompiledExpression::FunctionCall { name, .. } => {
                match name.to_ascii_lowercase().as_str() {
                    "lower" | "upper" | "concat" => Some((
                        Type::Varchar {
                            length: None,
                            charset: None,
                        },
                        None,
                    )),
                    "env" => Some((
                        Type::Varchar {
                            length: None,
                            charset: None,
                        },
                        None,
                    )),
                    _ => None,
                }
            }

            // DotPath with 2+ segments = cross-entity reference (table.column)
            CompiledExpression::DotPath(segments) if segments.len() >= 2 => {
                let entity = &segments[0];
                let key = &segments[1];
                let table_name = mapping.entities.reverse_resolve(entity);

                // For DotPath, the entity name IS the source table name, not a destination
                let meta = introspector.table_metadata(&table_name).await.ok()?;
                meta.columns()
                    .iter()
                    .find(|col| col.name.eq_ignore_ascii_case(key))
                    .map(|col| (source_dialect.to_canonical(col), col.char_max_length))
            }

            // Single-segment DotPath is just a field reference
            CompiledExpression::DotPath(segments) if segments.len() == 1 => computed_types
                .get(&segments[0].to_ascii_lowercase())
                .cloned()
                .or_else(|| {
                    columns
                        .iter()
                        .find(|col| col.name.eq_ignore_ascii_case(&segments[0]))
                        .map(|col| (source_dialect.to_canonical(col), col.char_max_length))
                }),

            // Handle other expression types
            CompiledExpression::Unary { operand, .. } => {
                ExpressionWrapper((**operand).clone())
                    .infer_type(
                        columns,
                        computed_types,
                        mapping,
                        introspector,
                        source_dialect,
                    )
                    .await
            }

            CompiledExpression::Grouped(expr) => {
                ExpressionWrapper((**expr).clone())
                    .infer_type(
                        columns,
                        computed_types,
                        mapping,
                        introspector,
                        source_dialect,
                    )
                    .await
            }

            CompiledExpression::When {
                branches,
                else_expr,
            } => {
                // Try to infer from first branch value, fallback to else
                if let Some(branch) = branches.first() {
                    ExpressionWrapper(branch.value.clone())
                        .infer_type(
                            columns,
                            computed_types,
                            mapping,
                            introspector,
                            source_dialect,
                        )
                        .await
                } else if let Some(else_val) = else_expr {
                    ExpressionWrapper((**else_val).clone())
                        .infer_type(
                            columns,
                            computed_types,
                            mapping,
                            introspector,
                            source_dialect,
                        )
                        .await
                } else {
                    None
                }
            }

            CompiledExpression::IsNull(_) | CompiledExpression::IsNotNull(_) => {
                Some((Type::Boolean, None))
            }

            CompiledExpression::Array(_) => None, // Arrays not yet supported
            CompiledExpression::DotPath(_) => None, // Empty DotPath
        }
    }
}

fn get_numeric_type(left: &Type, right: &Type) -> (Type, Option<usize>) {
    let default_int = Type::Int {
        bits: IntSize::I64,
        unsigned: false,
        auto_increment: false,
    };
    let default_float = Type::Float {
        bits: FloatSize::F64,
    };
    let default_decimal = Type::Decimal {
        precision: None,
        scale: None,
    };

    match (left, right) {
        // Integer + Integer = Integer
        (Type::Int { .. }, Type::Int { .. }) => (default_int, None),

        // Float + Float = Float
        (Type::Float { .. }, Type::Float { .. }) => (default_float, None),

        // Integer + Float = Float
        (Type::Int { .. }, Type::Float { .. }) | (Type::Float { .. }, Type::Int { .. }) => {
            (default_float, None)
        }

        // Decimal + Decimal = Decimal
        (Type::Decimal { .. }, Type::Decimal { .. }) => (default_decimal.clone(), None),

        // Integer + Decimal = Decimal
        (Type::Int { .. }, Type::Decimal { .. }) | (Type::Decimal { .. }, Type::Int { .. }) => {
            (default_decimal.clone(), None)
        }

        // Float + Decimal = Decimal
        (Type::Float { .. }, Type::Decimal { .. }) | (Type::Decimal { .. }, Type::Float { .. }) => {
            (default_decimal, None)
        }

        _ => {
            warn!(left = ?left, right = ?right, "incompatible types for arithmetic operation");
            (Type::Text { charset: None }, None) // Fallback to Text for unsupported types
        }
    }
}
