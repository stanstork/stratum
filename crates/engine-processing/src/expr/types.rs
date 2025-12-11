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
        // Check if this is a cross-entity reference (DotPath with 2+ segments)
        // If so, handle it here with async metadata fetching
        if let CompiledExpression::DotPath(segments) = &self.0 {
            if segments.len() >= 2 {
                let entity = &segments[0];
                let key = &segments[1];
                let table_name = mapping.entities.resolve(entity);
                let meta = source.fetch_meta(table_name).await.ok()?;
                return match meta {
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
                };
            }
        }

        // For all other cases, delegate to expression-engine's synchronous inference
        let column_lookup = |name: &str| {
            columns
                .iter()
                .find(|col| col.name().eq_ignore_ascii_case(name))
                .map(|col| col.data_type())
        };

        expression_engine::infer_expression_type(&self.0, &column_lookup)
    }
}
