use crate::{
    expr::types::ExpressionWrapper, metadata::field::FieldMetadata, source::data::DataSource,
};
use async_trait::async_trait;
use common::{computed::ComputedField, mapping::EntityMapping, types::DataType};
use smql::statements::expr::Expression;
use sql_adapter::{
    adapter::SqlAdapter,
    metadata::{column::ColumnMetadata, table::TableMetadata},
};
use std::sync::Arc;

// Alias for the SQL adapter reference
pub type AdapterRef = Arc<dyn SqlAdapter + Send + Sync>;

/// A function that converts a source database type to a target database type,
/// returning the target type name and optional size (e.g., MySQL `blob` → PostgreSQL `bytea`).
pub type TypeConverter = dyn Fn(&FieldMetadata) -> (DataType, Option<usize>) + Send + Sync;

/// A function that extracts enums from a table’s metadata.
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
        mapping: &EntityMapping,
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
        mapping: &EntityMapping,
    ) -> Option<DataType> {
        // Clone the expression node into wrapper and run inference.
        let expr = ExpressionWrapper(computed.expression.clone());
        let data_type = expr.infer_type(columns, mapping, &self.source).await;

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
}
