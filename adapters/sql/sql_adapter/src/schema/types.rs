use crate::{
    adapter::SqlAdapter,
    metadata::{
        column::{data_type::ColumnDataType, metadata::ColumnMetadata},
        table::TableMetadata,
    },
};
use async_trait::async_trait;
use common::{computed::ComputedField, mapping::EntityMapping};
use std::{future::Future, pin::Pin, sync::Arc};

// Alias for the SQL adapter reference
pub type AdapterRef = Arc<dyn SqlAdapter + Send + Sync>;

/// A function that converts a source database type to a target database type,
/// returning the target type name and optional size (e.g., MySQL `blob` → PostgreSQL `bytea`).
pub type TypeConverter = dyn Fn(&ColumnMetadata) -> (String, Option<usize>) + Send + Sync;

/// A function that extracts custom types (such as enums) from a table’s metadata.
pub type TypeExtractor = dyn Fn(&TableMetadata) -> Vec<ColumnMetadata> + Send + Sync;

/// A function that infers the type of computed fields based on the source database metadata.
pub type InferComputedTypeFn =
    for<'a> fn(
        &'a ComputedField,
        &'a [ColumnMetadata],
        &'a EntityMapping,
        &'a AdapterRef,
    ) -> Pin<Box<dyn Future<Output = Option<ColumnDataType>> + Send + 'a>>;

pub struct TypeEngine<'a> {
    /// Adapter for the source database; used to read metadata.
    adapter: Arc<dyn SqlAdapter + Send + Sync>,

    /// Function used to convert column types from source to target database format.
    type_converter: &'a TypeConverter,

    /// Function used to extract custom types such as enums from table metadata.
    type_extractor: &'a TypeExtractor,

    /// Function used to infer the type of computed fields.
    type_inferencer: InferComputedTypeFn,
}

#[async_trait]
pub trait TypeInferencer {
    async fn infer_type(
        &self,
        columns: &[ColumnMetadata],
        mapping: &EntityMapping,
        adapter: &AdapterRef,
    ) -> Option<ColumnDataType>;
}

impl<'a> TypeEngine<'a> {
    pub fn new(
        adapter: Arc<dyn SqlAdapter + Send + Sync>,
        type_converter: &'a TypeConverter,
        type_extractor: &'a TypeExtractor,
        type_inferencer: InferComputedTypeFn,
    ) -> Self {
        Self {
            adapter,
            type_converter,
            type_extractor,
            type_inferencer,
        }
    }

    pub async fn infer_type<E: TypeInferencer>(
        &self,
        expr: &E,
        columns: &[ColumnMetadata],
        mapping: &EntityMapping,
    ) -> Option<ColumnDataType> {
        E::infer_type(&expr, columns, mapping, &self.adapter).await
    }

    pub fn type_converter(&self) -> &TypeConverter {
        self.type_converter
    }

    pub fn type_extractor(&self) -> &TypeExtractor {
        self.type_extractor
    }

    pub async fn infer_computed_type(
        &self,
        computed: &ComputedField,
        columns: &[ColumnMetadata],
        mapping: &EntityMapping,
    ) -> Option<ColumnDataType> {
        (self.type_inferencer)(computed, columns, mapping, &self.adapter).await
    }
}
