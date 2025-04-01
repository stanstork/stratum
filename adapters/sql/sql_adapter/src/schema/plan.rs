use super::context::SchemaContext;
use crate::{
    adapter::SqlAdapter,
    metadata::{
        column::metadata::ColumnMetadata, provider::MetadataProvider, table::TableMetadata,
    },
};
use common::mapping::{NameMap, NamespaceMap};
use std::collections::HashSet;

#[derive(Clone, Debug)]
pub struct SchemaPlan {
    pub metadata: TableMetadata,
    pub table_queries: HashSet<String>,
    pub constraint_queries: HashSet<String>,
    pub enum_queries: HashSet<String>,
}

impl SchemaPlan {
    pub async fn build<F, T>(
        adapter: &(dyn SqlAdapter + Send + Sync),
        metadata: TableMetadata,
        table_name_map: NameMap,
        column_name_map: NamespaceMap,
        type_converter: &F,
        type_extractor: &T,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        F: Fn(&ColumnMetadata) -> (String, Option<usize>),
        T: Fn(&TableMetadata) -> Vec<&ColumnMetadata>,
    {
        let mut ctx = SchemaContext::new(
            type_converter,
            type_extractor,
            table_name_map,
            column_name_map,
        );

        MetadataProvider::collect_schema_deps(&metadata, &mut ctx);

        Ok(SchemaPlan {
            metadata,
            table_queries: ctx.table_queries(),
            constraint_queries: ctx.fk_queries(),
            enum_queries: ctx.enum_queries(adapter).await.unwrap_or_default(),
        })
    }

    pub fn table_name(&self) -> &str {
        &self.metadata.name
    }
}
