use super::context::{SchemaContext, TableNameMap};
use crate::{
    adapter::SqlAdapter,
    metadata::{
        column::metadata::ColumnMetadata, provider::MetadataProvider, table::TableMetadata,
    },
};
use std::collections::{HashMap, HashSet};

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
        table_name_map: &HashMap<String, String>,
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
            TableNameMap::new(table_name_map),
        );

        MetadataProvider::collect_schema_deps(&metadata, &mut ctx);

        Ok(SchemaPlan {
            metadata,
            table_queries: ctx.table_queries(),
            constraint_queries: ctx.constraint_queries(),
            enum_queries: ctx.enum_queries(adapter).await.unwrap_or_default(),
        })
    }

    pub fn table_name(&self) -> &str {
        &self.metadata.name
    }
}
