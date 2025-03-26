use crate::{
    adapter::SqlAdapter,
    metadata::{
        column::metadata::ColumnMetadata, provider::MetadataProvider, table::TableMetadata,
    },
    query::builder::SqlQueryBuilder,
};
use std::collections::HashSet;

pub struct SchemaPlan {
    pub metadata: TableMetadata,
    pub create_table_queries: HashSet<String>,
    pub constraint_queries: HashSet<String>,
    pub enum_queries: HashSet<String>,
}

impl SchemaPlan {
    pub async fn build<F, T>(
        adaper: &(dyn SqlAdapter + Send + Sync),
        metadata: TableMetadata,
        type_converter: &F,
        custom_type_extractor: &T,
    ) -> Result<Self, Box<dyn std::error::Error>>
    where
        F: Fn(&ColumnMetadata) -> (String, Option<usize>),
        T: Fn(&TableMetadata) -> Vec<&ColumnMetadata>,
    {
        let (tbl_queries, const_queries, enum_declarations) =
            MetadataProvider::collect_schema_deps(&metadata, type_converter, custom_type_extractor);

        let mut enum_queries = HashSet::new();
        for enum_declaration in enum_declarations {
            let enum_type = adaper
                .fetch_column_type(&enum_declaration.0, &enum_declaration.1)
                .await?;

            enum_queries.insert(
                SqlQueryBuilder::new()
                    .create_enum(&enum_declaration.1, &Self::parse_enum(&enum_type))
                    .build()
                    .0,
            );
        }

        Ok(SchemaPlan {
            metadata,
            create_table_queries: tbl_queries,
            constraint_queries: const_queries,
            enum_queries,
        })
    }

    pub fn table_name(&self) -> &str {
        &self.metadata.name
    }

    fn parse_enum(raw: &str) -> Vec<String> {
        let start = raw.find('(').unwrap_or(0) + 1;
        let end = raw.rfind(')').unwrap_or(raw.len());

        raw[start..end]
            .split(',')
            .map(|s| s.trim().trim_matches('\'').to_string())
            .collect()
    }
}
