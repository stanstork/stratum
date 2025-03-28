use crate::{
    adapter::SqlAdapter,
    metadata::{column::metadata::ColumnMetadata, table::TableMetadata},
    query::builder::SqlQueryBuilder,
};
use std::collections::{HashMap, HashSet};

pub struct SchemaContext<'a, F, T>
where
    F: Fn(&ColumnMetadata) -> (String, Option<usize>),
    T: Fn(&TableMetadata) -> Vec<&ColumnMetadata>,
{
    pub type_converter: &'a F,
    pub type_extractor: &'a T,
    pub table_name_map: TableNameMap<'a>,

    pub table_queries: HashSet<String>,
    pub constraint_queries: HashSet<String>,
    pub enum_declarations: HashSet<(String, String)>,
}

impl<'a, F, T> SchemaContext<'a, F, T>
where
    F: Fn(&ColumnMetadata) -> (String, Option<usize>),
    T: Fn(&TableMetadata) -> Vec<&ColumnMetadata>,
{
    pub fn new(
        type_converter: &'a F,
        type_extractor: &'a T,
        table_name_map: TableNameMap<'a>,
    ) -> Self {
        Self {
            type_converter,
            type_extractor,
            table_name_map,
            table_queries: HashSet::new(),
            constraint_queries: HashSet::new(),
            enum_declarations: HashSet::new(),
        }
    }

    pub fn table_queries(&self) -> HashSet<String> {
        self.table_queries.iter().map(|sql| sql.clone()).collect()
    }

    pub fn constraint_queries(&self) -> HashSet<String> {
        self.constraint_queries
            .iter()
            .map(|sql| sql.clone())
            .collect()
    }

    pub async fn enum_queries(
        &self,
        adapter: &(dyn SqlAdapter + Send + Sync),
    ) -> Result<HashSet<String>, Box<dyn std::error::Error>> {
        let mut enum_queries = HashSet::new();
        for enum_declaration in self.enum_declarations.iter() {
            let enum_type = adapter
                .fetch_column_type(&enum_declaration.0, &enum_declaration.1)
                .await?;

            let enum_sql = SqlQueryBuilder::new()
                .create_enum(&enum_declaration.1, &Self::parse_enum(&enum_type))
                .build();
            enum_queries.insert(enum_sql.0);
        }
        Ok(enum_queries)
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

pub struct TableNameMap<'a> {
    map: &'a HashMap<String, String>,
}

impl<'a> TableNameMap<'a> {
    pub fn new(map: &'a HashMap<String, String>) -> Self {
        Self { map }
    }

    pub fn resolve(&self, name: &str) -> String {
        self.map
            .get(name)
            .cloned()
            .unwrap_or_else(|| name.to_string())
    }
}
