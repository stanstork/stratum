use crate::{
    adapter::SqlAdapter,
    metadata::{column::metadata::ColumnMetadata, table::TableMetadata},
    query::{builder::SqlQueryBuilder, column::ColumnDef, fk::ForeignKeyDef},
};
use common::mapping::{NameMap, NamespaceMap};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug)]
pub struct SchemaContext<'a, F, T>
where
    F: Fn(&ColumnMetadata) -> (String, Option<usize>),
    T: Fn(&TableMetadata) -> Vec<&ColumnMetadata>,
{
    pub type_converter: &'a F,
    pub type_extractor: &'a T,

    pub column_name_map: NamespaceMap,
    pub table_name_map: NameMap,

    pub column_defs: HashMap<String, Vec<ColumnDef>>,
    pub enum_defs: HashSet<(String, String)>,
    pub fk_defs: HashMap<String, Vec<ForeignKeyDef>>,
}

impl<'a, F, T> SchemaContext<'a, F, T>
where
    F: Fn(&ColumnMetadata) -> (String, Option<usize>),
    T: Fn(&TableMetadata) -> Vec<&ColumnMetadata>,
{
    pub fn new(
        type_converter: &'a F,
        type_extractor: &'a T,
        table_name_map: NameMap,
        column_name_map: NamespaceMap,
    ) -> Self {
        Self {
            type_converter,
            type_extractor,
            table_name_map,
            column_name_map,
            column_defs: HashMap::new(),
            enum_defs: HashSet::new(),
            fk_defs: HashMap::new(),
        }
    }

    pub fn table_queries(&self) -> HashSet<String> {
        self.column_defs
            .iter()
            .map(|(table, columns)| {
                let resolved_table = self.table_name_map.resolve(table);
                let resolved_columns = columns
                    .iter()
                    .map(|col| ColumnDef {
                        name: self.column_name_map.resolve(&resolved_table, &col.name),
                        ..col.clone()
                    })
                    .collect::<Vec<_>>();

                SqlQueryBuilder::new()
                    .create_table(&resolved_table, &resolved_columns, &[])
                    .build()
                    .0
            })
            .collect()
    }

    pub fn fk_queries(&self) -> HashSet<String> {
        self.fk_defs
            .iter()
            .flat_map(|(table, fks)| {
                let resolved_table = self.table_name_map.resolve(table);
                fks.iter().map(move |fk| {
                    let ref_table = self.table_name_map.resolve(&fk.referenced_table);
                    let ref_column = self
                        .column_name_map
                        .resolve(&ref_table, &fk.referenced_column);

                    let resolved_fk = ForeignKeyDef {
                        referenced_table: ref_table,
                        referenced_column: ref_column,
                        column: self.column_name_map.resolve(&resolved_table, &fk.column),
                        ..fk.clone()
                    };

                    SqlQueryBuilder::new()
                        .add_foreign_key(&resolved_table, &resolved_fk)
                        .build()
                        .0
                })
            })
            .collect()
    }

    pub async fn enum_queries(
        &self,
        adapter: &(dyn SqlAdapter + Send + Sync),
    ) -> Result<HashSet<String>, Box<dyn std::error::Error>> {
        let mut queries = HashSet::new();

        for (table, column) in &self.enum_defs {
            let enum_type = adapter.fetch_column_type(table, column).await?;
            let variants = Self::parse_enum(&enum_type);

            let enum_sql = SqlQueryBuilder::new()
                .create_enum(column, &variants)
                .build()
                .0;

            queries.insert(enum_sql);
        }

        Ok(queries)
    }

    pub fn add_column_defs(&mut self, table_name: &str, column_defs: Vec<ColumnDef>) {
        self.column_defs
            .entry(table_name.to_string())
            .or_insert_with(Vec::new)
            .extend(column_defs);
    }

    pub fn add_enum_def(&mut self, table_name: &str, column_name: &str) {
        self.enum_defs
            .insert((table_name.to_string(), column_name.to_string()));
    }

    pub fn add_fk_defs(&mut self, table_name: &str, fk_defs: Vec<ForeignKeyDef>) {
        self.fk_defs
            .entry(table_name.to_string())
            .or_insert_with(Vec::new)
            .extend(fk_defs);
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
