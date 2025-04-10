use crate::{
    adapter::SqlAdapter,
    metadata::{column::metadata::ColumnMetadata, table::TableMetadata},
    query::{builder::SqlQueryBuilder, column::ColumnDef, fk::ForeignKeyDef},
};
use common::mapping::{FieldNameMap, ScopedNameMap};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use super::types::TypeInferencer;

/// A function that converts a source database type to a target database type,
/// returning the target type name and optional size (e.g., MySQL `blob` → PostgreSQL `bytea`).
pub type TypeConverter = dyn Fn(&ColumnMetadata) -> (String, Option<usize>) + Send + Sync;

/// A function that extracts custom types (such as enums) from a table’s metadata.
pub type TypeExtractor = dyn Fn(&TableMetadata) -> Vec<ColumnMetadata> + Send + Sync;

/// Represents the schema migration plan from source to target, including type conversion,
/// name mapping, and metadata relationships.
pub struct SchemaPlan<'a> {
    /// Adapter for the source database; used to read metadata.
    source_adapter: Arc<(dyn SqlAdapter + Send + Sync)>,

    /// Function used to convert column types from source to target database format.
    type_converter: &'a TypeConverter,

    /// Function used to extract custom types such as enums from table metadata.
    type_extractor: &'a TypeExtractor,

    /// Custom column name mapping provided by the user (e.g., source → target column names).
    column_name_map: ScopedNameMap,

    /// Custom table name mapping provided by the user (e.g., source → target table names).
    table_name_map: FieldNameMap,

    /// Metadata graph containing all source tables and their relationships
    /// (both referencing and referenced dependencies).
    metadata_graph: HashMap<String, TableMetadata>,

    /// Definitions of columns collected for each table, used later for generating `CREATE TABLE` queries.
    column_definitions: HashMap<String, Vec<ColumnDef>>,

    /// Definitions of enum types collected for each table.
    enum_definitions: HashSet<(String, String)>,

    /// Foreign key definitions collected for each table.
    fk_definitions: HashMap<String, Vec<ForeignKeyDef>>,
}

impl<'a> SchemaPlan<'a> {
    pub fn new(
        source_adapter: Arc<(dyn SqlAdapter + Send + Sync)>,
        type_converter: &'a TypeConverter,
        type_extractor: &'a TypeExtractor,
        table_name_map: FieldNameMap,
        column_name_map: ScopedNameMap,
    ) -> Self {
        Self {
            source_adapter,
            type_converter,
            type_extractor,
            column_name_map,
            table_name_map,
            metadata_graph: HashMap::new(),
            column_definitions: HashMap::new(),
            enum_definitions: HashSet::new(),
            fk_definitions: HashMap::new(),
        }
    }

    pub fn table_queries(&self) -> HashSet<String> {
        self.column_definitions
            .iter()
            .map(|(table, columns)| {
                let resolved_table = self.table_name_map.resolve(table);

                let mut resolved_columns = self.resolve_column_definitions(table, columns);
                resolved_columns.extend(self.computed_column_definitions(&table));

                SqlQueryBuilder::new()
                    .create_table(&resolved_table, &resolved_columns, &[])
                    .build()
                    .0
            })
            .collect()
    }

    pub fn fk_queries(&self) -> HashSet<String> {
        self.fk_definitions
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

    pub async fn enum_queries(&self) -> Result<HashSet<String>, Box<dyn std::error::Error>> {
        let mut queries = HashSet::new();

        for (table, column) in &self.enum_definitions {
            let enum_type = self.source_adapter.fetch_column_type(table, column).await?;
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
        self.column_definitions
            .insert(table_name.to_string(), column_defs);
    }

    pub fn add_enum_def(&mut self, table_name: &str, column_name: &str) {
        self.enum_definitions
            .insert((table_name.to_string(), column_name.to_string()));
    }

    pub fn add_fk_defs(&mut self, table_name: &str, fk_defs: Vec<ForeignKeyDef>) {
        self.fk_definitions.insert(table_name.to_string(), fk_defs);
    }

    pub fn add_metadata(&mut self, table_name: &str, metadata: TableMetadata) {
        self.metadata_graph.insert(table_name.to_string(), metadata);
    }

    pub fn metadata_exists(&self, table_name: &str) -> bool {
        self.metadata_graph.contains_key(table_name)
    }

    pub fn type_converter(&self) -> &TypeConverter {
        self.type_converter
    }

    pub fn type_extractor(&self) -> &TypeExtractor {
        self.type_extractor
    }

    fn resolve_column_definitions(&self, table: &str, columns: &[ColumnDef]) -> Vec<ColumnDef> {
        let resolved_table = self.table_name_map.resolve(table);
        columns
            .iter()
            .map(|col| ColumnDef {
                name: self.column_name_map.resolve(&resolved_table, &col.name),
                ..col.clone()
            })
            .collect()
    }

    fn computed_column_definitions(&self, table: &str) -> Vec<ColumnDef> {
        let mut defs = Vec::new();

        let resolved_table = self.table_name_map.resolve(table);
        let computed_fields = match self.column_name_map.get_computed(&resolved_table) {
            Some(fields) => fields,
            None => return defs,
        };

        let metadata = match self.metadata_graph.get(table) {
            Some(m) => m,
            None => {
                eprintln!("Missing metadata for table: {}", table);
                return defs;
            }
        };

        for computed in computed_fields {
            let column_name = &computed.name;
            if metadata.get_column(column_name).is_some() {
                continue;
            }

            if let Some(inferred_type) = computed.expression.infer_type(&metadata.columns()) {
                defs.push(ColumnDef {
                    name: column_name.clone(),
                    is_nullable: true, // Assuming computed fields are nullable
                    default: None,
                    data_type: inferred_type.to_string(),
                    is_primary_key: false,
                    char_max_length: None,
                });
            } else {
                eprintln!(
                    "Warning: Could not infer type for computed field `{}` in table `{}`",
                    column_name, table
                );
            }
        }

        defs
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
