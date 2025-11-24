use crate::{connectors::source::DataSource, schema::types::TypeEngine};
use connectors::{
    metadata::{entity::EntityMetadata, field::FieldMetadata},
    sql::base::{
        error::DbError,
        metadata::table::TableMetadata,
        query::{column::ColumnDef, fk::ForeignKeyDef, generator::QueryGenerator},
    },
};
use model::transform::mapping::EntityMapping;
use planner::query::dialect;
use std::collections::{HashMap, HashSet};
use tracing::warn;

/// Represents the schema migration plan from source to target, including type conversion,
/// name mapping, and metadata relationships.
pub struct SchemaPlan {
    source: DataSource,

    /// Type engine for converting types between source and target databases.
    type_engine: TypeEngine,

    /// Indicates whether to ignore constraints during the migration process.
    /// Foreign keys are not created in the target database.
    ignore_constraints: bool,

    /// Indicates whether to create columns in the target table that are present in the mapping block only.
    mapped_columns_only: bool,

    /// Mapping of table names from source to target database.
    mapping: EntityMapping,

    /// Metadata graph containing all source tables and their relationships
    /// (both referencing and referenced dependencies).
    metadata_graph: HashMap<String, EntityMetadata>,

    /// Definitions of columns collected for each table, used later for generating `CREATE TABLE` queries.
    column_definitions: HashMap<String, Vec<ColumnDef>>,

    /// Definitions of enum types collected for each table.
    enum_definitions: HashSet<(String, String)>,

    /// Foreign key definitions collected for each table.
    fk_definitions: HashMap<String, Vec<ForeignKeyDef>>,
}

impl SchemaPlan {
    pub fn new(
        source: DataSource,
        type_engine: TypeEngine,
        ignore_constraints: bool,
        mapped_columns_only: bool,
        mapping: EntityMapping,
    ) -> Self {
        Self {
            source,
            type_engine,
            ignore_constraints,
            mapped_columns_only,
            mapping,
            metadata_graph: HashMap::new(),
            column_definitions: HashMap::new(),
            enum_definitions: HashSet::new(),
            fk_definitions: HashMap::new(),
        }
    }

    pub fn type_engine(&self) -> &TypeEngine {
        &self.type_engine
    }

    pub async fn table_queries(&self) -> HashSet<(String, String)> {
        let mut queries = HashSet::new();

        for (table, columns) in &self.column_definitions {
            let resolved_table = self.mapping.entity_name_map.resolve(table);
            let mut resolved_columns = self.resolve_column_definitions(table, columns);

            // Optionally drop unmapped columns
            if self.mapped_columns_only {
                resolved_columns =
                    self.filter_to_mapped_columns(&resolved_table, resolved_columns.clone());
            }

            // Always append computed columns
            resolved_columns.extend(self.computed_column_definitions(table).await);

            let (sql, _) = QueryGenerator::new(&dialect::Postgres).create_table(
                &resolved_table,
                &resolved_columns,
                self.ignore_constraints,
                false,
            );

            queries.insert((sql, resolved_table));
        }

        queries
    }

    pub fn fk_queries(&self) -> HashSet<(String, String)> {
        if self.ignore_constraints {
            return HashSet::new();
        }

        self.fk_definitions
            .iter()
            .flat_map(|(table, fks)| {
                let resolved_table = self.mapping.entity_name_map.resolve(table);
                fks.iter().map(move |fk| {
                    let ref_table = self.mapping.entity_name_map.resolve(&fk.referenced_table);
                    let ref_column = self
                        .mapping
                        .field_mappings
                        .resolve(&ref_table, &fk.referenced_column);

                    let resolved_fk = ForeignKeyDef {
                        referenced_table: ref_table,
                        referenced_column: ref_column,
                        column: self
                            .mapping
                            .field_mappings
                            .resolve(&resolved_table, &fk.column),
                    };

                    let (sql, _) = QueryGenerator::new(&dialect::Postgres)
                        .add_foreign_key(&resolved_table, &resolved_fk);
                    (sql, fk.column.clone())
                })
            })
            .collect()
    }

    pub async fn enum_queries(&self) -> Result<HashSet<(String, String)>, DbError> {
        let mut queries = HashSet::new();

        for (table, column) in &self.enum_definitions {
            let adapter = match self.source {
                DataSource::Database(ref db) => db.lock().await.adapter(),
                _ => panic!("Enum queries are only supported for SQL data sources"),
            };

            let enum_type = adapter.column_db_type(table, column).await?;
            let variants = Self::parse_enum(&enum_type);
            let (sql, _) = QueryGenerator::new(&dialect::Postgres).create_enum(column, &variants);

            queries.insert((sql, column.clone()));
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

    pub fn add_fk_def(&mut self, table_name: &str, fk_def: ForeignKeyDef) {
        self.fk_definitions
            .entry(table_name.to_string())
            .or_default()
            .push(fk_def);
    }

    pub fn add_metadata(&mut self, table_name: &str, metadata: EntityMetadata) {
        self.metadata_graph.insert(table_name.to_string(), metadata);
    }

    pub fn metadata_exists(&self, table_name: &str) -> bool {
        self.metadata_graph.contains_key(table_name)
    }

    pub fn collect_schema_deps(metadata: &TableMetadata, plan: &mut SchemaPlan) {
        let mut visited = HashSet::new();
        Self::visit_schema_deps(metadata, plan, &mut visited);
    }

    /// Build a vector of ColumnDef from EntityMetadata, sorted by ordinal,
    /// filtering out invalid fields and using the type engine for conversion.
    pub fn column_defs(&self, meta: &EntityMetadata) -> Vec<ColumnDef> {
        // Filter only valid fields
        let mut valid_cols: Vec<_> = meta
            .columns()
            .into_iter()
            .filter(FieldMetadata::is_valid)
            .collect();

        // Sort by ordinal for stable ordering
        valid_cols.sort_by_key(|col| col.ordinal());

        // Grab the converter
        let convert = self.type_engine().type_converter();

        // Map into ColumnDef
        valid_cols
            .into_iter()
            .map(|col| {
                let (data_type, char_max_length) = convert(&col);
                ColumnDef {
                    name: col.name(),
                    data_type,
                    is_nullable: col.is_nullable(),
                    is_primary_key: col.is_primary_key(),
                    default: col.default_value(),
                    char_max_length,
                }
            })
            .collect()
    }

    fn resolve_column_definitions(&self, table: &str, columns: &[ColumnDef]) -> Vec<ColumnDef> {
        let resolved_table = self.mapping.entity_name_map.resolve(table);
        columns
            .iter()
            .map(|col| ColumnDef {
                name: self
                    .mapping
                    .field_mappings
                    .resolve(&resolved_table, &col.name),
                ..col.clone()
            })
            .collect()
    }

    async fn computed_column_definitions(&self, table: &str) -> Vec<ColumnDef> {
        let mut defs = Vec::new();

        let resolved_table = self.mapping.entity_name_map.resolve(table);
        let computed_fields = match self.mapping.field_mappings.get_computed(&resolved_table) {
            Some(fields) => fields,
            None => return defs,
        };

        let metadata = match self.metadata_graph.get(table) {
            Some(m) => m,
            None => {
                warn!("Missing metadata for table: {}", table);
                return defs;
            }
        };

        for computed in computed_fields {
            let column_name = &computed.name;
            if metadata.column(column_name).is_some() {
                warn!(
                    "Computed field {} conflicts with existing column",
                    column_name
                );
                warn!("Skipping computed field {}", column_name);
                // TODO: add to documentation
                warn!(
                    "If CopyColumns=MapOnly and the name of the computed field matches an existing column, the computed field will be ignored."
                );
                continue;
            }

            if let Some(inferred_type) = self
                .type_engine
                .infer_computed_type(computed, &metadata.columns(), &self.mapping)
                .await
            {
                defs.push(ColumnDef {
                    name: (*column_name).clone(),
                    is_nullable: true, // Assuming computed fields are nullable
                    default: None,
                    data_type: inferred_type,
                    is_primary_key: false,
                    char_max_length: None,
                });
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

    fn filter_to_mapped_columns(&self, table: &str, columns: Vec<ColumnDef>) -> Vec<ColumnDef> {
        let mapping = &self
            .mapping
            .field_mappings
            .column_mappings
            .get(table)
            .expect("Mapping must exist for table");
        columns
            .into_iter()
            .filter(|col| mapping.contains_target_key(&col.name))
            .collect()
    }

    fn visit_schema_deps(
        metadata: &TableMetadata,
        plan: &mut SchemaPlan,
        visited: &mut HashSet<String>,
    ) {
        if !visited.insert(metadata.name.clone()) || plan.metadata_exists(&metadata.name) {
            return;
        }

        metadata
            .referenced_tables
            .values()
            .chain(metadata.referencing_tables.values())
            .for_each(|related| {
                Self::visit_schema_deps(related, plan, visited);
            });

        plan.add_column_defs(
            &metadata.name,
            plan.column_defs(&EntityMetadata::Table(metadata.clone())),
        );
        plan.add_fk_defs(&metadata.name, metadata.fk_defs());

        for col in (plan.type_engine().enum_extractor())(metadata) {
            plan.add_enum_def(&metadata.name, &col.name);
        }
    }
}
