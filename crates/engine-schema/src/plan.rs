use crate::{
    dep_graph::DependencyGraph,
    schema_ops::{SchemaOp, SchemaOps},
    types::TypeEngine,
};
use connectors::sql::{
    metadata::table::TableMetadata,
    query::{
        column::ColumnDef,
        constraint::{CheckConstraintDef, UniqueConstraintDef},
        fk::ForeignKeyDef,
        generator::QueryGenerator,
        index::IndexDef,
        sequence::SequenceDef,
    },
};
use model::{
    core::types::Type, execution::expr::CompiledExpression,
    transform::mapping::TransformationMetadata,
};
use query_builder::dialect::{self, Dialect};
use std::collections::{HashMap, HashSet};
use tracing::warn;

/// Represents the schema migration plan from source to target, including type conversion,
/// name mapping, and metadata relationships.
///
/// Supports multi-object collection (tables, enums, FKs, indexes, sequences) and
/// generates properly-ordered `SchemaOps` via `build_ops()`.
pub struct SchemaPlan {
    /// Type engine for converting types between source and target databases.
    type_engine: TypeEngine,

    /// Target dialect for DDL rendering.
    target_dialect: Box<dyn Dialect + Send + Sync>,

    /// Indicates whether to ignore constraints during the migration process.
    ignore_constraints: bool,

    /// Indicates whether to create columns in the target table that are present in the mapping block only.
    mapped_columns_only: bool,

    /// When true, emit DROP CONSTRAINT IF EXISTS ops in the pre-migration phase so
    /// that data is always written without FK constraints in place. FK constraints
    /// are re-added in the post-migration phase as usual.
    drop_constraints: bool,

    /// Index creation strategy.
    index_creation: IndexCreationStrategy,

    /// Foreign key creation strategy.
    fk_creation: FkCreationStrategy,

    /// Mapping of table names from source to target database.
    mapping: TransformationMetadata,

    /// Metadata graph containing all source tables and their relationships.
    metadata_graph: HashMap<String, TableMetadata>,

    /// Definitions of columns collected for each table.
    column_definitions: HashMap<String, Vec<ColumnDef>>,

    /// Definitions of enum types collected for each table.
    enum_definitions: HashSet<(String, String)>,

    /// Foreign key definitions collected for each table.
    fk_definitions: HashMap<String, Vec<ForeignKeyDef>>,

    /// Index definitions collected for each table.
    index_definitions: HashMap<String, Vec<IndexDef>>,

    /// Sequence definitions.
    sequence_definitions: Vec<SequenceDef>,

    /// UNIQUE constraint definitions collected for each table.
    unique_constraint_definitions: HashMap<String, Vec<UniqueConstraintDef>>,

    /// CHECK constraint definitions collected for each table.
    check_constraint_definitions: HashMap<String, Vec<CheckConstraintDef>>,
}

/// Strategy for when indexes are created relative to data migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum IndexCreationStrategy {
    /// Create indexes after data migration (default, better for large tables).
    #[default]
    AfterData,
    /// Create indexes before data migration.
    BeforeData,
}

/// Strategy for when foreign key constraints are created relative to data migration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FkCreationStrategy {
    /// Create FKs after data migration (default, avoids constraint violations during bulk insert).
    #[default]
    AfterData,
    /// Create FKs before data migration (useful for small tables or pre-validated data).
    BeforeData,
}

impl SchemaPlan {
    pub fn new(
        type_engine: TypeEngine,
        ignore_constraints: bool,
        mapped_columns_only: bool,
        mapping: TransformationMetadata,
    ) -> Self {
        Self {
            type_engine,
            target_dialect: Box::new(dialect::Postgres),
            ignore_constraints,
            mapped_columns_only,
            drop_constraints: false,
            index_creation: IndexCreationStrategy::default(),
            fk_creation: FkCreationStrategy::default(),
            mapping,
            metadata_graph: HashMap::new(),
            column_definitions: HashMap::new(),
            enum_definitions: HashSet::new(),
            fk_definitions: HashMap::new(),
            index_definitions: HashMap::new(),
            sequence_definitions: Vec::new(),
            unique_constraint_definitions: HashMap::new(),
            check_constraint_definitions: HashMap::new(),
        }
    }

    pub fn set_target_dialect(&mut self, dialect: Box<dyn Dialect + Send + Sync>) {
        self.target_dialect = dialect;
    }

    pub fn set_index_creation(&mut self, strategy: IndexCreationStrategy) {
        self.index_creation = strategy;
    }

    pub fn set_fk_creation(&mut self, strategy: FkCreationStrategy) {
        self.fk_creation = strategy;
    }

    pub fn set_drop_constraints(&mut self, drop: bool) {
        self.drop_constraints = drop;
    }

    pub fn type_engine(&self) -> &TypeEngine {
        &self.type_engine
    }

    pub fn add_column_defs(&mut self, table_name: &str, column_defs: Vec<ColumnDef>) {
        self.column_definitions
            .insert(table_name.to_string(), column_defs);
    }

    pub fn extend_column_defs(&mut self, table_name: &str, extra: Vec<ColumnDef>) {
        if !extra.is_empty() {
            self.column_definitions
                .entry(table_name.to_string())
                .or_default()
                .extend(extra);
        }
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

    pub fn add_index_defs(&mut self, table_name: &str, indexes: Vec<IndexDef>) {
        self.index_definitions
            .entry(table_name.to_string())
            .or_default()
            .extend(indexes);
    }

    pub fn add_sequence(&mut self, seq: SequenceDef) {
        self.sequence_definitions.push(seq);
    }

    pub fn add_unique_constraint_defs(&mut self, table_name: &str, defs: Vec<UniqueConstraintDef>) {
        self.unique_constraint_definitions
            .entry(table_name.to_string())
            .or_default()
            .extend(defs);
    }

    pub fn add_check_constraint_defs(&mut self, table_name: &str, defs: Vec<CheckConstraintDef>) {
        self.check_constraint_definitions
            .entry(table_name.to_string())
            .or_default()
            .extend(defs);
    }

    pub fn add_metadata(&mut self, table_name: &str, metadata: TableMetadata) {
        self.metadata_graph.insert(table_name.to_string(), metadata);
    }

    pub fn get_table_metadata(&self, table_name: &str) -> Option<&TableMetadata> {
        self.metadata_graph.get(table_name)
    }

    pub fn metadata_graph(&self) -> &HashMap<String, TableMetadata> {
        &self.metadata_graph
    }

    pub fn metadata_exists(&self, table_name: &str) -> bool {
        self.metadata_graph.contains_key(table_name)
    }

    /// Main entry point: produces ordered pre/post ops from all collected definitions.
    pub fn build_ops(&self) -> SchemaOps {
        let mut pre = Vec::new();
        let mut post = Vec::new();

        // Sequences must come after tables because OWNED BY references the table column.
        pre.extend(self.enum_ops());
        pre.extend(self.table_ops());
        pre.extend(self.sequence_ops());

        // When requested: drop existing FK constraints before data migration so that
        // a cascade run succeeds even if a prior schema_only run already created them.
        // FKs are re-added in the post phase as usual.
        if self.drop_constraints && !self.ignore_constraints {
            pre.extend(self.drop_fk_ops());
        }

        // Indexes: post-data by default, pre-data if configured
        match self.index_creation {
            IndexCreationStrategy::AfterData => post.extend(self.index_ops()),
            IndexCreationStrategy::BeforeData => pre.extend(self.index_ops()),
        }

        // FK constraints: post-data by default, pre-data if configured
        match self.fk_creation {
            FkCreationStrategy::AfterData => post.extend(self.constraint_ops()),
            FkCreationStrategy::BeforeData => pre.extend(self.constraint_ops()),
        }

        SchemaOps { pre, post }
    }

    /// Generate CREATE TYPE ... AS ENUM ops.
    fn enum_ops(&self) -> Vec<SchemaOp> {
        let qgen = QueryGenerator::new(self.target_dialect.as_ref());
        let mut ops = Vec::new();

        for (table, column) in &self.enum_definitions {
            // Prefer full_column_type (e.g. "enum('G','PG','PG-13','R','NC-17')")
            // over data_type (which is just "enum" from MySQL INFORMATION_SCHEMA.DATA_TYPE).
            let enum_type = self
                .metadata_graph
                .get(table)
                .and_then(|meta| meta.columns.get(column))
                .and_then(|col| {
                    col.full_column_type.clone().or_else(|| {
                        if col.data_type.contains('(') {
                            Some(col.data_type.clone())
                        } else {
                            None
                        }
                    })
                })
                .unwrap_or_default();

            if enum_type.is_empty() {
                warn!(
                    "Could not find enum type for column '{}' in table '{}'",
                    column, table
                );
                continue;
            }

            let variants = Self::parse_enum(&enum_type);
            let (sql, _) = qgen.create_enum(column, &variants);

            ops.push(SchemaOp {
                sql,
                description: format!("Create enum type '{}'", column),
                idempotent: true,
                skip_if_missing_ref: false,
            });
        }

        ops
    }

    /// Generate CREATE SEQUENCE ops.
    fn sequence_ops(&self) -> Vec<SchemaOp> {
        let qgen = QueryGenerator::new(self.target_dialect.as_ref());

        self.sequence_definitions
            .iter()
            .map(|seq| {
                let (sql, _) = qgen.create_sequence(seq);
                SchemaOp {
                    sql,
                    description: format!("Create sequence '{}'", seq.name),
                    idempotent: true,
                    skip_if_missing_ref: false,
                }
            })
            .collect()
    }

    /// Generate CREATE TABLE ops (topologically sorted, no FKs inline).
    fn table_ops(&self) -> Vec<SchemaOp> {
        let qgen = QueryGenerator::new(self.target_dialect.as_ref());
        let dep_graph = self.build_dependency_graph();

        // Get topological order; fall back to deterministic partial order on cycle
        // (partial_topological_order sorts acyclic tables first, then cycle members
        // alphabetically - always deterministic, avoids random HashMap iteration order).
        let table_order = dep_graph
            .without_self_references()
            .partial_topological_order();

        let mut ops = Vec::new();

        for table in &table_order {
            let columns = match self.column_definitions.get(table) {
                Some(cols) => cols,
                None => continue,
            };

            let resolved_table = self.mapping.entities.resolve(table);
            let mut resolved_columns = self.resolve_column_definitions(table, columns);

            if self.mapped_columns_only {
                resolved_columns = self.filter_to_mapped_columns(&resolved_table, resolved_columns);
            }

            // Computed columns are async - we handle them synchronously for build_ops
            // by using the pre-collected column defs. Callers should ensure computed
            // columns are already added via add_column_defs if needed.

            let (sql, _) = qgen.create_table(
                &resolved_table,
                &resolved_columns,
                self.ignore_constraints,
                false,
            );

            ops.push(SchemaOp {
                sql,
                description: format!("Create table '{}'", resolved_table),
                idempotent: true,
                skip_if_missing_ref: false,
            });
        }

        ops
    }

    /// Generate CREATE INDEX ops.
    fn index_ops(&self) -> Vec<SchemaOp> {
        let qgen = QueryGenerator::new(self.target_dialect.as_ref());
        let mut ops = Vec::new();

        // Non-unique indexes first, then unique (unique may depend on data)
        let mut all_indexes: Vec<&IndexDef> = self
            .index_definitions
            .values()
            .flat_map(|idxs| idxs.iter())
            .collect();
        all_indexes.sort_by_key(|idx| idx.unique);

        for index in all_indexes {
            let (sql, _) = qgen.create_index(index);
            ops.push(SchemaOp {
                sql,
                description: format!("Create index '{}'", index.name),
                idempotent: true,
                skip_if_missing_ref: false,
            });
        }

        ops
    }

    /// Generate ALTER TABLE ADD CONSTRAINT ops (FKs, CHECK, UNIQUE).
    fn constraint_ops(&self) -> Vec<SchemaOp> {
        if self.ignore_constraints {
            return Vec::new();
        }

        let qgen = QueryGenerator::new(self.target_dialect.as_ref());
        let mut ops = Vec::new();

        for (table, fks) in &self.fk_definitions {
            let resolved_table = self.mapping.entities.resolve(table);

            for fk in fks {
                let ref_table = self.mapping.entities.resolve(&fk.referenced_table);
                let ref_columns: Vec<String> = fk
                    .referenced_columns
                    .iter()
                    .map(|col| self.mapping.field_mappings.resolve(&ref_table, col))
                    .collect();
                let columns: Vec<String> = fk
                    .columns
                    .iter()
                    .map(|col| self.mapping.field_mappings.resolve(&resolved_table, col))
                    .collect();

                let resolved_fk = ForeignKeyDef {
                    constraint_name: fk.constraint_name.clone(),
                    referenced_table: ref_table,
                    referenced_columns: ref_columns,
                    columns: columns.clone(),
                    on_delete: fk.on_delete.clone(),
                    on_update: fk.on_update.clone(),
                };

                let (sql, _) = qgen.add_foreign_key(&resolved_table, &resolved_fk);
                let desc = fk.constraint_name.as_deref().unwrap_or("FK");
                ops.push(SchemaOp {
                    sql,
                    description: format!("Add foreign key '{}' on '{}'", desc, resolved_table),
                    idempotent: true,
                    skip_if_missing_ref: true,
                });
            }
        }

        // UNIQUE constraints
        for (table, constraints) in &self.unique_constraint_definitions {
            let resolved_table = self.mapping.entities.resolve(table);

            for uc in constraints {
                let columns: Vec<String> = uc
                    .columns
                    .iter()
                    .map(|col| self.mapping.field_mappings.resolve(&resolved_table, col))
                    .collect();

                let resolved_uc = UniqueConstraintDef {
                    constraint_name: uc.constraint_name.clone(),
                    table: resolved_table.clone(),
                    columns,
                };

                let (sql, _) = qgen.add_unique_constraint(&resolved_table, &resolved_uc);
                let desc = uc.constraint_name.as_deref().unwrap_or("UNIQUE");
                ops.push(SchemaOp {
                    sql,
                    description: format!(
                        "Add unique constraint '{}' on '{}'",
                        desc, resolved_table
                    ),
                    idempotent: true,
                    skip_if_missing_ref: false,
                });
            }
        }

        // CHECK constraints
        for (table, constraints) in &self.check_constraint_definitions {
            let resolved_table = self.mapping.entities.resolve(table);

            for cc in constraints {
                let resolved_cc = CheckConstraintDef {
                    constraint_name: cc.constraint_name.clone(),
                    table: resolved_table.clone(),
                    expression: cc.expression.clone(),
                };

                let (sql, _) = qgen.add_check_constraint(&resolved_table, &resolved_cc);
                let desc = cc.constraint_name.as_deref().unwrap_or("CHECK");
                ops.push(SchemaOp {
                    sql,
                    description: format!("Add check constraint '{}' on '{}'", desc, resolved_table),
                    idempotent: true,
                    skip_if_missing_ref: false,
                });
            }
        }

        ops
    }

    /// Generate ALTER TABLE DROP CONSTRAINT IF EXISTS ops for all named FK constraints.
    /// Emitted in pre-migration so data is written without active FK constraints.
    fn drop_fk_ops(&self) -> Vec<SchemaOp> {
        let mut ops = Vec::new();

        for (table, fks) in &self.fk_definitions {
            let resolved_table = self.mapping.entities.resolve(table);
            let quoted_table = self.target_dialect.quote_identifier(&resolved_table);

            for fk in fks {
                let Some(name) = &fk.constraint_name else {
                    continue; // can't reference anonymous constraints by name
                };
                let quoted_name = self.target_dialect.quote_identifier(name);
                ops.push(SchemaOp {
                    sql: format!(
                        "ALTER TABLE {quoted_table} DROP CONSTRAINT IF EXISTS {quoted_name};"
                    ),
                    description: format!(
                        "Drop foreign key '{}' on '{}' before data migration",
                        name, resolved_table
                    ),
                    idempotent: true, // IF EXISTS makes this a no-op when constraint is absent
                    skip_if_missing_ref: false,
                });
            }
        }

        ops
    }

    pub async fn table_queries(&self) -> HashSet<(String, String)> {
        let mut queries = HashSet::new();

        for (table, columns) in &self.column_definitions {
            let resolved_table = self.mapping.entities.resolve(table);
            let mut resolved_columns = self.resolve_column_definitions(table, columns);

            if self.mapped_columns_only {
                resolved_columns =
                    self.filter_to_mapped_columns(&resolved_table, resolved_columns.clone());
            }

            // Computed columns may already be in resolved_columns if plan_schema() pre-added
            // them via extend_column_defs(). Only append those not already present.
            let existing_names: HashSet<String> =
                resolved_columns.iter().map(|c| c.name.clone()).collect();
            let new_computed: Vec<_> = self
                .computed_column_defs(table)
                .await
                .into_iter()
                .filter(|col| !existing_names.contains(&col.name))
                .collect();
            resolved_columns.extend(new_computed);

            let (sql, _) = QueryGenerator::new(self.target_dialect.as_ref()).create_table(
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
                let resolved_table = self.mapping.entities.resolve(table);
                fks.iter().map(move |fk| {
                    let ref_table = self.mapping.entities.resolve(&fk.referenced_table);
                    let ref_columns: Vec<String> = fk
                        .referenced_columns
                        .iter()
                        .map(|col| self.mapping.field_mappings.resolve(&ref_table, col))
                        .collect();
                    let columns: Vec<String> = fk
                        .columns
                        .iter()
                        .map(|col| self.mapping.field_mappings.resolve(&resolved_table, col))
                        .collect();

                    let resolved_fk = ForeignKeyDef {
                        constraint_name: fk.constraint_name.clone(),
                        referenced_table: ref_table,
                        referenced_columns: ref_columns,
                        columns: columns.clone(),
                        on_delete: fk.on_delete.clone(),
                        on_update: fk.on_update.clone(),
                    };

                    let (sql, _) = QueryGenerator::new(self.target_dialect.as_ref())
                        .add_foreign_key(&resolved_table, &resolved_fk);
                    let key = fk.columns.first().cloned().unwrap_or_default();
                    (sql, key)
                })
            })
            .collect()
    }

    pub fn enum_queries(&self) -> HashSet<(String, String)> {
        let mut queries = HashSet::new();

        for (table, column) in &self.enum_definitions {
            // Prefer full_column_type (e.g. "enum('G','PG','PG-13','R','NC-17')")
            // over data_type (which is just "enum" from MySQL INFORMATION_SCHEMA.DATA_TYPE).
            let enum_type = self
                .metadata_graph
                .get(table)
                .and_then(|meta| meta.columns.get(column))
                .and_then(|col| {
                    col.full_column_type.clone().or_else(|| {
                        if col.data_type.contains('(') {
                            Some(col.data_type.clone())
                        } else {
                            None
                        }
                    })
                })
                .unwrap_or_default();

            if enum_type.is_empty() {
                warn!(
                    "Could not find enum type for column '{}' in table '{}'",
                    column, table
                );
                continue;
            }

            let variants = Self::parse_enum(&enum_type);
            let (sql, _) =
                QueryGenerator::new(self.target_dialect.as_ref()).create_enum(column, &variants);

            queries.insert((sql, column.clone()));
        }

        queries
    }

    pub fn index_queries(&self) -> Vec<(String, String)> {
        let qgen = QueryGenerator::new(self.target_dialect.as_ref());

        self.index_definitions
            .values()
            .flat_map(|idxs| idxs.iter())
            .map(|index| {
                let (sql, _) = qgen.create_index(index);
                (sql, index.name.clone())
            })
            .collect()
    }

    /// Merge another SchemaPlan into this one, deduplicating enums and sequences.
    pub fn merge(&mut self, other: SchemaPlan) {
        // Merge column definitions: first plan's definition wins per table.
        // When two plans both define the same table (e.g. shared FK target discovered
        // from multiple roots), we keep the first definition that was merged in.
        for (table, cols) in other.column_definitions {
            self.column_definitions.entry(table).or_insert(cols);
        }

        // Merge enums (set deduplication)
        self.enum_definitions.extend(other.enum_definitions);

        // Merge FK definitions
        for (table, fks) in other.fk_definitions {
            self.fk_definitions.entry(table).or_default().extend(fks);
        }

        // Merge index definitions
        for (table, idxs) in other.index_definitions {
            self.index_definitions
                .entry(table)
                .or_default()
                .extend(idxs);
        }

        // Merge sequences (dedup by name)
        let existing_names: HashSet<String> = self
            .sequence_definitions
            .iter()
            .map(|s| s.name.clone())
            .collect();
        for seq in other.sequence_definitions {
            if !existing_names.contains(&seq.name) {
                self.sequence_definitions.push(seq);
            }
        }

        // Merge unique constraint definitions
        for (table, ucs) in other.unique_constraint_definitions {
            self.unique_constraint_definitions
                .entry(table)
                .or_default()
                .extend(ucs);
        }

        // Merge check constraint definitions
        for (table, ccs) in other.check_constraint_definitions {
            self.check_constraint_definitions
                .entry(table)
                .or_default()
                .extend(ccs);
        }

        // Merge metadata graph
        for (table, meta) in other.metadata_graph {
            self.metadata_graph.entry(table).or_insert(meta);
        }
    }

    pub fn build_dependency_graph(&self) -> DependencyGraph {
        let mut graph = DependencyGraph::new();

        for (table_name, table_metadata) in &self.metadata_graph {
            graph.add_table(table_name.clone());

            for fk in &table_metadata.foreign_keys {
                if &fk.referenced_table != table_name {
                    graph.add_dependency(table_name.clone(), fk.referenced_table.clone());
                }
            }
        }

        graph
    }

    /// Build a vector of ColumnDef from TableMetadata, sorted by ordinal.
    pub fn column_defs(&self, meta: &TableMetadata) -> Vec<ColumnDef> {
        let mut columns = meta.columns.values().cloned().collect::<Vec<_>>();
        columns.sort_by_key(|col| col.ordinal);
        columns
            .into_iter()
            .map(|col| {
                let (data_type, char_max_length) = self.type_engine.convert_column(&col);
                let generated_expression = col
                    .generated_expression
                    .as_deref()
                    .map(|e| self.type_engine.normalize_generated_expression(e));
                ColumnDef {
                    name: col.name.clone(),
                    data_type,
                    is_nullable: col.is_nullable,
                    is_primary_key: col.is_primary_key,
                    default: col.default_value.clone(),
                    char_max_length,
                    generated_expression,
                    is_stored: col.is_stored,
                    is_generated: col.is_generated,
                }
            })
            .collect()
    }

    pub async fn computed_column_defs(&self, table: &str) -> Vec<ColumnDef> {
        let mut defs = Vec::new();

        let resolved_table = self.mapping.entities.resolve(table);
        // Try by destination name first; fall back to source table name (cascade pipelines
        // key computed fields by source table since destination.table is empty).
        let computed_fields = self
            .mapping
            .field_mappings
            .get_computed(&resolved_table)
            .or_else(|| self.mapping.field_mappings.get_computed(table));
        let computed_fields = match computed_fields {
            Some(fields) => fields,
            None => return defs,
        };

        let metadata = match self.metadata_graph.get(table) {
            Some(m) => m,
            None => {
                warn!(
                    "Missing metadata for source table: {} (resolved: {})",
                    table, resolved_table
                );
                return defs;
            }
        };

        for computed in computed_fields {
            let column_name = &computed.name;
            let inferred_type = self
                .type_engine
                .infer_computed_type(computed, &metadata.columns(), &self.mapping)
                .await;

            if let Some((mut data_type, char_max_length)) = inferred_type {
                if let Type::Enum { values, .. } = &data_type
                    && let CompiledExpression::DotPath(segments) = &computed.expression
                    && segments.len() == 2
                {
                    let field = &segments[1];
                    data_type = Type::Enum {
                        name: field.clone(),
                        values: values.clone(),
                    };
                }

                defs.push(ColumnDef {
                    name: (*column_name).clone(),
                    is_nullable: true,
                    default: None,
                    data_type,
                    is_primary_key: false,
                    char_max_length,
                    generated_expression: None,
                    is_stored: false,
                    is_generated: false,
                });
            } else {
                warn!(
                    "Failed to infer type for computed field '{}' in table '{}' (resolved: '{}')",
                    column_name, table, resolved_table
                );
            }
        }

        defs
    }

    pub async fn resolved_column_defs(&self) -> Vec<ColumnDef> {
        let mut resolved_defs = Vec::new();
        for (table, columns) in &self.column_definitions {
            let resolved_table = self.mapping.entities.resolve(table);
            let mut resolved_columns = self.resolve_column_definitions(table, columns);

            if self.mapped_columns_only {
                resolved_columns =
                    self.filter_to_mapped_columns(&resolved_table, resolved_columns.clone());
            }

            // Guard against double-adding computed columns that plan_schema() may have
            // already included in column_definitions via extend_column_defs().
            let existing_names: HashSet<String> =
                resolved_columns.iter().map(|c| c.name.clone()).collect();
            let new_computed: Vec<_> = self
                .computed_column_defs(table)
                .await
                .into_iter()
                .filter(|col| !existing_names.contains(&col.name))
                .collect();
            resolved_columns.extend(new_computed);
            resolved_defs.extend(resolved_columns);
        }

        resolved_defs
    }

    pub fn collect_schema_deps(metadata: &TableMetadata, plan: &mut SchemaPlan) {
        let mut visited = HashSet::new();
        Self::visit_schema_deps(metadata, plan, &mut visited);
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

        plan.add_column_defs(&metadata.name, plan.column_defs(metadata));
        plan.add_fk_defs(&metadata.name, metadata.fk_defs());

        for col in plan.type_engine().extract_enums(metadata) {
            plan.add_enum_def(&metadata.name, &col.name);
        }
    }

    fn resolve_column_definitions(&self, table: &str, columns: &[ColumnDef]) -> Vec<ColumnDef> {
        let resolved_table = self.mapping.entities.resolve(table);
        let resolver = self.mapping.field_mappings.get_entity(&resolved_table);
        columns
            .iter()
            .map(|col| {
                let name = self
                    .mapping
                    .field_mappings
                    .resolve(&resolved_table, &col.name);
                // Rewrite column name references inside generated expressions so they
                // match the (potentially renamed) destination column names.
                let generated_expression = col.generated_expression.as_deref().map(|expr| {
                    if let Some(res) = resolver {
                        rewrite_column_refs(expr, &res.source_to_target)
                    } else {
                        expr.to_owned()
                    }
                });
                ColumnDef {
                    name,
                    generated_expression,
                    ..col.clone()
                }
            })
            .collect()
    }

    fn parse_enum(raw: &str) -> Vec<String> {
        let start = raw.find('(').map(|i| i + 1).unwrap_or(0);
        let end = raw.rfind(')').unwrap_or(raw.len());

        if start > end {
            return vec![];
        }

        raw[start..end]
            .split(',')
            .map(|s| s.trim().trim_matches('\'').to_string())
            .filter(|s| !s.is_empty())
            .collect()
    }

    fn filter_to_mapped_columns(&self, table: &str, columns: Vec<ColumnDef>) -> Vec<ColumnDef> {
        let Some(mapping) = self.mapping.field_mappings.field_renames.get(table) else {
            warn!(
                "No field mapping found for table '{}'; returning all columns unchanged",
                table
            );
            return columns;
        };
        columns
            .into_iter()
            .filter(|col| mapping.contains_target(&col.name))
            .collect()
    }
}

/// Rewrite column name references inside a SQL expression (e.g. a generated column body).
/// Performs whole-word replacement so `rental_rate` is not matched inside `original_rental_rate`.
fn rewrite_column_refs(
    expr: &str,
    source_to_target: &std::collections::HashMap<String, String>,
) -> String {
    let mut result = expr.to_owned();
    for (src, dst) in source_to_target {
        result = replace_word(&result, src, dst);
    }
    result
}

fn replace_word(haystack: &str, needle: &str, replacement: &str) -> String {
    let mut out = String::with_capacity(haystack.len());
    let mut rest = haystack;
    while let Some(pos) = rest.find(needle) {
        let before = &rest[..pos];
        let after = &rest[pos + needle.len()..];
        let left_ok = before
            .as_bytes()
            .last()
            .is_none_or(|&c| !c.is_ascii_alphanumeric() && c != b'_');
        let right_ok = after
            .as_bytes()
            .first()
            .is_none_or(|&c| !c.is_ascii_alphanumeric() && c != b'_');
        out.push_str(before);
        if left_ok && right_ok {
            out.push_str(replacement);
        } else {
            out.push_str(needle);
        }
        rest = after;
    }
    out.push_str(rest);
    out
}
