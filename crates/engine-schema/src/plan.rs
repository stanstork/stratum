use crate::{
    dep_graph::DependencyGraph,
    schema_ops::{SchemaOp, SchemaOps},
    types::{ComputedTypes, TypeEngine},
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
use query_builder::dialect::{self, QueryDialect};
use std::collections::{HashMap, HashSet, hash_map::Entry};
use tracing::warn;

/// Which schema objects to skip when creating destination tables.
#[derive(Debug, Clone, Copy, Default)]
pub struct SchemaObjectFlags {
    pub skip_pk: bool,
    pub skip_fk: bool,
    pub skip_idx: bool,
    pub skip_seq: bool,
    pub skip_unique: bool,
    pub skip_check: bool,
}

impl SchemaObjectFlags {
    /// Read every skip flag from a pipeline's raw settings map.
    pub fn from_pipeline(pipeline: &model::execution::pipeline::Pipeline) -> Self {
        Self {
            skip_pk: pipeline.setting_flag("skip_pk"),
            skip_fk: pipeline.setting_flag("skip_fk"),
            skip_idx: pipeline.setting_flag("skip_idx"),
            skip_seq: pipeline.setting_flag("skip_seq"),
            skip_unique: pipeline.setting_flag("skip_unique"),
            skip_check: pipeline.setting_flag("skip_check"),
        }
    }
}

/// Represents the schema migration plan from source to target, including type conversion,
/// name mapping, and metadata relationships.
///
/// Supports multi-object collection (tables, enums, FKs, indexes, sequences) and
/// generates properly-ordered `SchemaOps` via `build_ops()`.
pub struct SchemaPlan {
    /// Type engine for converting types between source and target databases.
    type_engine: TypeEngine,

    /// Target dialect for DDL rendering.
    target_dialect: &'static (dyn QueryDialect + Send + Sync),

    /// Create destination tables without a primary key (and never add one).
    skip_pk: bool,

    /// Defer PRIMARY KEY creation to the post phase.
    defer_pk: bool,

    /// Don't create foreign-key constraints on the destination.
    skip_fk: bool,

    /// Don't create secondary (non-constraint) indexes on the destination.
    skip_idx: bool,

    /// Don't create sequences (identity / auto-increment) on the destination.
    skip_seq: bool,

    /// Don't create UNIQUE constraints on the destination.
    skip_unique: bool,

    /// Don't create CHECK constraints on the destination.
    skip_check: bool,

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
        flags: SchemaObjectFlags,
        mapped_columns_only: bool,
        mapping: TransformationMetadata,
    ) -> Self {
        Self {
            type_engine,
            target_dialect: &dialect::Postgres,
            skip_pk: flags.skip_pk,
            defer_pk: false,
            skip_fk: flags.skip_fk,
            skip_idx: flags.skip_idx,
            skip_seq: flags.skip_seq,
            skip_unique: flags.skip_unique,
            skip_check: flags.skip_check,
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

    pub fn set_target_dialect(&mut self, dialect: &'static (dyn QueryDialect + Send + Sync)) {
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

    pub fn defer_pk(&mut self, defer: bool) {
        self.defer_pk = defer;
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

    /// Register columns produced by plugin transforms (e.g. `select { sum =
    /// plugin.adder({...}) }`) so they're included in the destination DDL.
    pub fn add_plugin_columns(&mut self, table_name: &str, outputs: Vec<(String, Type)>) {
        if outputs.is_empty() {
            return;
        }

        let cols = self
            .column_definitions
            .entry(table_name.to_string())
            .or_default();

        for (name, data_type) in outputs {
            if let Some(existing) = cols.iter_mut().find(|c| c.name == name) {
                // Plugin output shadows a source column: retype it, drop any
                // char length / generated-expression carried over from the
                // source definition. Keep nullability and primary-key as-is.
                existing.data_type = data_type;
                existing.char_max_length = None;
                existing.generated_expression = None;
                existing.is_generated = false;
                existing.is_stored = false;
            } else {
                cols.push(ColumnDef {
                    name,
                    is_nullable: true,
                    default: None,
                    data_type,
                    is_primary_key: false,
                    char_max_length: None,
                    generated_expression: None,
                    is_stored: false,
                    is_generated: false,
                });
            }
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

        if !self.skip_seq {
            pre.extend(self.sequence_ops());
        }

        // When requested: drop existing FK constraints before data migration so that
        // a cascade run succeeds even if a prior schema_only run already created them.
        // FKs are re-added in the post phase as usual.
        if self.drop_constraints && !self.skip_fk {
            pre.extend(self.drop_fk_ops());
        }

        // Deferred primary keys: post-data, before indexes and FKs.
        post.extend(self.pk_ops());

        // Indexes: post-data by default, pre-data if configured. Skipped entirely
        // when `skip_idx` is set.
        if !self.skip_idx {
            match self.index_creation {
                IndexCreationStrategy::AfterData => post.extend(self.index_ops()),
                IndexCreationStrategy::BeforeData => pre.extend(self.index_ops()),
            }
        }

        // FK constraints: post-data by default, pre-data if configured
        match self.fk_creation {
            FkCreationStrategy::AfterData => post.extend(self.constraint_ops()),
            FkCreationStrategy::BeforeData => pre.extend(self.constraint_ops()),
        }

        SchemaOps { pre, post }
    }

    fn enum_type_name(&self, table: &str, column: &str) -> String {
        self.metadata_graph
            .get(table)
            .and_then(|meta| meta.columns.get(column))
            .map(|col| match self.type_engine.convert_column(col).0 {
                Type::Enum { name, .. } if !name.is_empty() => name,
                _ => column.to_string(),
            })
            .unwrap_or_else(|| column.to_string())
    }

    fn enum_ops(&self) -> Vec<SchemaOp> {
        // MySQL spells the variants inline in the column; `CREATE TYPE ... AS ENUM`
        // is PostgreSQL-only syntax and would be a hard error there.
        if !self.target_dialect.supports_enums() {
            return Vec::new();
        }

        let qgen = QueryGenerator::new(self.target_dialect);
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
                warn!(column = %column, table = %table, "could not find enum type for column");
                continue;
            }

            let variants = Self::parse_enum(&enum_type);
            let type_name = self.enum_type_name(table, column);
            let (sql, _) = qgen.create_enum(&type_name, &variants);

            ops.push(SchemaOp {
                sql,
                description: format!("Create enum type '{}'", type_name),
                idempotent: true,
                skip_if_missing_ref: false,
            });
        }

        ops
    }

    /// For each destination table, the set of column names that will actually
    /// exist after projection (`mapped_columns_only`) and renames.
    fn dest_column_index(&self) -> HashMap<String, HashSet<String>> {
        let mut index = HashMap::new();
        for (table, columns) in &self.column_definitions {
            let resolved_table = self.mapping.entities.resolve(table);
            let mut resolved = self.resolve_column_defs(table, columns);

            if self.mapped_columns_only {
                resolved = self.filter_to_mapped_columns(&resolved_table, resolved);
            }

            index.insert(
                resolved_table,
                resolved.into_iter().map(|c| c.name).collect::<HashSet<_>>(),
            );
        }
        index
    }

    /// Resolved (destination table, sorted columns) keys for every UNIQUE constraint.
    fn unique_keys(&self) -> HashSet<(String, Vec<String>)> {
        let mut keys = HashSet::new();
        for (table, constraints) in &self.unique_constraint_definitions {
            let resolved_table = self.mapping.entities.resolve(table);
            for uc in constraints {
                let mut cols: Vec<String> = uc
                    .columns
                    .iter()
                    .map(|c| self.mapping.field_mappings.resolve(&resolved_table, c))
                    .collect();
                cols.sort_unstable();
                keys.insert((resolved_table.clone(), cols));
            }
        }
        keys
    }

    /// True when `table` is copied whole with no column renames.
    fn is_verbatim(&self, table: &str) -> bool {
        if self.mapped_columns_only {
            return false;
        }

        match self.column_definitions.get(table) {
            Some(columns) => {
                let resolved_table = self.mapping.entities.resolve(table);
                columns.iter().all(|c| {
                    self.mapping
                        .field_mappings
                        .resolve(&resolved_table, &c.name)
                        == c.name
                })
            }
            None => false,
        }
    }

    /// Generate CREATE SEQUENCE ops, skipping sequences whose owning column was
    /// dropped by projection.
    fn sequence_ops(&self) -> Vec<SchemaOp> {
        let qgen = QueryGenerator::new(self.target_dialect);
        let dest_cols = self.dest_column_index();

        self.sequence_definitions
            .iter()
            .filter(|seq| match &seq.owned_by {
                Some((table, column)) => dest_cols
                    .get(table)
                    .is_some_and(|cols| cols.contains(column)),
                None => true,
            })
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

    /// Collapse columns that share a name, keeping the later definition (a
    /// computed or plugin output shadows the source column it overrides).
    fn dedupe_columns(columns: Vec<ColumnDef>) -> Vec<ColumnDef> {
        let mut out: Vec<ColumnDef> = Vec::with_capacity(columns.len());
        let mut positions: HashMap<String, usize> = HashMap::new();

        for col in columns {
            match positions.entry(col.name.clone()) {
                Entry::Occupied(e) => {
                    out[*e.get()] = col;
                }
                Entry::Vacant(e) => {
                    e.insert(out.len());
                    out.push(col);
                }
            }
        }
        out
    }

    /// Generate CREATE TABLE ops (topologically sorted, no FKs inline).
    fn table_ops(&self) -> Vec<SchemaOp> {
        let qgen = QueryGenerator::new(self.target_dialect);
        let dep_graph = self.build_dependency_graph();

        // Get topological order; fall back to deterministic partial order on cycle
        // (partial_topo_order sorts acyclic tables first, then cycle members
        // alphabetically - always deterministic, avoids random HashMap iteration order).
        let table_order = dep_graph.without_self_refs().partial_topo_order();

        let mut ops = Vec::new();

        for table in &table_order {
            let Some(columns) = self.column_definitions.get(table) else {
                continue;
            };

            let resolved_table = self.mapping.entities.resolve(table);
            let mut resolved_columns = self.resolve_column_defs(table, columns);

            if self.mapped_columns_only {
                resolved_columns = self.filter_to_mapped_columns(&resolved_table, resolved_columns);
            }

            // A computed column can share a source column's name (e.g.
            // `amount = amount / 0`); plan_schema appends it alongside the
            // original, so collapse duplicates, letting the later (computed)
            // definition shadow the source one.
            resolved_columns = Self::dedupe_columns(resolved_columns);

            // `skip_pk` drops the PK permanently; `defer_pk`
            // omits it here so `pk_ops()` can rebuild it in the post phase.
            let omit_pk = self.skip_pk || self.defer_pk;
            let (sql, _) = qgen.create_table(&resolved_table, &resolved_columns, omit_pk, false);

            ops.push(SchemaOp {
                sql,
                description: format!("Create table '{}'", resolved_table),
                idempotent: true,
                skip_if_missing_ref: false,
            });
        }

        ops
    }

    /// Generate ALTER TABLE ADD PRIMARY KEY ops for tables whose primary key was
    /// deferred (`defer_pk`). Empty otherwise.
    fn pk_ops(&self) -> Vec<SchemaOp> {
        if self.skip_pk || !self.defer_pk {
            return Vec::new();
        }

        let qgen = QueryGenerator::new(self.target_dialect);
        let dep_graph = self.build_dependency_graph();
        let table_order = dep_graph.without_self_refs().partial_topo_order();

        let mut ops = Vec::new();
        for table in &table_order {
            let Some(columns) = self.column_definitions.get(table) else {
                continue;
            };

            let resolved_table = self.mapping.entities.resolve(table);
            let mut resolved_columns = self.resolve_column_defs(table, columns);

            if self.mapped_columns_only {
                resolved_columns = self.filter_to_mapped_columns(&resolved_table, resolved_columns);
            }

            let pk_cols: Vec<String> = resolved_columns
                .iter()
                .filter(|c| c.is_primary_key)
                .map(|c| c.name.clone())
                .collect();

            if pk_cols.is_empty() {
                continue;
            }

            let (sql, _) = qgen.add_primary_key(&resolved_table, &pk_cols);

            ops.push(SchemaOp {
                sql,
                description: format!("Add primary key on '{}'", resolved_table),
                idempotent: false,
                skip_if_missing_ref: false,
            });
        }

        ops
    }

    /// Generate CREATE INDEX ops.
    fn index_ops(&self) -> Vec<SchemaOp> {
        let qgen = QueryGenerator::new(self.target_dialect);
        let mut ops = Vec::new();

        // Non-unique indexes first, then unique (unique may depend on data).
        // Skip an index when it references a column the destination table doesn't
        // have (dropped by projection), or when it duplicates a UNIQUE constraint
        // that `constraint_ops` already emits (same object on MySQL - emitting
        // both collides on a duplicate key name).
        let dest_cols = self.dest_column_index();
        let unique_keys = self.unique_keys();

        let mut all_indexes: Vec<&IndexDef> = self
            .index_definitions
            .values()
            .flat_map(|idxs| idxs.iter())
            .filter(|idx| {
                let columns_exist = dest_cols
                    .get(&idx.table)
                    .is_some_and(|cols| idx.columns.iter().all(|c| cols.contains(&c.name)));

                if !columns_exist {
                    return false;
                }

                if idx.unique {
                    let mut cols: Vec<String> =
                        idx.columns.iter().map(|c| c.name.clone()).collect();
                    cols.sort_unstable();
                    !unique_keys.contains(&(idx.table.clone(), cols))
                } else {
                    true
                }
            })
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
        let qgen = QueryGenerator::new(self.target_dialect);
        let dest_cols = self.dest_column_index();
        let mut ops = Vec::new();

        // Foreign keys are skipped when `skip_fk` is set; UNIQUE/CHECK
        // constraints below are unaffected.
        for (table, fks) in self.fk_definitions.iter().filter(|_| !self.skip_fk) {
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

                // Skip FKs whose local column was dropped by projection.
                if !dest_cols
                    .get(&resolved_table)
                    .is_some_and(|c| columns.iter().all(|col| c.contains(col)))
                {
                    continue;
                }

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
        if !self.skip_unique {
            for (table, constraints) in &self.unique_constraint_definitions {
                let resolved_table = self.mapping.entities.resolve(table);

                for uc in constraints {
                    let columns: Vec<String> = uc
                        .columns
                        .iter()
                        .map(|col| self.mapping.field_mappings.resolve(&resolved_table, col))
                        .collect();

                    // Skip UNIQUE constraints whose column was dropped by projection.
                    if !dest_cols
                        .get(&resolved_table)
                        .is_some_and(|c| columns.iter().all(|col| c.contains(col)))
                    {
                        continue;
                    }

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
                            "Add unique constraint '{desc}' on '{resolved_table}'"
                        ),
                        idempotent: true,
                        skip_if_missing_ref: false,
                    });
                }
            }
        }

        // CHECK constraints. The expression is opaque SQL we can't rewrite, so
        // only reproduce it when the table is copied whole with no renames.
        if !self.skip_check {
            for (table, constraints) in &self.check_constraint_definitions {
                if !self.is_verbatim(table) {
                    continue;
                }
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
                        description: format!("Add check constraint '{desc}' on '{resolved_table}'"),
                        idempotent: true,
                        skip_if_missing_ref: false,
                    });
                }
            }
        }

        ops
    }

    /// Generate ALTER TABLE DROP CONSTRAINT IF EXISTS ops for all named FK constraints.
    /// Emitted in pre-migration so data is written without active FK constraints.
    fn drop_fk_ops(&self) -> Vec<SchemaOp> {
        let qgen = QueryGenerator::new(self.target_dialect);
        let mut ops = Vec::new();

        for (table, fks) in &self.fk_definitions {
            let resolved_table = self.mapping.entities.resolve(table);

            for fk in fks {
                let Some(name) = &fk.constraint_name else {
                    continue; // can't reference anonymous constraints by name
                };

                let sql = qgen.drop_foreign_key(&resolved_table, name);

                ops.push(SchemaOp {
                    sql,
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
        columns.sort_unstable_by_key(|col| col.ordinal);

        columns
            .into_iter()
            .map(|col| {
                let (data_type, char_max_length) = self.type_engine.convert_column(&col);
                let generated_expression = col
                    .generated_expression
                    .as_deref()
                    .map(|e| self.type_engine.normalize_generated_expr(e));

                ColumnDef {
                    name: col.name.clone(),
                    data_type,
                    is_nullable: col.is_nullable,
                    is_primary_key: col.is_primary_key,
                    default: col
                        .default_value
                        .as_deref()
                        .map(|d| self.type_engine.normalize_default_expr(d)),
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
        let Some(computed_fields) = self
            .mapping
            .field_mappings
            .get_computed(&resolved_table)
            .or_else(|| self.mapping.field_mappings.get_computed(table))
        else {
            return defs;
        };

        let Some(metadata) = self.metadata_graph.get(table) else {
            warn!(table = %table, resolved = %resolved_table, "missing metadata for source table");
            return defs;
        };

        // Computed columns are inferred in declaration order; each resolved type
        // is recorded so a later computed column can reference an earlier one.
        let mut computed_types = ComputedTypes::new();

        for computed in computed_fields {
            let column_name = &computed.name;
            let inferred_type = self
                .type_engine
                .infer_computed_type(
                    computed,
                    &metadata.columns(),
                    &computed_types,
                    &self.mapping,
                )
                .await;

            if let Some((mut data_type, char_max_length)) = inferred_type {
                if let Type::Enum { values, .. } = &data_type
                    && let CompiledExpression::DotPath(segments) = &computed.expression
                    && segments.len() == 2
                {
                    data_type = Type::Enum {
                        name: segments[1].clone(),
                        values: values.clone(),
                    };
                }

                computed_types.insert(
                    column_name.to_ascii_lowercase(),
                    (data_type.clone(), char_max_length),
                );

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
                    column = %column_name,
                    table = %table,
                    resolved = %resolved_table,
                    "failed to infer type for computed field"
                );
            }
        }

        defs
    }

    pub async fn resolved_column_defs(&self) -> Vec<ColumnDef> {
        let mut resolved_defs = Vec::new();

        for (table, columns) in &self.column_definitions {
            let resolved_table = self.mapping.entities.resolve(table);
            let mut resolved_columns = self.resolve_column_defs(table, columns);

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

    fn resolve_column_defs(&self, table: &str, columns: &[ColumnDef]) -> Vec<ColumnDef> {
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
            warn!(table = %table, "no field mapping found for table, returning all columns unchanged");
            return columns;
        };

        // Computed-field targets (e.g. `full_name = concat(...)`) are projected
        // outputs too, but they live in `computed_fields`, not `field_renames`.
        let computed_targets: HashSet<&str> = self
            .mapping
            .field_mappings
            .get_computed(table)
            .into_iter()
            .flatten()
            .map(|c| c.name.as_str())
            .collect();

        columns
            .into_iter()
            .filter(|col| {
                // Keep mapped targets, computed outputs, and plugin-transform outputs.
                mapping.contains_target(&col.name)
                    || computed_targets.contains(col.name.as_str())
                    || self
                        .mapping
                        .plugin_columns
                        .iter()
                        .any(|(name, _)| name == &col.name)
            })
            .collect()
    }
}

/// Rewrite column name references inside a SQL expression (e.g. a generated column body).
/// Performs whole-word replacement so `rental_rate` is not matched inside `original_rental_rate`.
fn rewrite_column_refs(expr: &str, source_to_target: &HashMap<String, String>) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{type_registry::TypeRegistry, types::TypeEngine};
    use connectors::sql::metadata::fk::ForeignKeyAction;
    use connectors::sql::metadata::index::{NullsOrder, SortOrder};
    use connectors::sql::query::{
        constraint::{CheckConstraintDef, UniqueConstraintDef},
        index::{IndexColumnDef, IndexDef},
        sequence::SequenceDef,
    };
    use connectors::{
        error::DriverError,
        sql::metadata::{
            capabilities::Capabilities, fk::ForeignKeyMetadata, index::IndexMetadata,
            table::TableMetadata,
        },
        sql::query::fk::ForeignKeyDef,
        traits::{
            driver::{Driver, DriverInfo},
            introspector::SchemaIntrospector,
        },
    };
    use model::core::types::{IntSize, Type};
    use model::transform::mapping::{FieldTransformations, NameResolver, TransformationMetadata};
    use std::sync::Arc;

    /// build_ops() never introspects (definitions are collected up front), so a
    /// stub that answers nothing is enough to construct the TypeEngine.
    struct StubIntrospector;

    impl Driver for StubIntrospector {
        fn info(&self) -> &DriverInfo {
            static INFO: DriverInfo = DriverInfo {
                id: "stub",
                name: "Stub",
                schemes: &[],
            };
            &INFO
        }
        fn version(&self) -> &str {
            "0.0.0"
        }
        fn capabilities(&self) -> &Capabilities {
            use std::sync::LazyLock;
            static CAPS: LazyLock<Capabilities> = LazyLock::new(Capabilities::default);
            &CAPS
        }
    }

    #[async_trait::async_trait]
    impl SchemaIntrospector for StubIntrospector {
        async fn table_exists(&self, _t: &str) -> Result<bool, DriverError> {
            Ok(false)
        }
        async fn list_tables(&self, _s: Option<&str>) -> Result<Vec<String>, DriverError> {
            Ok(vec![])
        }
        async fn table_metadata(&self, _t: &str) -> Result<TableMetadata, DriverError> {
            Err(DriverError::QueryError("stub".into()))
        }
        async fn index_metadata(&self, _t: &str) -> Result<Vec<IndexMetadata>, DriverError> {
            Ok(vec![])
        }
        async fn fk_metadata(&self, _t: &str) -> Result<Vec<ForeignKeyMetadata>, DriverError> {
            Ok(vec![])
        }
        async fn referencing_tables(&self, _t: &str) -> Result<Vec<String>, DriverError> {
            Ok(vec![])
        }
        async fn table_size_bytes(&self, _t: &str) -> Result<u64, DriverError> {
            Ok(0)
        }
    }

    fn identity_mapping() -> TransformationMetadata {
        TransformationMetadata {
            entities: NameResolver::new(HashMap::new()),
            field_mappings: FieldTransformations::new(),
            foreign_fields: HashMap::new(),
            plugin_columns: Vec::new(),
            migrated_tables: HashSet::new(),
            has_projection: false,
        }
    }

    fn plan_with_flags(flags: SchemaObjectFlags) -> SchemaPlan {
        let introspector: Arc<dyn SchemaIntrospector> = Arc::new(StubIntrospector);
        let type_registry = Arc::new(TypeRegistry::new(
            crate::type_registry::Dialect::MySql,
            crate::type_registry::Dialect::Postgres,
        ));
        let type_engine = TypeEngine::new(
            introspector,
            type_registry,
            crate::type_registry::Dialect::MySql,
        );
        // Target dialect defaults to Postgres.
        SchemaPlan::new(type_engine, flags, false, identity_mapping())
    }

    fn empty_plan() -> SchemaPlan {
        plan_with_flags(SchemaObjectFlags::default())
    }

    /// Populate a table with a sequence, a UNIQUE + CHECK constraint, and an index
    /// so each skip flag has something to suppress.
    fn with_secondary_objects(plan: &mut SchemaPlan) {
        register(plan, "t", vec![int_col("id", true), int_col("a", false)]);
        plan.add_sequence(SequenceDef {
            name: "t_id_seq".into(),
            start: Some(1),
            increment: Some(1),
            min_value: None,
            max_value: None,
            owned_by: Some(("t".into(), "id".into())),
        });
        plan.add_unique_constraint_defs(
            "t",
            vec![UniqueConstraintDef {
                constraint_name: Some("uq_a".into()),
                table: "t".into(),
                columns: vec!["a".into()],
            }],
        );
        plan.add_check_constraint_defs(
            "t",
            vec![CheckConstraintDef {
                constraint_name: Some("ck_a".into()),
                table: "t".into(),
                expression: "a > 0".into(),
            }],
        );
        plan.add_index_defs(
            "t",
            vec![IndexDef {
                name: "idx_a".into(),
                table: "t".into(),
                columns: vec![index_col("a")],
                unique: false,
                index_type: None,
                condition: None,
            }],
        );
    }

    fn all_sql(plan: &SchemaPlan) -> String {
        let ops = plan.build_ops();
        ops.pre
            .iter()
            .chain(&ops.post)
            .map(|o| o.sql.to_uppercase())
            .collect::<Vec<_>>()
            .join("\n")
    }

    fn int_col(name: &str, pk: bool) -> ColumnDef {
        ColumnDef {
            name: name.into(),
            data_type: Type::Int {
                bits: IntSize::I32,
                unsigned: false,
                auto_increment: false,
            },
            is_nullable: !pk,
            is_primary_key: pk,
            default: None,
            char_max_length: None,
            generated_expression: None,
            is_stored: false,
            is_generated: false,
        }
    }

    /// Register a table's columns *and* metadata, mirroring `plan_schema`: the
    /// dependency graph (which drives table/pk ordering) is built from
    /// `metadata_graph`, so a table absent there emits no ops.
    fn register(plan: &mut SchemaPlan, table: &str, cols: Vec<ColumnDef>) {
        plan.add_column_defs(table, cols);
        plan.add_metadata(
            table,
            TableMetadata {
                name: table.into(),
                schema: None,
                columns: HashMap::new(),
                primary_keys: Vec::new(),
                foreign_keys: Vec::new(),
                referenced_tables: HashMap::new(),
                referencing_tables: HashMap::new(),
            },
        );
    }

    #[test]
    fn build_ops_inlines_primary_key_by_default() {
        let mut plan = empty_plan();
        register(
            &mut plan,
            "users",
            vec![int_col("id", true), int_col("age", false)],
        );

        let ops = plan.build_ops();

        assert_eq!(ops.pre.len(), 1, "one CREATE TABLE in pre");
        let create = &ops.pre[0].sql;
        assert!(create.contains("CREATE TABLE"), "got: {create}");
        assert!(
            create.to_uppercase().contains("PRIMARY KEY"),
            "PK must be inline by default: {create}"
        );
        assert!(ops.post.is_empty(), "nothing deferred: {:?}", ops.post);
    }

    #[test]
    fn build_ops_defers_primary_key_when_requested() {
        let mut plan = empty_plan();
        register(
            &mut plan,
            "users",
            vec![int_col("id", true), int_col("age", false)],
        );
        plan.defer_pk(true);

        let ops = plan.build_ops();

        let create = &ops.pre[0].sql;
        assert!(
            !create.to_uppercase().contains("PRIMARY KEY"),
            "deferred PK must not be inline in CREATE TABLE: {create}"
        );
        let pk_ops: Vec<_> = ops
            .post
            .iter()
            .filter(|o| o.sql.to_uppercase().contains("PRIMARY KEY"))
            .collect();
        assert_eq!(
            pk_ops.len(),
            1,
            "one ADD PRIMARY KEY in post: {:?}",
            ops.post
        );
        assert!(pk_ops[0].sql.contains("id"), "PK on id: {}", pk_ops[0].sql);
    }

    #[test]
    fn skip_pk_wins_over_defer() {
        // skip_pk is the first bool arg to SchemaPlan::new.
        let introspector: Arc<dyn SchemaIntrospector> = Arc::new(StubIntrospector);
        let type_registry = Arc::new(TypeRegistry::new(
            crate::type_registry::Dialect::MySql,
            crate::type_registry::Dialect::Postgres,
        ));
        let type_engine = TypeEngine::new(
            introspector,
            type_registry,
            crate::type_registry::Dialect::MySql,
        );
        let mut plan = SchemaPlan::new(
            type_engine,
            SchemaObjectFlags {
                skip_pk: true,
                ..Default::default()
            },
            false,
            identity_mapping(),
        );
        register(&mut plan, "users", vec![int_col("id", true)]);
        plan.defer_pk(true);

        let ops = plan.build_ops();
        assert!(
            !ops.pre[0].sql.to_uppercase().contains("PRIMARY KEY"),
            "no inline PK when skipped"
        );
        assert!(
            ops.post
                .iter()
                .all(|o| !o.sql.to_uppercase().contains("PRIMARY KEY")),
            "skip_pk suppresses the deferred ADD PRIMARY KEY too"
        );
    }

    #[test]
    fn deferred_primary_keys_precede_foreign_keys_in_post() {
        let mut plan = empty_plan();
        register(&mut plan, "users", vec![int_col("id", true)]);
        register(
            &mut plan,
            "orders",
            vec![int_col("id", true), int_col("user_id", false)],
        );
        plan.add_fk_def(
            "orders",
            ForeignKeyDef {
                constraint_name: None,
                columns: vec!["user_id".into()],
                referenced_table: "users".into(),
                referenced_columns: vec!["id".into()],
                on_delete: ForeignKeyAction::NoAction,
                on_update: ForeignKeyAction::NoAction,
            },
        );
        plan.defer_pk(true);

        let ops = plan.build_ops();

        // FKs are post-data (default) and must not leak into pre.
        assert!(
            ops.pre
                .iter()
                .all(|o| !o.sql.to_uppercase().contains("FOREIGN KEY")),
            "FKs belong in post: {:?}",
            ops.pre
        );
        let first_fk = ops
            .post
            .iter()
            .position(|o| o.sql.to_uppercase().contains("FOREIGN KEY"))
            .expect("an FK op in post");
        let last_pk = ops
            .post
            .iter()
            .rposition(|o| o.sql.to_uppercase().contains("PRIMARY KEY"))
            .expect("deferred PK ops in post");
        assert!(
            last_pk < first_fk,
            "deferred PKs must be added before FKs that may reference them: {:?}",
            ops.post
        );
    }

    #[test]
    fn drop_fk_ops_are_dialect_specific() {
        let mut plan = empty_plan();
        register(&mut plan, "users", vec![int_col("id", true)]);
        register(
            &mut plan,
            "orders",
            vec![int_col("id", true), int_col("user_id", false)],
        );
        plan.add_fk_def(
            "orders",
            ForeignKeyDef {
                constraint_name: Some("fk_orders_users".into()),
                columns: vec!["user_id".into()],
                referenced_table: "users".into(),
                referenced_columns: vec!["id".into()],
                on_delete: ForeignKeyAction::NoAction,
                on_update: ForeignKeyAction::NoAction,
            },
        );
        plan.set_drop_constraints(true);

        // Postgres (default target): `DROP CONSTRAINT IF EXISTS`.
        let pg_drop = plan
            .build_ops()
            .pre
            .into_iter()
            .find(|o| o.sql.to_uppercase().contains("DROP CONSTRAINT"))
            .expect("a pre DROP op on Postgres");
        assert_eq!(
            pg_drop.sql,
            r#"ALTER TABLE "orders" DROP CONSTRAINT IF EXISTS "fk_orders_users";"#
        );

        // MySQL: `DROP FOREIGN KEY`, no `IF EXISTS` (unsupported there).
        plan.set_target_dialect(&dialect::MySql);
        let my_drop = plan
            .build_ops()
            .pre
            .into_iter()
            .find(|o| o.sql.to_uppercase().contains("DROP FOREIGN KEY"))
            .expect("a pre DROP op on MySQL");
        assert_eq!(
            my_drop.sql,
            "ALTER TABLE `orders` DROP FOREIGN KEY `fk_orders_users`;"
        );
        assert!(
            !my_drop.sql.to_uppercase().contains("IF EXISTS"),
            "MySQL has no IF EXISTS for constraint drops: {}",
            my_drop.sql
        );
    }

    fn index_col(name: &str) -> IndexColumnDef {
        IndexColumnDef {
            name: name.into(),
            sort_order: SortOrder::Asc,
            nulls_order: NullsOrder::Default,
            prefix_length: None,
        }
    }

    #[test]
    fn computed_column_shadowing_source_is_deduped() {
        // A computed field can reuse a source column's name (e.g. `amount =
        // amount / 0`); plan_schema appends it, so column_definitions holds two
        // "amount" entries. build_ops must emit the column once.
        let mut plan = empty_plan();
        register(
            &mut plan,
            "t",
            vec![
                int_col("id", true),
                int_col("amount", false),
                int_col("amount", false),
            ],
        );

        let create = &plan.build_ops().pre[0].sql;
        assert_eq!(
            create.matches("\"amount\"").count(),
            1,
            "amount must appear exactly once: {create}"
        );
    }

    #[test]
    fn index_on_dropped_column_is_skipped_but_valid_one_kept() {
        let mut plan = empty_plan();
        register(&mut plan, "t", vec![int_col("id", true)]);
        plan.add_index_defs(
            "t",
            vec![
                IndexDef {
                    name: "idx_id".into(),
                    table: "t".into(),
                    columns: vec![index_col("id")],
                    unique: false,
                    index_type: None,
                    condition: None,
                },
                IndexDef {
                    name: "idx_ghost".into(),
                    table: "t".into(),
                    columns: vec![index_col("ghost")],
                    unique: false,
                    index_type: None,
                    condition: None,
                },
            ],
        );

        let ops = plan.build_ops();
        let all_sql: String = ops
            .pre
            .iter()
            .chain(&ops.post)
            .map(|o| o.sql.clone())
            .collect();
        assert!(all_sql.contains("idx_id"), "valid index kept: {all_sql}");
        assert!(
            !all_sql.contains("idx_ghost"),
            "index on a dropped column must be skipped: {all_sql}"
        );
    }

    #[test]
    fn unique_index_duplicating_constraint_is_skipped() {
        // MySQL reports a UNIQUE as both an index and a constraint. Emitting both
        // collides on a duplicate key name, so the index form is dropped.
        let mut plan = empty_plan();
        register(
            &mut plan,
            "t",
            vec![int_col("id", true), int_col("a", false)],
        );
        plan.add_index_defs(
            "t",
            vec![IndexDef {
                name: "uq_a".into(),
                table: "t".into(),
                columns: vec![index_col("a")],
                unique: true,
                index_type: None,
                condition: None,
            }],
        );
        plan.add_unique_constraint_defs(
            "t",
            vec![UniqueConstraintDef {
                constraint_name: Some("uq_a".into()),
                table: "t".into(),
                columns: vec!["a".into()],
            }],
        );

        let ops = plan.build_ops();
        let unique_ops: Vec<&str> = ops
            .pre
            .iter()
            .chain(&ops.post)
            .filter(|o| o.sql.to_uppercase().contains("UNIQUE"))
            .map(|o| o.sql.as_str())
            .collect();
        assert_eq!(
            unique_ops.len(),
            1,
            "exactly one UNIQUE op (the constraint, not the index): {unique_ops:?}"
        );
        assert!(
            unique_ops[0].to_uppercase().contains("ADD CONSTRAINT"),
            "the surviving UNIQUE is the constraint form: {}",
            unique_ops[0]
        );
    }

    #[test]
    fn secondary_objects_emitted_by_default() {
        let mut plan = empty_plan();
        with_secondary_objects(&mut plan);
        let sql = all_sql(&plan);
        assert!(sql.contains("SEQUENCE"), "sequence emitted: {sql}");
        assert!(sql.contains("UNIQUE"), "unique emitted: {sql}");
        assert!(sql.contains("CHECK"), "check emitted: {sql}");
        assert!(sql.contains("IDX_A"), "index emitted: {sql}");
    }

    #[test]
    fn skip_flags_suppress_their_objects() {
        let mut plan = plan_with_flags(SchemaObjectFlags {
            skip_idx: true,
            skip_seq: true,
            skip_unique: true,
            skip_check: true,
            ..Default::default()
        });
        with_secondary_objects(&mut plan);
        let sql = all_sql(&plan);
        assert!(!sql.contains("SEQUENCE"), "skip_seq: {sql}");
        assert!(!sql.contains("UNIQUE"), "skip_unique: {sql}");
        assert!(!sql.contains("CHECK"), "skip_check: {sql}");
        assert!(!sql.contains("IDX_A"), "skip_idx: {sql}");
        // The table itself is still created.
        assert!(sql.contains("CREATE TABLE"), "table still created: {sql}");
    }
}
