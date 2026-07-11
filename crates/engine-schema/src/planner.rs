use crate::{
    plan::SchemaPlan,
    type_registry::{Dialect, TypeRegistry},
    types::TypeEngine,
};
use connectors::{
    error::DriverError,
    sql::{
        metadata::{index::IndexMetadata, table::TableMetadata},
        query::{
            constraint::{CheckConstraintDef, UniqueConstraintDef},
            index::{IndexColumnDef, IndexDef},
            sequence::SequenceDef,
        },
    },
    traits::introspector::SchemaIntrospector,
};
use model::{core::types::Type, transform::mapping::TransformationMetadata};
use std::sync::Arc;

/// Key prefix used when a target dialect (MySQL) cannot index an unbounded
/// TEXT/BLOB column outright. 255 chars stays within InnoDB's 3072-byte key
/// limit even for 4-byte utf8mb4 characters.
const TEXT_INDEX_PREFIX_LEN: u32 = 255;

/// Responsible for orchestrating metadata retrieval and populating a robust SchemaPlan.
pub struct SchemaPlanner {
    introspector: Arc<dyn SchemaIntrospector>,
    source_dialect: Dialect,
    mapping: TransformationMetadata,
    ignore_constraints: bool,
    mapped_columns_only: bool,
    type_registry: Arc<TypeRegistry>,
}

impl SchemaPlanner {
    pub fn new(
        introspector: Arc<dyn SchemaIntrospector>,
        source_dialect: Dialect,
        mapping: TransformationMetadata,
        ignore_constraints: bool,
        mapped_columns_only: bool,
        type_registry: TypeRegistry,
    ) -> Self {
        Self {
            introspector,
            source_dialect,
            mapping,
            ignore_constraints,
            mapped_columns_only,
            type_registry: Arc::new(type_registry),
        }
    }

    /// Primary entry point: Orchestrates the construction of a SchemaPlan for a source table.
    pub async fn plan_schema(&self, table: &str) -> Result<SchemaPlan, DriverError> {
        let meta = self.introspector.table_metadata(table).await?;
        let indexes = self.introspector.index_metadata(table).await?;

        let mut plan = self.init_plan()?;

        self.add_table_details(&mut plan, table, &meta);
        self.add_index_details(&mut plan, table, &meta, &indexes);
        self.add_sequence_details(&mut plan, table, &meta);
        self.add_constraint_details(&mut plan, table).await?;

        // Resolve computed columns eagerly so build_ops() (sync) picks them up.
        let computed = plan.computed_column_defs(table).await;
        plan.extend_column_defs(&meta.name, computed);

        // Fold in columns produced by plugin transforms.
        plan.add_plugin_columns(&meta.name, self.mapping.plugin_columns.clone());

        Ok(plan)
    }

    /// Initializes a SchemaPlan with the specialized TypeEngine and configuration.
    pub fn init_plan(&self) -> Result<SchemaPlan, DriverError> {
        let type_engine = TypeEngine::new(
            self.introspector.clone(),
            self.type_registry.clone(),
            self.source_dialect,
        );

        let mut plan = SchemaPlan::new(
            type_engine,
            self.ignore_constraints,
            self.mapped_columns_only,
            self.mapping.clone(),
        );

        // Render DDL in the destination's dialect.
        plan.set_target_dialect(self.type_registry.target_dialect().as_query_dialect());

        Ok(plan)
    }

    /// Helper to populate SchemaPlan with table definitions.
    fn add_table_details(&self, plan: &mut SchemaPlan, table: &str, meta: &TableMetadata) {
        let columns = plan.column_defs(meta);
        plan.add_column_defs(&meta.name, columns);

        plan.add_metadata(table, meta.clone());

        // Only recreate a foreign key when its referenced table is also migrated
        // (created) in this run.
        let fks_to_add: Vec<_> = meta
            .fk_defs()
            .into_iter()
            .filter(|fk| self.mapping.migrates(&fk.referenced_table))
            .collect();

        plan.add_fk_defs(&meta.name, fks_to_add);

        // Extract Enums using the plan's type engine
        for col in plan.type_engine().extract_enums(meta) {
            plan.add_enum_def(&meta.name, &col.name);
        }
    }

    /// Populate SchemaPlan with index definitions from introspected metadata.
    /// Converts source `IndexType` to target dialect via TypeRegistry.
    fn add_index_details(
        &self,
        plan: &mut SchemaPlan,
        table: &str,
        meta: &TableMetadata,
        indexes: &[IndexMetadata],
    ) {
        let resolved_table = self.mapping.entities.resolve(table);

        // MySQL cannot index an unbounded TEXT/BLOB column without a key prefix;
        // PostgreSQL has no prefix syntax at all.
        let target_supports_prefix = self
            .type_registry
            .target_dialect()
            .as_query_dialect()
            .supports_index_prefix();

        let index_defs: Vec<IndexDef> = indexes
            .iter()
            .filter(|idx| !idx.is_primary)
            .map(|idx| {
                let columns = idx
                    .columns
                    .iter()
                    .map(|col| IndexColumnDef {
                        name: self
                            .mapping
                            .field_mappings
                            .resolve(&resolved_table, &col.name),
                        sort_order: col.sort_order.clone(),
                        nulls_order: col.nulls_order.clone(),
                        prefix_length: target_supports_prefix
                            .then(|| self.index_prefix_for(plan, meta, &col.name))
                            .flatten(),
                    })
                    .collect();

                IndexDef {
                    name: idx.name.clone(),
                    table: resolved_table.clone(),
                    columns,
                    unique: idx.is_unique,
                    index_type: Some(self.type_registry.convert_index_type(&idx.index_type)),
                    condition: idx.condition.clone(),
                }
            })
            .collect();

        if !index_defs.is_empty() {
            plan.add_index_defs(table, index_defs);
        }
    }

    /// Key prefix required to index `column` in the target dialect, if any.
    fn index_prefix_for(
        &self,
        plan: &SchemaPlan,
        meta: &TableMetadata,
        column: &str,
    ) -> Option<u32> {
        let col = meta.columns.get(column)?;
        let (target_type, _) = plan.type_engine().convert_column(col);

        matches!(target_type, Type::Text { .. } | Type::Blob { .. })
            .then_some(TEXT_INDEX_PREFIX_LEN)
    }

    /// Populate SchemaPlan with UNIQUE and CHECK constraint definitions from introspected metadata.
    async fn add_constraint_details(
        &self,
        plan: &mut SchemaPlan,
        table: &str,
    ) -> Result<(), DriverError> {
        let resolved_table = self.mapping.entities.resolve(table);

        // UNIQUE constraints
        let unique_constraints = self.introspector.unique_constraint_metadata(table).await?;
        if !unique_constraints.is_empty() {
            let unique_defs: Vec<UniqueConstraintDef> = unique_constraints
                .into_iter()
                .map(|uc| {
                    let columns = uc
                        .columns
                        .iter()
                        .map(|col| self.mapping.field_mappings.resolve(&resolved_table, col))
                        .collect();

                    UniqueConstraintDef {
                        constraint_name: Some(uc.constraint_name),
                        table: resolved_table.clone(),
                        columns,
                    }
                })
                .collect();

            plan.add_unique_constraint_defs(table, unique_defs);
        }

        // CHECK constraints
        let check_constraints = self.introspector.check_constraint_metadata(table).await?;
        if !check_constraints.is_empty() {
            let check_defs: Vec<CheckConstraintDef> = check_constraints
                .into_iter()
                .map(|cc| CheckConstraintDef {
                    constraint_name: Some(cc.constraint_name),
                    table: resolved_table.clone(),
                    expression: cc.definition,
                })
                .collect();

            plan.add_check_constraint_defs(table, check_defs);
        }

        Ok(())
    }

    /// Extract sequences from auto_increment columns when the target dialect requires them.
    fn add_sequence_details(&self, plan: &mut SchemaPlan, table: &str, meta: &TableMetadata) {
        if !self.type_registry.use_explicit_sequences() {
            return;
        }

        let resolved_table = self.mapping.entities.resolve(table);

        for col in meta.columns.values() {
            if !col.is_auto_increment {
                continue;
            }

            let resolved_col = self
                .mapping
                .field_mappings
                .resolve(&resolved_table, &col.name);

            plan.add_sequence(SequenceDef {
                name: format!("{}_{}_seq", resolved_table, resolved_col),
                start: Some(1),
                increment: Some(1),
                min_value: None,
                max_value: None,
                owned_by: Some((resolved_table.clone(), resolved_col)),
            });
        }
    }
}
