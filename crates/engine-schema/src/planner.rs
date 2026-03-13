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
use model::transform::mapping::TransformationMetadata;
use std::sync::Arc;

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
        self.add_index_details(&mut plan, table, &indexes);
        self.add_sequence_details(&mut plan, table, &meta);
        self.add_constraint_details(&mut plan, table).await?;

        // Resolve computed columns eagerly so build_ops() (sync) picks them up.
        let computed = plan.computed_column_defs(table).await;
        plan.extend_column_defs(&meta.name, computed);

        Ok(plan)
    }

    /// Initializes a SchemaPlan with the specialized TypeEngine and configuration.
    pub fn init_plan(&self) -> Result<SchemaPlan, DriverError> {
        let type_engine = TypeEngine::new(
            self.introspector.clone(),
            self.type_registry.clone(),
            self.source_dialect,
        );

        Ok(SchemaPlan::new(
            type_engine,
            self.ignore_constraints,
            self.mapped_columns_only,
            self.mapping.clone(),
        ))
    }

    /// Helper to populate SchemaPlan with table definitions.
    fn add_table_details(&self, plan: &mut SchemaPlan, table: &str, meta: &TableMetadata) {
        let columns = plan.column_defs(meta);
        plan.add_column_defs(&meta.name, columns);

        plan.add_metadata(table, meta.clone());

        // Process Foreign Keys: Filter by what is actually being migrated/mapped
        let fks_to_add: Vec<_> = meta
            .fk_defs()
            .into_iter()
            .filter(|fk| self.mapping.entities.contains_source(&fk.referenced_table))
            .collect();

        plan.add_fk_defs(&meta.name, fks_to_add);

        // Extract Enums using the plan's type engine
        for col in plan.type_engine().extract_enums(meta) {
            plan.add_enum_def(&meta.name, &col.name);
        }
    }

    /// Populate SchemaPlan with index definitions from introspected metadata.
    /// Converts source `IndexType` to target dialect via TypeRegistry.
    fn add_index_details(&self, plan: &mut SchemaPlan, table: &str, indexes: &[IndexMetadata]) {
        let resolved_table = self.mapping.entities.resolve(table);

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
