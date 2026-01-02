use crate::{
    connectors::{linked::LinkedSource, source::Source},
    schema::{plan::SchemaPlan, types::TypeEngine},
};
use connectors::{
    error::AdapterError,
    metadata::{entity::EntityMetadata, field::FieldMetadata},
    sql::base::metadata::{column::ColumnMetadata, table::TableMetadata},
};
use model::{core::data_type::DataType, transform::mapping::TransformationMetadata};

/// Responsible for orchestrating metadata retrieval and populating a robust SchemaPlan.
pub struct SchemaPlanner {
    source: Source,
    mapping: TransformationMetadata,
    ignore_constraints: bool,
    mapped_columns_only: bool,
}

impl SchemaPlanner {
    pub fn new(
        source: Source,
        mapping: TransformationMetadata,
        ignore_constraints: bool,
        mapped_columns_only: bool,
    ) -> Self {
        Self {
            source,
            mapping,
            ignore_constraints,
            mapped_columns_only,
        }
    }

    /// Primary entry point: Orchestrates the construction of a SchemaPlan for a source table.
    pub async fn plan_schema(&self, table: &str) -> Result<SchemaPlan, AdapterError> {
        // Fetch primary metadata from the source
        let meta = self.source.primary.fetch_meta(table.to_string()).await?;

        let mut plan = self.init_plan().await?;

        // Populate details for the primary entity
        self.add_table_details(&mut plan, table, &meta).await?;

        // Handle Join-based dependencies (LinkedSource)
        // This processes tables explicitly joined in the pipeline configuration
        if let Some(linked_source) = &self.source.linked
            && let LinkedSource::Table(join_source) = linked_source
        {
            for join_clause in &join_source.clauses {
                let table_name = &join_clause.right.table;
                let alias = &join_clause.right.alias;

                if !plan.metadata_exists(table_name)
                    && let Ok(join_meta) = self.source.primary.fetch_meta(table_name.clone()).await
                {
                    // Add metadata under both the table name and the alias
                    plan.add_metadata(table_name, join_meta.clone());
                    plan.add_metadata(alias, join_meta.clone());

                    // Extract enums from joined table
                    if let EntityMetadata::Table(joined_table_meta) = &join_meta {
                        let extract_enums = plan.type_engine().enum_extractor();
                        for col in extract_enums(joined_table_meta) {
                            plan.add_enum_def(&joined_table_meta.name, &col.name);
                        }
                    }
                }
            }
        }

        Ok(plan)
    }

    /// Initializes a SchemaPlan with the specialized TypeEngine and configuration.
    pub async fn init_plan(&self) -> Result<SchemaPlan, AdapterError> {
        let source_handle = self.source.primary.clone();

        let type_engine = TypeEngine::new(
            source_handle.clone(),
            // Converter: Mapping field metadata to Target Types (e.g., Postgres)
            Box::new(|meta: &FieldMetadata| -> (DataType, Option<usize>) { meta.pg_type() }),
            // Extractor: Pulling enum metadata for schema creation
            Box::new(|meta: &TableMetadata| -> Vec<ColumnMetadata> { TableMetadata::enums(meta) }),
        );

        Ok(SchemaPlan::new(
            source_handle,
            type_engine,
            self.ignore_constraints,
            self.mapped_columns_only,
            self.mapping.clone(),
        ))
    }

    /// Helper to bridge EntityMetadata into the SchemaPlan definitions.
    async fn add_table_details(
        &self,
        plan: &mut SchemaPlan,
        table: &str,
        meta: &EntityMetadata,
    ) -> Result<(), AdapterError> {
        let columns = plan.column_defs(meta);
        plan.add_column_defs(&meta.name(), columns);

        // Store metadata for computed column inference and relationship lookups
        plan.add_metadata(table, meta.clone());

        if let EntityMetadata::Table(table_meta) = meta {
            // Process Foreign Keys: Filter by what is actually being migrated/mapped
            let fks_to_add: Vec<_> = table_meta
                .fk_defs()
                .into_iter()
                .filter(|fk| self.mapping.entities.contains_source(&fk.referenced_table))
                .collect();

            plan.add_fk_defs(&meta.name(), fks_to_add);

            // Extract Enums using the plan's type engine configuration
            let extract_enums = plan.type_engine().enum_extractor();
            for col in extract_enums(table_meta) {
                plan.add_enum_def(&meta.name(), &col.name);
            }
        }

        Ok(())
    }
}
