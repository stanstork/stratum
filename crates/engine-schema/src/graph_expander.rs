use crate::{
    plan::SchemaPlan,
    planner::SchemaPlanner,
    schema_ops::SchemaOps,
    type_registry::{Dialect, TypeRegistry},
};
use connectors::{
    error::DriverError,
    sql::metadata::{provider::MetadataProvider, table::TableMetadata},
    traits::introspector::SchemaIntrospector,
};
use model::{
    execution::references::{GraphReferences, TraversalDepth},
    transform::mapping::TransformationMetadata,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
};
use tracing::info;

/// Result of graph expansion: schema DDL ops + discovered table metadata.
pub struct GraphExpansionResult {
    /// Ordered DDL operations split into pre-migration and post-migration phases.
    pub schema_ops: SchemaOps,
    /// All discovered tables and their metadata (for cascade data fetching).
    pub discovered_tables: HashMap<String, TableMetadata>,
}

/// Expands a root table's FK graph into a complete set of schema operations.
///
/// Given a root table, the expander:
/// 1. Introspects FK dependencies via `MetadataProvider::build_metadata_graph()`
/// 2. Filters by depth and exclusion patterns
/// 3. Plans schema for each discovered table via `SchemaPlanner`
/// 4. Merges all plans and calls `SchemaPlan::build_ops()` for topologically sorted DDL
pub struct GraphExpander {
    introspector: Arc<dyn SchemaIntrospector>,
    type_registry: Arc<TypeRegistry>,
    source_dialect: Dialect,
}

impl GraphExpander {
    pub fn new(
        introspector: Arc<dyn SchemaIntrospector>,
        type_registry: Arc<TypeRegistry>,
        source_dialect: Dialect,
    ) -> Self {
        Self {
            introspector,
            type_registry,
            source_dialect,
        }
    }

    /// Expand the FK graph from the root table and produce schema operations.
    pub async fn expand(
        &self,
        root_table: &str,
        refs: &GraphReferences,
        mapping: &TransformationMetadata,
        ignore_constraints: bool,
        mapped_columns_only: bool,
    ) -> Result<GraphExpansionResult, DriverError> {
        // Build full metadata graph from root table
        let full_graph = MetadataProvider::build_metadata_graph(
            self.introspector.as_ref(),
            &[root_table.to_string()],
        )
        .await?;

        info!(tables = full_graph.len(), root = %root_table, "graph expansion: discovered tables");

        // Filter by depth and exclusion patterns
        let filtered_tables = self.filter_tables(root_table, &full_graph, refs);

        info!(
            tables = filtered_tables.len(),
            depth = ?refs.depth,
            exclude = ?refs.exclude,
            "graph expansion: tables after filtering"
        );

        let schema_ops = self
            .build_schema_ops(
                &filtered_tables,
                mapping,
                ignore_constraints,
                mapped_columns_only,
                refs.drop_constraints,
            )
            .await?;

        // Collect discovered table metadata for cascade data
        let discovered_tables: HashMap<String, TableMetadata> = filtered_tables
            .into_iter()
            .filter_map(|name| full_graph.get(&name).map(|meta| (name, meta.clone())))
            .collect();

        Ok(GraphExpansionResult {
            schema_ops,
            discovered_tables,
        })
    }

    /// Filter tables by depth from root and exclusion patterns.
    fn filter_tables(
        &self,
        root_table: &str,
        graph: &HashMap<String, TableMetadata>,
        refs: &GraphReferences,
    ) -> Vec<String> {
        let max_depth = match refs.depth {
            TraversalDepth::All => usize::MAX,
            TraversalDepth::Limited(n) => n,
        };

        // BFS from root to discover tables within depth
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut result = Vec::new();

        queue.push_back((root_table.to_string(), 0usize));
        visited.insert(root_table.to_string());

        while let Some((table, depth)) = queue.pop_front() {
            if self.is_excluded(&table, &refs.exclude) {
                continue;
            }

            result.push(table.clone());

            if depth >= max_depth {
                continue;
            }

            // Follow FK references (tables this table depends on)
            if let Some(meta) = graph.get(&table) {
                for fk in &meta.foreign_keys {
                    if !visited.contains(&fk.referenced_table)
                        && graph.contains_key(&fk.referenced_table)
                    {
                        visited.insert(fk.referenced_table.clone());
                        queue.push_back((fk.referenced_table.clone(), depth + 1));
                    }
                }

                // Also follow referencing tables (tables that point to this one)
                for ref_table_name in meta.referencing_tables.keys() {
                    if !visited.contains(ref_table_name) && graph.contains_key(ref_table_name) {
                        visited.insert(ref_table_name.clone());
                        queue.push_back((ref_table_name.clone(), depth + 1));
                    }
                }
            }
        }

        result
    }

    /// Check if a table matches any exclusion pattern.
    ///
    /// Supported pattern forms:
    /// - `audit_logs` - exact match
    /// - `audit_*`    - prefix wildcard (matches tables starting with "audit_")
    /// - `*_log`      - suffix wildcard (matches tables ending with "_log")
    /// - `*`          - matches everything
    fn is_excluded(&self, table: &str, patterns: &[String]) -> bool {
        patterns.iter().any(|pattern| {
            match (pattern.starts_with('*'), pattern.ends_with('*')) {
                // Both ends: bare `*` matches everything; `*foo*` - substring match
                (true, true) => {
                    let inner = pattern.trim_matches('*');
                    inner.is_empty() || table.contains(inner)
                }
                // Prefix wildcard: `*_log` - suffix match
                (true, false) => {
                    let suffix = &pattern[1..];
                    table.ends_with(suffix)
                }
                // Suffix wildcard: `audit_*` - prefix match
                (false, true) => {
                    let prefix = &pattern[..pattern.len() - 1];
                    table.starts_with(prefix)
                }
                // Exact match
                (false, false) => table == pattern,
            }
        })
    }

    async fn build_schema_ops(
        &self,
        tables: &[String],
        mapping: &TransformationMetadata,
        ignore_constraints: bool,
        mapped_columns_only: bool,
        drop_constraints: bool,
    ) -> Result<SchemaOps, DriverError> {
        // Augment the mapping so all discovered tables are treated as in-scope sources.
        // This ensures FKs between graph-discovered tables are not filtered out.
        let augmented = mapping.with_extra_sources(tables);

        // Plan schema for each table, merge into a unified plan
        let planner = SchemaPlanner::new(
            self.introspector.clone(),
            self.source_dialect,
            augmented,
            ignore_constraints,
            mapped_columns_only,
            (*self.type_registry).clone(),
        );

        let mut merged_plan: Option<SchemaPlan> = None;

        for table_name in tables {
            let plan = planner.plan_schema(table_name).await.map_err(|e| {
                DriverError::QueryError(format!(
                    "Failed to plan schema for table '{}': {}",
                    table_name, e
                ))
            })?;

            if let Some(ref mut merged) = merged_plan {
                merged.merge(plan);
            } else {
                merged_plan = Some(plan);
            }
        }

        // Build ops from the merged plan
        Ok(merged_plan
            .map(|mut p| {
                p.set_drop_constraints(drop_constraints);
                p.build_ops()
            })
            .unwrap_or_else(SchemaOps::empty))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_excluded_exact_match() {
        let expander = GraphExpander {
            introspector: Arc::new(MockIntrospector),
            type_registry: Arc::new(TypeRegistry::new(Dialect::MySql, Dialect::Postgres)),
            source_dialect: Dialect::MySql,
        };

        let patterns = vec!["audit_logs".to_string(), "temp_*".to_string()];
        assert!(expander.is_excluded("audit_logs", &patterns));
        assert!(expander.is_excluded("temp_data", &patterns));
        assert!(expander.is_excluded("temp_", &patterns));
        assert!(!expander.is_excluded("users", &patterns));
        assert!(!expander.is_excluded("audit_log", &patterns)); // no trailing 's'
    }

    #[test]
    fn test_is_excluded_prefix_wildcard() {
        let expander = GraphExpander {
            introspector: Arc::new(MockIntrospector),
            type_registry: Arc::new(TypeRegistry::new(Dialect::MySql, Dialect::Postgres)),
            source_dialect: Dialect::MySql,
        };

        let patterns = vec!["*_log".to_string()];
        assert!(expander.is_excluded("audit_log", &patterns));
        assert!(expander.is_excluded("error_log", &patterns));
        assert!(!expander.is_excluded("log_archive", &patterns));
        assert!(!expander.is_excluded("users", &patterns));
    }

    #[test]
    fn test_is_excluded_wildcard_all() {
        let expander = GraphExpander {
            introspector: Arc::new(MockIntrospector),
            type_registry: Arc::new(TypeRegistry::new(Dialect::MySql, Dialect::Postgres)),
            source_dialect: Dialect::MySql,
        };

        let patterns = vec!["*".to_string()];
        assert!(expander.is_excluded("anything", &patterns));
        assert!(expander.is_excluded("users", &patterns));
    }

    #[test]
    fn test_filter_tables_with_depth() {
        let expander = GraphExpander {
            introspector: Arc::new(MockIntrospector),
            type_registry: Arc::new(TypeRegistry::new(Dialect::MySql, Dialect::Postgres)),
            source_dialect: Dialect::MySql,
        };

        // Build a simple graph: orders -> customers -> addresses
        let mut graph = HashMap::new();
        graph.insert(
            "orders".to_string(),
            make_meta("orders", vec![("customers", "customer_id", "id")]),
        );
        graph.insert(
            "customers".to_string(),
            make_meta("customers", vec![("addresses", "address_id", "id")]),
        );
        graph.insert("addresses".to_string(), make_meta("addresses", vec![]));

        // Depth 1: orders + customers only
        let refs = GraphReferences {
            data_mode: model::execution::references::DataMode::SchemaOnly,
            depth: TraversalDepth::Limited(1),
            exclude: vec![],
            drop_constraints: false,
        };

        let result = expander.filter_tables("orders", &graph, &refs);
        assert!(result.contains(&"orders".to_string()));
        assert!(result.contains(&"customers".to_string()));
        assert!(!result.contains(&"addresses".to_string()));

        // Depth all: all tables
        let refs_all = GraphReferences {
            data_mode: model::execution::references::DataMode::SchemaOnly,
            depth: TraversalDepth::All,
            exclude: vec![],
            drop_constraints: false,
        };

        let result_all = expander.filter_tables("orders", &graph, &refs_all);
        assert_eq!(result_all.len(), 3);
    }

    #[test]
    fn test_filter_tables_with_exclusions() {
        let expander = GraphExpander {
            introspector: Arc::new(MockIntrospector),
            type_registry: Arc::new(TypeRegistry::new(Dialect::MySql, Dialect::Postgres)),
            source_dialect: Dialect::MySql,
        };

        let mut graph = HashMap::new();
        graph.insert(
            "orders".to_string(),
            make_meta("orders", vec![("audit_logs", "audit_id", "id")]),
        );
        graph.insert("audit_logs".to_string(), make_meta("audit_logs", vec![]));

        let refs = GraphReferences {
            data_mode: model::execution::references::DataMode::SchemaOnly,
            depth: TraversalDepth::All,
            exclude: vec!["audit_*".to_string()],
            drop_constraints: false,
        };

        let result = expander.filter_tables("orders", &graph, &refs);
        assert!(result.contains(&"orders".to_string()));
        assert!(!result.contains(&"audit_logs".to_string()));
    }

    // --- Test helpers ---

    use connectors::sql::metadata::fk::{ForeignKeyAction, ForeignKeyMetadata};

    fn make_meta(name: &str, fks: Vec<(&str, &str, &str)>) -> TableMetadata {
        let foreign_keys: Vec<ForeignKeyMetadata> = fks
            .into_iter()
            .map(|(ref_table, col, ref_col)| ForeignKeyMetadata {
                constraint_name: String::new(),
                table: name.to_string(),
                schema: String::new(),
                columns: vec![col.to_string()],
                referenced_table: ref_table.to_string(),
                referenced_schema: None,
                referenced_columns: vec![ref_col.to_string()],
                on_delete: ForeignKeyAction::NoAction,
                on_update: ForeignKeyAction::NoAction,
                nullable: false,
                deferrable: None,
                initially_deferred: None,
            })
            .collect();

        TableMetadata {
            name: name.to_string(),
            schema: None,
            columns: HashMap::new(),
            primary_keys: vec![],
            foreign_keys,
            referenced_tables: HashMap::new(),
            referencing_tables: HashMap::new(),
        }
    }

    // Minimal mock for tests that only use filter_tables / is_excluded
    struct MockIntrospector;

    impl connectors::traits::driver::Driver for MockIntrospector {
        fn info(&self) -> &connectors::traits::driver::DriverInfo {
            static INFO: connectors::traits::driver::DriverInfo =
                connectors::traits::driver::DriverInfo {
                    id: "mock",
                    name: "Mock",
                    schemes: &[],
                };
            &INFO
        }

        fn version(&self) -> &str {
            "0.0.0"
        }

        fn capabilities(&self) -> &connectors::sql::metadata::capabilities::Capabilities {
            use std::sync::LazyLock;
            static CAPS: LazyLock<connectors::sql::metadata::capabilities::Capabilities> =
                LazyLock::new(connectors::sql::metadata::capabilities::Capabilities::default);
            &CAPS
        }
    }

    #[async_trait::async_trait]
    impl SchemaIntrospector for MockIntrospector {
        async fn table_exists(&self, _table: &str) -> Result<bool, DriverError> {
            Ok(false)
        }
        async fn list_tables(&self, _schema: Option<&str>) -> Result<Vec<String>, DriverError> {
            Ok(vec![])
        }
        async fn table_metadata(&self, _table: &str) -> Result<TableMetadata, DriverError> {
            Err(DriverError::QueryError("mock: table not found".to_string()))
        }
        async fn index_metadata(
            &self,
            _table: &str,
        ) -> Result<Vec<connectors::sql::metadata::index::IndexMetadata>, DriverError> {
            Ok(vec![])
        }
        async fn fk_metadata(&self, _table: &str) -> Result<Vec<ForeignKeyMetadata>, DriverError> {
            Ok(vec![])
        }
        async fn referencing_tables(&self, _table: &str) -> Result<Vec<String>, DriverError> {
            Ok(vec![])
        }
        async fn table_size_bytes(&self, _table: &str) -> Result<u64, DriverError> {
            Ok(0)
        }
    }
}
