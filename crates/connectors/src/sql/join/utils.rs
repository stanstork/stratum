use super::clause::{JoinClause, JoinColumn, JoinCondition, JoinType, JoinedTable};
use crate::sql::metadata::table::TableMetadata;
use std::collections::{HashMap, HashSet, VecDeque};
use tracing::warn;

/// Given a graph of TableMetadata keyed by table‐name, find a path
/// (as a Vec of table‐names) from `start` to `target`, traversing
/// both referenced_tables and referencing_tables edges.
pub fn find_join_path(
    graph: &HashMap<String, TableMetadata>,
    start: &str,
    target: &str,
) -> Option<Vec<String>> {
    // early exit if start or target missing
    if !graph.contains_key(start) || !graph.contains_key(target) {
        return None;
    }

    let mut visited: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<Vec<String>> = VecDeque::new();

    // start BFS from `start`
    visited.insert(start.to_string());
    queue.push_back(vec![start.to_string()]);

    while let Some(path) = queue.pop_front() {
        let last = path.last().unwrap();
        if last.eq_ignore_ascii_case(target) {
            return Some(path);
        }

        // look up the full metadata for `last`
        if let Some(meta) = graph.get(last) {
            // gather all neighbor names
            let neighbors = meta
                .referenced_tables
                .keys()
                .chain(meta.referencing_tables.keys());

            for nbr in neighbors {
                if visited.insert(nbr.clone()) {
                    let mut new_path = path.clone();
                    new_path.push(nbr.clone());
                    queue.push_back(new_path);
                }
            }
        }
    }

    // no route found
    None
}

/// Given a Vec of paths (each path is a Vec of table‐names),
/// combine them into a single Vec of table‐names, removing
/// duplicates and skipping the root table.
pub fn combine_join_paths(paths: Vec<Vec<String>>, root: &str) -> Vec<String> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();

    for path in paths {
        // skip the root table on this path
        for table in path.into_iter().skip_while(|t| t == root) {
            // `skip_while` above will drop "products" and anything before it—
            // so for ["products","order_items",…] it yields "order_items" onward.
            if seen.insert(table.clone()) {
                out.push(table);
            }
        }
    }

    out
}

/// Build a `Vec<JoinClause>` that steps from `root` through `path`,
/// looking up FK metadata in `graph` to wire up ON conditions.
/// Supports composite foreign keys with multiple columns.
pub fn build_join_clauses(
    root: &str,
    path: &[String],
    graph: &HashMap<String, TableMetadata>,
    join_type: JoinType,
) -> Vec<JoinClause> {
    let mut clauses = Vec::new();
    let mut current_table = root.to_string();
    let mut current_alias = root.to_string();

    for next_table in path {
        let next_alias = next_table.clone();
        let mut conditions = Vec::new();

        // Try next_table -> current_table FK
        if let Some(next_meta) = graph.get(next_table)
            && let Some(fk) = next_meta
                .foreign_keys
                .iter()
                .find(|fk| fk.referenced_table.eq_ignore_ascii_case(&current_table))
        {
            // Create join conditions for all column pairs in the FK
            for (col, ref_col) in fk.columns.iter().zip(fk.referenced_columns.iter()) {
                conditions.push(JoinCondition {
                    left: JoinColumn {
                        alias: next_alias.clone(),
                        column: col.clone(),
                    },
                    right: JoinColumn {
                        alias: current_alias.clone(),
                        column: ref_col.clone(),
                    },
                });
            }
        }

        // Otherwise current_table -> next_table FK
        if conditions.is_empty()
            && let Some(cur_meta) = graph.get(&current_table)
            && let Some(fk) = cur_meta
                .foreign_keys
                .iter()
                .find(|fk| fk.referenced_table.eq_ignore_ascii_case(next_table))
        {
            // Create join conditions for all column pairs in the FK
            for (col, ref_col) in fk.columns.iter().zip(fk.referenced_columns.iter()) {
                conditions.push(JoinCondition {
                    left: JoinColumn {
                        alias: next_alias.clone(),
                        column: ref_col.clone(),
                    },
                    right: JoinColumn {
                        alias: current_alias.clone(),
                        column: col.clone(),
                    },
                });
            }
        }

        if conditions.is_empty() {
            warn!(current = %current_table, next = %next_table, "no FK relation between tables in schema");
            return Vec::new();
        }

        clauses.push(JoinClause {
            left: JoinedTable {
                table: next_table.clone(),
                alias: next_alias.clone(),
            },
            right: JoinedTable {
                table: current_table.clone(),
                alias: current_alias.clone(),
            },
            join_type: join_type.clone(),
            conditions,
        });

        current_table = next_table.clone();
        current_alias = next_alias;
    }

    clauses
}

#[cfg(test)]
mod tests {
    use crate::sql::metadata::fk::{ForeignKeyAction, ForeignKeyMetadata};

    use super::*;

    fn create_test_table(name: &str, fks: Vec<ForeignKeyMetadata>) -> TableMetadata {
        TableMetadata {
            name: name.to_string(),
            schema: None,
            columns: Default::default(),
            primary_keys: vec![],
            foreign_keys: fks,
            referenced_tables: Default::default(),
            referencing_tables: Default::default(),
        }
    }

    #[test]
    fn test_build_join_clauses_single_column_fk() {
        let mut graph = HashMap::new();

        // orders -> users (single column FK)
        let orders_fk = ForeignKeyMetadata {
            constraint_name: "fk_user".to_string(),
            table: "orders".to_string(),
            schema: "public".to_string(),
            columns: vec!["user_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_schema: None,
            referenced_columns: vec!["id".to_string()],
            on_delete: ForeignKeyAction::NoAction,
            on_update: ForeignKeyAction::NoAction,
            nullable: false,
            deferrable: None,
            initially_deferred: None,
        };

        graph.insert("users".to_string(), create_test_table("users", vec![]));
        graph.insert(
            "orders".to_string(),
            create_test_table("orders", vec![orders_fk]),
        );

        let path = vec!["orders".to_string()];
        let clauses = build_join_clauses("users", &path, &graph, JoinType::Left);

        assert_eq!(clauses.len(), 1);
        assert_eq!(clauses[0].conditions.len(), 1);
        // JOIN condition: orders.user_id = users.id
        assert_eq!(clauses[0].conditions[0].left.alias, "orders");
        assert_eq!(clauses[0].conditions[0].left.column, "user_id");
        assert_eq!(clauses[0].conditions[0].right.alias, "users");
        assert_eq!(clauses[0].conditions[0].right.column, "id");
    }

    #[test]
    fn test_build_join_clauses_composite_fk() {
        let mut graph = HashMap::new();

        // orders -> users (composite FK: tenant_id + user_id)
        let orders_fk = ForeignKeyMetadata {
            constraint_name: "fk_user".to_string(),
            table: "orders".to_string(),
            schema: "public".to_string(),
            columns: vec!["tenant_id".to_string(), "user_id".to_string()],
            referenced_table: "users".to_string(),
            referenced_schema: None,
            referenced_columns: vec!["tenant_id".to_string(), "id".to_string()],
            on_delete: ForeignKeyAction::NoAction,
            on_update: ForeignKeyAction::NoAction,
            nullable: false,
            deferrable: None,
            initially_deferred: None,
        };

        graph.insert("users".to_string(), create_test_table("users", vec![]));
        graph.insert(
            "orders".to_string(),
            create_test_table("orders", vec![orders_fk]),
        );

        let path = vec!["orders".to_string()];
        let clauses = build_join_clauses("users", &path, &graph, JoinType::Left);

        assert_eq!(clauses.len(), 1);
        assert_eq!(
            clauses[0].conditions.len(),
            2,
            "Should have 2 join conditions for composite FK"
        );

        // First condition: orders.tenant_id = users.tenant_id
        assert_eq!(clauses[0].conditions[0].left.alias, "orders");
        assert_eq!(clauses[0].conditions[0].left.column, "tenant_id");
        assert_eq!(clauses[0].conditions[0].right.alias, "users");
        assert_eq!(clauses[0].conditions[0].right.column, "tenant_id");

        // Second condition: orders.user_id = users.id
        assert_eq!(clauses[0].conditions[1].left.alias, "orders");
        assert_eq!(clauses[0].conditions[1].left.column, "user_id");
        assert_eq!(clauses[0].conditions[1].right.alias, "users");
        assert_eq!(clauses[0].conditions[1].right.column, "id");
    }

    #[test]
    fn test_find_join_path() {
        let mut graph = HashMap::new();

        let users_fk = ForeignKeyMetadata {
            constraint_name: "fk_org".to_string(),
            table: "users".to_string(),
            schema: "public".to_string(),
            columns: vec!["org_id".to_string()],
            referenced_table: "organizations".to_string(),
            referenced_schema: None,
            referenced_columns: vec!["id".to_string()],
            on_delete: ForeignKeyAction::NoAction,
            on_update: ForeignKeyAction::NoAction,
            nullable: false,
            deferrable: None,
            initially_deferred: None,
        };

        let mut users_table = create_test_table("users", vec![users_fk]);
        let orgs_table = create_test_table("organizations", vec![]);

        // Add referenced_tables link
        users_table
            .referenced_tables
            .insert("organizations".to_string(), orgs_table.clone());

        graph.insert("users".to_string(), users_table);
        graph.insert("organizations".to_string(), orgs_table);

        let path = find_join_path(&graph, "users", "organizations");
        assert!(path.is_some());
        assert_eq!(path.unwrap(), vec!["users", "organizations"]);
    }
}
