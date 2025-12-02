use super::clause::{JoinClause, JoinColumn, JoinCondition, JoinType, JoinedTable};
use crate::sql::base::metadata::table::TableMetadata;
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
            conditions.push(JoinCondition {
                left: JoinColumn {
                    alias: next_alias.clone(),
                    column: fk.column.clone(),
                },
                right: JoinColumn {
                    alias: current_alias.clone(),
                    column: fk.referenced_column.clone(),
                },
            });
        }

        // Otherwise current_table -> next_table FK
        if conditions.is_empty()
            && let Some(cur_meta) = graph.get(&current_table)
            && let Some(fk) = cur_meta
                .foreign_keys
                .iter()
                .find(|fk| fk.referenced_table.eq_ignore_ascii_case(next_table))
        {
            conditions.push(JoinCondition {
                left: JoinColumn {
                    alias: next_alias.clone(),
                    column: fk.referenced_column.clone(),
                },
                right: JoinColumn {
                    alias: current_alias.clone(),
                    column: fk.column.clone(),
                },
            });
        }

        if conditions.is_empty() {
            warn!(
                "No FK relation between `{}` and `{}` in schema",
                current_table, next_table
            );
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
