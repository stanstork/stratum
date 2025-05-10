use crate::metadata::table::TableMetadata;
use clause::{JoinClause, JoinColumn, JoinCondition, JoinType, JoinedTable};
use std::collections::{HashMap, HashSet, VecDeque};

pub mod clause;
pub mod field;
pub mod source;

pub fn join_path_clauses(
    graph: &HashMap<String, TableMetadata>,
    root: &str,
    target: &str,
) -> Option<Vec<JoinClause>> {
    let mut seen = HashSet::new();
    let mut queue = VecDeque::new();

    // For each discovered table, remember (parent_table, fk)
    let mut prev = HashMap::new();

    seen.insert(root.to_lowercase());
    queue.push_back(root.to_lowercase());

    while let Some(curr) = queue.pop_front() {
        if curr.eq_ignore_ascii_case(target) {
            break;
        }

        // Outgoing edges: curr -> fk.referenced_table
        if let Some(meta) = graph.get(&curr) {
            for fk in &meta.foreign_keys {
                let child = fk.referenced_table.to_lowercase();
                if seen.insert(child.clone()) {
                    prev.insert(child.clone(), (curr.clone(), fk.clone()));
                    queue.push_back(child);
                }
            }
        }

        // Incoming edges: for every table that references curr
        for (tbl, meta) in graph {
            for fk in &meta.foreign_keys {
                if fk.referenced_table.eq_ignore_ascii_case(&curr) {
                    let child = tbl.to_lowercase();
                    if seen.insert(child.clone()) {
                        // note: parent = `curr`, fk comes from `tbl`
                        prev.insert(child.clone(), (curr.clone(), fk.clone()));
                        queue.push_back(child);
                    }
                }
            }
        }
    }

    // If we never reached `target`, no path exists
    if !seen.contains(&target.to_lowercase()) {
        return None;
    }

    let mut clauses = Vec::new();
    let mut table = target.to_lowercase();

    while table != root.to_lowercase() {
        let (parent, fk) = prev.remove(&table).unwrap();

        let parent_alias = parent.clone();
        let child_alias = table.clone();

        let clause = JoinClause {
            left: JoinedTable {
                table: parent.clone(),
                alias: parent_alias.clone(),
            },
            right: JoinedTable {
                table: child_alias.clone(),
                alias: child_alias.clone(),
            },
            join_type: JoinType::Inner,
            conditions: vec![JoinCondition {
                left: JoinColumn {
                    alias: parent_alias.clone(),
                    column: fk.referenced_column.clone(),
                },
                right: JoinColumn {
                    alias: child_alias.clone(),
                    column: fk.column.clone(),
                },
            }],
        };

        clauses.push(clause);
        table = parent;
    }

    clauses.reverse();
    Some(clauses)
}
