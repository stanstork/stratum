use crate::metadata::{fk::ForeignKeyMetadata, table::TableMetadata};
use clause::{JoinClause, JoinColumn, JoinCondition, JoinType, JoinedTable};
use std::collections::{HashMap, HashSet, VecDeque};

pub mod clause;
pub mod field;
pub mod source;

struct PrevRec {
    /// The table we came _from_ (parent in the join path).
    parent: String,
    /// The table we discovered (the referencing side of the FK).
    child: String,
    /// The FK metadata we used to traverse.
    fk: ForeignKeyMetadata,
}

pub fn join_path_clauses(
    graph: &HashMap<String, TableMetadata>,
    root: &str,
    target: &str,
) -> Option<Vec<JoinClause>> {
    let root_lc = root.to_lowercase();
    let target_lc = target.to_lowercase();

    // BFS state: visited set, queue, and backpointers
    let mut seen = HashSet::new();
    let mut queue = VecDeque::new();
    let mut prev = HashMap::new();

    seen.insert(root_lc.clone());
    queue.push_back(root_lc.clone());

    // 1) BFS until we find `target`
    while let Some(curr) = queue.pop_front() {
        if curr == target_lc {
            break;
        }

        // (a) Traverse outgoing FKs: curr → referenced_table
        if let Some(meta) = graph.get(&curr) {
            for fk in &meta.foreign_keys {
                let child = fk.referenced_table.to_lowercase();
                if seen.insert(child.clone()) {
                    prev.insert(
                        child.clone(),
                        PrevRec {
                            parent: curr.clone(),
                            child: child.clone(),
                            fk: fk.clone(),
                        },
                    );
                    queue.push_back(child);
                }
            }
        }

        // (b) Traverse incoming FKs: tables that reference `curr`
        for (tbl, meta) in graph {
            for fk in &meta.foreign_keys {
                if fk.referenced_table.eq_ignore_ascii_case(&curr) {
                    let child = tbl.to_lowercase();
                    if seen.insert(child.clone()) {
                        prev.insert(
                            child.clone(),
                            PrevRec {
                                parent: curr.clone(),
                                child: child.clone(),
                                fk: fk.clone(),
                            },
                        );
                        queue.push_back(child);
                    }
                }
            }
        }
    }

    // If we never discovered `target`, there is no path
    if !seen.contains(&target_lc) {
        return None;
    }

    // 2) Reconstruct the path of JoinClauses from `target` back to `root`
    let mut path = Vec::new();
    let mut node = target_lc.clone();
    while node != root_lc {
        let rec = prev.remove(&node).unwrap();

        // Build the clause: left = parent table, right = child table
        let clause = JoinClause {
            left: JoinedTable {
                table: rec.parent.clone(),
                alias: rec.parent.clone(),
            },
            right: JoinedTable {
                table: rec.child.clone(),
                alias: rec.child.clone(),
            },
            join_type: JoinType::Inner,
            conditions: vec![JoinCondition {
                left: JoinColumn {
                    alias: rec.parent.clone(),
                    column: rec.fk.column.clone(),
                },
                right: JoinColumn {
                    alias: rec.child.clone(),
                    column: rec.fk.referenced_column.clone(),
                },
            }],
        };

        path.push(clause);
        node = rec.parent;
    }

    // 3) Reverse so the clauses go root→...→target
    path.reverse();
    Some(path)
}
