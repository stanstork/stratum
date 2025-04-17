use crate::metadata::table::TableMetadata;
use std::collections::{HashSet, VecDeque};

#[derive(Debug, Clone)]
pub struct Join {
    pub source_metadata: TableMetadata,
    pub join_clause: JoinClause,
}

impl Join {
    pub fn new(source_metadata: TableMetadata, join_clause: JoinClause) -> Self {
        Self {
            source_metadata,
            join_clause,
        }
    }

    pub fn collect_related_joins(root_table: String, joins: &Vec<Join>) -> Vec<Join> {
        let mut visited_tables = HashSet::new();
        let mut result_joins = Vec::new();
        let mut queue = VecDeque::new();

        visited_tables.insert(root_table.clone());
        queue.push_back(root_table.clone());

        let mut remaining_joins = joins.to_vec();

        while let Some(current) = queue.pop_front() {
            let mut still_unprocessed = Vec::new();

            for join in remaining_joins.into_iter() {
                let (next_table, matches) =
                    if join.join_clause.left.table.eq_ignore_ascii_case(&current)
                        && !visited_tables.contains(&join.join_clause.right.table)
                    {
                        (Some(join.join_clause.right.clone()), true)
                    } else if join.join_clause.right.table.eq_ignore_ascii_case(&current)
                        && !visited_tables.contains(&join.join_clause.left.table)
                    {
                        (Some(join.join_clause.left.clone()), true)
                    } else if visited_tables.contains(&join.join_clause.left.table)
                        && visited_tables.contains(&join.join_clause.right.table)
                    {
                        // Already visited both sides, still valid join
                        (None, true)
                    } else {
                        (None, false)
                    };

                if matches {
                    result_joins.push(join.clone());
                    if let Some(next) = next_table {
                        if visited_tables.insert(next.table.clone()) {
                            queue.push_back(next.table);
                        }
                    }
                } else {
                    still_unprocessed.push(join);
                }
            }

            remaining_joins = still_unprocessed;
        }

        result_joins
    }
}

#[derive(Debug, Clone)]
pub struct JoinClause {
    pub left: JoinedTable,
    pub right: JoinedTable,
    pub join_type: JoinType,
    pub conditions: Vec<JoinCondition>,
    pub fields: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct JoinedTable {
    pub table: String,
    pub alias: String,
}

#[derive(Debug, Clone)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

#[derive(Debug, Clone)]
pub struct JoinCondition {
    pub left: JoinColumn,
    pub right: JoinColumn,
}

#[derive(Debug, Clone)]
pub struct JoinColumn {
    pub alias: String,
    pub column: String,
}
