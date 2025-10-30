use super::clause::JoinClause;
use crate::sql::base::{metadata::table::TableMetadata, query::select::SelectField};
use model::transform::mapping::EntityMapping;
use std::collections::{HashMap, HashSet, VecDeque};

#[derive(Debug, Clone)]
pub struct JoinSource {
    pub meta: HashMap<String, TableMetadata>,
    pub clauses: Vec<JoinClause>,
    pub mapping: EntityMapping,
    pub projection: HashMap<String, Vec<String>>,
}

impl JoinSource {
    pub fn new(
        meta: HashMap<String, TableMetadata>,
        clauses: Vec<JoinClause>,
        projection: HashMap<String, Vec<String>>,
        mapping: EntityMapping,
    ) -> Self {
        Self {
            meta,
            clauses,
            mapping,
            projection,
        }
    }

    pub fn fields(&self) -> Vec<SelectField> {
        self.clauses
            .iter()
            .flat_map(|clause| {
                let left_alias = clause.left.alias.clone();

                // fetch the map of table -> fields, then get table's Vec<SelectField>
                let source_fields = self
                    .meta
                    .get(&clause.left.table)
                    .map(|m| m.select_fields_rec())
                    .unwrap_or_default()
                    .get(&clause.left.table)
                    .cloned()
                    .unwrap_or_default();

                source_fields
                    .into_iter()
                    .map(|mut field| {
                        // override the field's table with alias
                        field.table = left_alias.clone();

                        // apply any lookup aliases
                        if let Some(alias) = self
                            .mapping
                            .get_lookups_for(&field.table)
                            .iter()
                            .find_map(|lk| {
                                (lk.key.eq_ignore_ascii_case(&field.column))
                                    .then(|| lk.target.clone())
                            })
                        {
                            field.alias = alias;
                        }

                        field
                    })
                    // filter out anything not explicitly projected
                    .filter(|field| {
                        self.projection
                            .get(&clause.left.table)
                            .is_some_and(|fields| {
                                fields
                                    .iter()
                                    .any(|col| col.eq_ignore_ascii_case(&field.column))
                            })
                    })
                    .collect::<Vec<SelectField>>()
            })
            .collect()
    }

    pub fn related_joins(&self, root_table: String) -> Vec<JoinClause> {
        let mut visited = HashSet::new();
        let mut result_joins = Vec::new();
        let mut queue = VecDeque::new();

        visited.insert(root_table.clone());
        queue.push_back(root_table.clone());

        let mut remaining = self.clauses.to_vec();

        while let Some(current) = queue.pop_front() {
            let mut unprocessed = Vec::new();

            for join in remaining.into_iter() {
                let (next_table, matches) = if join.left.table.eq_ignore_ascii_case(&current)
                    && !visited.contains(&join.right.table)
                {
                    (Some(join.right.clone()), true)
                } else if join.right.table.eq_ignore_ascii_case(&current)
                    && !visited.contains(&join.left.table)
                {
                    (Some(join.left.clone()), true)
                } else if visited.contains(&join.left.table) && visited.contains(&join.right.table)
                {
                    // Already visited both sides, still valid join
                    (None, true)
                } else {
                    (None, false)
                };

                if matches {
                    if !join.left.table.eq_ignore_ascii_case(&root_table) {
                        // Only add joins that are not the root table
                        result_joins.push(join.clone());
                    }

                    if let Some(next) = next_table {
                        if visited.insert(next.table.clone()) {
                            queue.push_back(next.table);
                        }
                    }
                } else {
                    unprocessed.push(join);
                }
            }

            remaining = unprocessed;
        }

        result_joins
    }
}
