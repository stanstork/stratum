use super::clause::JoinClause;
use crate::{metadata::table::TableMetadata, query::select::SelectField};
use common::mapping::{FieldMappings, FieldNameMap};
use smql::statements::expr::Expression;
use std::collections::{HashSet, VecDeque};

#[derive(Debug, Clone)]
pub struct JoinSource {
    pub clause: JoinClause,
    pub metadata: TableMetadata,
    pub projection: Vec<String>,
    pub field_name_map: FieldNameMap,
    pub field_mappings: FieldMappings,
}

impl JoinSource {
    pub fn new(
        metadata: TableMetadata,
        clause: JoinClause,
        projection: Vec<String>,
        field_name_map: FieldNameMap,
        field_mappings: FieldMappings,
    ) -> Self {
        Self {
            metadata,
            clause,
            projection,
            field_name_map,
            field_mappings,
        }
    }

    pub fn related_joins(root_table: String, joins: &Vec<JoinSource>) -> Vec<JoinSource> {
        let mut visited = HashSet::new();
        let mut result_joins = Vec::new();
        let mut queue = VecDeque::new();

        visited.insert(root_table.clone());
        queue.push_back(root_table.clone());

        let mut remaining = joins.to_vec();

        while let Some(current) = queue.pop_front() {
            let mut unprocessed = Vec::new();

            for join in remaining.into_iter() {
                let (next_table, matches) = if join.clause.left.table.eq_ignore_ascii_case(&current)
                    && !visited.contains(&join.clause.right.table)
                {
                    (Some(join.clause.right.clone()), true)
                } else if join.clause.right.table.eq_ignore_ascii_case(&current)
                    && !visited.contains(&join.clause.left.table)
                {
                    (Some(join.clause.left.clone()), true)
                } else if visited.contains(&join.clause.left.table)
                    && visited.contains(&join.clause.right.table)
                {
                    // Already visited both sides, still valid join
                    (None, true)
                } else {
                    (None, false)
                };

                if matches {
                    result_joins.push(join.clone());
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

    pub fn select_fields(&self, table: &str) -> Vec<SelectField> {
        let left_alias = &self.clause.left.alias;
        let source_fields = self
            .metadata
            .select_fields()
            .get(&self.metadata.name)
            .cloned()
            .unwrap_or_default();

        let binding = vec![];
        let computed_fields = self
            .field_mappings
            .get_computed(&self.field_name_map.resolve(table))
            .unwrap_or(&binding);

        source_fields
            .into_iter()
            .filter_map(|mut field| {
                field.table = left_alias.clone();

                if let Some(alias) = computed_fields.iter().find_map(|cf| match &cf.expression {
                    Expression::Lookup { table, key, .. }
                        if table.eq_ignore_ascii_case(&field.table)
                            && key.eq_ignore_ascii_case(&field.column) =>
                    {
                        Some(cf.name.clone())
                    }
                    _ => None,
                }) {
                    field.alias = Some(alias);
                }

                println!("Field: {:?}", field);

                if self
                    .projection
                    .iter()
                    .any(|f| f.eq_ignore_ascii_case(&field.column))
                {
                    Some(field)
                } else {
                    None
                }
            })
            .collect()
    }
}
