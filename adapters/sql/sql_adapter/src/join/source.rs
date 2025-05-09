use super::clause::JoinClause;
use crate::{metadata::table::TableMetadata, query::select::SelectField};
use common::mapping::EntityMapping;
use std::collections::HashMap;

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
            .map(|clause| {
                let left_alias = clause.left.alias.clone();

                // fetch the map of table -> fields, then get table’s Vec<SelectField>
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
                        // override the field’s table with alias
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
                            field.alias = Some(alias);
                        }

                        field
                    })
                    // filter out anything not explicitly projected
                    .filter(|field| {
                        self.projection
                            .get(&clause.left.table)
                            .map_or(false, |fields| {
                                fields
                                    .iter()
                                    .any(|col| col.eq_ignore_ascii_case(&field.column))
                            })
                    })
                    .collect::<Vec<SelectField>>()
            })
            .flatten()
            .collect()
    }

    pub fn select_fields(&self, table: &str) -> Vec<SelectField> {
        self.clauses
            // find the first join‐clause matching table
            .iter()
            .find(|clause| clause.left.table.eq_ignore_ascii_case(table))
            // if none, return empty Vec
            .map_or_else(Vec::new, |clause| {
                let left_alias = clause.left.alias.clone();

                // fetch the map of table -> fields, then get table’s Vec<SelectField>
                let source_fields = self
                    .meta
                    .get(&clause.left.table)
                    .map(|m| m.select_fields_rec())
                    .unwrap_or_default()
                    .get(table)
                    .cloned()
                    .unwrap_or_default();

                source_fields
                    .into_iter()
                    .map(|mut field| {
                        // override the field’s table with alias
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
                            field.alias = Some(alias);
                        }

                        field
                    })
                    // filter out anything not explicitly projected
                    .filter(|field| {
                        self.projection
                            .get(&clause.left.table)
                            .map_or(false, |fields| {
                                fields
                                    .iter()
                                    .any(|col| col.eq_ignore_ascii_case(&field.column))
                            })
                    })
                    .collect()
            })
    }
}
