use super::{column::metadata::ColumnMetadata, foreign_key::ForeignKeyMetadata};
use crate::{query::builder::SelectColumn, requests::JoinClause};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct TableMetadata {
    pub name: String,
    pub schema: Option<String>,
    pub columns: HashMap<String, ColumnMetadata>,
    pub primary_keys: Vec<String>,
    pub foreign_keys: Vec<ForeignKeyMetadata>,
    pub referenced_tables: HashMap<String, TableMetadata>,
    pub referencing_tables: HashMap<String, TableMetadata>,
}

impl TableMetadata {
    pub fn collect_columns(&self) -> Vec<SelectColumn> {
        let mut columns: Vec<SelectColumn> = self
            .columns
            .iter()
            .map(|(col_name, _)| SelectColumn {
                table: self.name.clone(),
                alias: Some(self.name.clone()),
                column: col_name.clone(),
            })
            .collect();

        for fk in &self.foreign_keys {
            if let Some(parent_metadata) = self.referenced_tables.get(&fk.referenced_table) {
                let parent_columns = parent_metadata
                    .columns
                    .iter()
                    .map(|(col_name, _)| SelectColumn {
                        table: fk.referenced_table.clone(),
                        alias: Some(fk.referenced_table.clone()),
                        column: col_name.clone(),
                    })
                    .collect::<Vec<_>>();

                columns.extend(parent_columns);
            }
        }

        columns
    }

    pub fn collect_joins(&self) -> Vec<JoinClause> {
        self.foreign_keys
            .iter()
            .filter_map(|fk| {
                self.referenced_tables
                    .get(&fk.referenced_table)
                    .map(|_| JoinClause {
                        table: fk.referenced_table.clone(),
                        alias: fk.referenced_table.clone(),
                        from_alias: self.name.clone(),
                        from_col: fk.column.clone(),
                        to_col: fk.referenced_column.clone(),
                        join_type: "LEFT".to_string(),
                    })
            })
            .collect()
    }
}
