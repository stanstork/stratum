use super::{column::metadata::ColumnMetadata, foreign_key::ForeignKeyMetadata};
use crate::metadata::column::data_type::ColumnDataType;
use crate::query::builder::{ColumnInfo, ForeignKeyInfo};
use crate::{query::builder::SelectColumn, requests::JoinClause};
use std::collections::{HashMap, HashSet};

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
    pub fn to_column_definitions<F>(&self, type_converter: &F) -> Vec<ColumnInfo>
    where
        F: Fn(&ColumnMetadata) -> String,
    {
        self.columns
            .iter()
            .map(|(name, col)| ColumnInfo {
                name: name.clone(),
                data_type: type_converter(col),
                is_nullable: col.is_nullable,
                is_primary_key: self.primary_keys.contains(name),
                default: col.default_value.as_ref().map(ToString::to_string),
            })
            .collect::<Vec<_>>()
    }

    pub fn to_fk_definitions(&self) -> Vec<ForeignKeyInfo> {
        self.foreign_keys
            .iter()
            .map(|fk| ForeignKeyInfo {
                column: fk.column.clone(),
                referenced_table: fk.referenced_table.clone(),
                referenced_column: fk.referenced_column.clone(),
            })
            .collect::<Vec<_>>()
    }

    pub fn collect_select_columns(&self) -> Vec<SelectColumn> {
        let mut columns: Vec<SelectColumn> = self
            .columns
            .keys()
            .map(|col_name| SelectColumn {
                table: self.name.clone(),
                alias: Some(self.name.clone()),
                column: col_name.clone(),
            })
            .collect();

        for fk in &self.foreign_keys {
            if let Some(parent_metadata) = self.referenced_tables.get(&fk.referenced_table) {
                let parent_columns = parent_metadata
                    .columns
                    .keys()
                    .map(|col_name| SelectColumn {
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

    pub fn collect_enum_types(table: &TableMetadata) -> Vec<&ColumnMetadata> {
        table
            .columns
            .iter()
            .filter(|(_name, col)| col.data_type == ColumnDataType::Enum)
            .map(|(_name, col)| col)
            .collect()
    }

    pub fn print_tables_tree(table: &TableMetadata, indent: usize, visited: &mut HashSet<String>) {
        if visited.contains(&table.name) {
            println!("{}- {} ", "  ".repeat(indent), table.name);
            return;
        }

        visited.insert(table.name.clone());

        println!("{}- {}", "  ".repeat(indent), table.name);

        for (_, ref_table) in &table.referenced_tables {
            Self::print_tables_tree(ref_table, indent + 1, visited);
        }

        if !table.referencing_tables.is_empty() {
            println!("{}  Referenced by:", "  ".repeat(indent));
            for (_, referencing_table) in &table.referencing_tables {
                println!("{}  - {}", "  ".repeat(indent), referencing_table.name);
            }
        }
    }
}
