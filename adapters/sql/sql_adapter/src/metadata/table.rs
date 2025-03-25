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

    pub fn collect_select_columns(&self) -> HashMap<String, Vec<SelectColumn>> {
        fn collect_recursive(
            metadata: &TableMetadata,
            visited: &mut HashSet<String>,
            grouped: &mut HashMap<String, Vec<SelectColumn>>,
        ) {
            if !visited.insert(metadata.name.clone()) {
                return;
            }

            let columns: Vec<SelectColumn> = metadata
                .columns
                .keys()
                .map(|col_name| SelectColumn {
                    table: metadata.name.clone(),
                    alias: Some(format!("{}_{}", metadata.name, col_name)),
                    column: col_name.clone(),
                })
                .collect();

            grouped.insert(metadata.name.clone(), columns);

            for table in metadata.referenced_tables.values() {
                collect_recursive(table, visited, grouped);
            }

            for table in metadata.referencing_tables.values() {
                collect_recursive(table, visited, grouped);
            }
        }

        let mut visited = HashSet::new();
        let mut grouped = HashMap::new();

        collect_recursive(self, &mut visited, &mut grouped);

        grouped
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

        for ref_table in table.referenced_tables.values() {
            Self::print_tables_tree(ref_table, indent + 1, visited);
        }

        if !table.referencing_tables.is_empty() {
            println!("{}  Referenced by:", "  ".repeat(indent));
            for ref_table in table.referencing_tables.values() {
                println!("{}  - {}", "  ".repeat(indent), ref_table.name);
            }
        }
    }
}
