use super::{column::ColumnMetadata, fk::ForeignKeyMetadata};
use crate::sql::query::{column::ColumnDef, fk::ForeignKeyDef, select::SelectField};
use model::core::types::Type;
use serde::Serialize;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Serialize)]
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
    pub fn select_fields(&self) -> Vec<SelectField> {
        self.columns
            .keys()
            .map(|col_name| SelectField {
                table: self.name.clone(),
                alias: Some(col_name.clone()),
                column: col_name.clone(),
                data_type: self.columns[col_name].data_type.clone(),
            })
            .collect()
    }

    pub fn select_fields_rec(&self) -> HashMap<String, Vec<SelectField>> {
        let mut visited = HashSet::new();
        let mut tables = Vec::new();

        Self::collect_recursive_tables(self, &mut visited, &mut tables);

        let mut grouped = HashMap::new();

        for table in tables {
            let fields: Vec<SelectField> = table
                .columns
                .keys()
                .map(|col_name| SelectField {
                    table: table.name.clone(),
                    alias: Some(col_name.clone()),
                    column: col_name.clone(),
                    data_type: table.columns[col_name].data_type.clone(),
                })
                .collect();

            grouped.insert(table.name.clone(), fields);
        }

        grouped
    }

    pub fn enums(table: &TableMetadata) -> Vec<ColumnMetadata> {
        table
            .columns
            .iter()
            .filter(|(_name, col)| col.data_type.eq_ignore_ascii_case("enum"))
            .map(|(_name, col)| col.clone())
            .collect()
    }

    pub fn tables(&self) -> Vec<TableMetadata> {
        let mut visited = HashSet::new();
        let mut tables = Vec::new();

        Self::collect_recursive_tables(self, &mut visited, &mut tables);

        tables
    }

    pub fn column_defs<T: Fn(&ColumnMetadata) -> (Type, Option<usize>)>(
        &self,
        type_converter: &T,
    ) -> Vec<ColumnDef> {
        // Sort columns by ordinal to ensure consistent order of columns
        // in the output regardless of the order in which they were added to the HashMap
        let mut columns = self.columns.iter().collect::<Vec<_>>();
        columns.sort_by_key(|(_, col)| col.ordinal);

        columns
            .into_iter()
            .map(|(name, col)| {
                let (data_type, char_max_length) = type_converter(col);
                ColumnDef {
                    name: name.clone(),
                    data_type,
                    is_nullable: col.is_nullable,
                    is_primary_key: self.primary_keys.contains(name),
                    default: col.default_value.clone(),
                    char_max_length,
                    generated_expression: col.generated_expression.clone(),
                    is_stored: col.is_stored,
                    is_generated: col.is_generated,
                }
            })
            .collect()
    }

    pub fn fk_defs(&self) -> Vec<ForeignKeyDef> {
        self.foreign_keys
            .iter()
            .map(|fk| ForeignKeyDef {
                constraint_name: Some(fk.constraint_name.clone()),
                columns: fk.columns.clone(),
                referenced_table: fk.referenced_table.clone(),
                referenced_columns: fk.referenced_columns.clone(),
                on_delete: fk.on_delete.clone(),
                on_update: fk.on_update.clone(),
            })
            .collect()
    }

    pub fn column(&self, name: &str) -> Option<&ColumnMetadata> {
        self.columns.get(name)
    }

    pub fn columns(&self) -> Vec<ColumnMetadata> {
        self.columns.values().cloned().collect()
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

    pub fn set_name(&mut self, name: &str) {
        self.name = name.to_owned();
    }

    pub fn get_column(&self, name: &str) -> Option<&ColumnMetadata> {
        self.columns.get(name)
    }

    pub fn is_valid(&self) -> bool {
        !self.name.is_empty() && !self.columns.is_empty()
    }

    fn collect_recursive_tables(
        metadata: &TableMetadata,
        visited: &mut HashSet<String>,
        tables: &mut Vec<TableMetadata>,
    ) {
        if !visited.insert(metadata.name.clone()) {
            return;
        }

        tables.push(metadata.clone());

        for table in metadata.referenced_tables.values() {
            Self::collect_recursive_tables(table, visited, tables);
        }

        for table in metadata.referencing_tables.values() {
            Self::collect_recursive_tables(table, visited, tables);
        }
    }
}
