use super::{column::metadata::ColumnMetadata, fk::ForeignKeyMetadata};
use crate::{
    metadata::column::data_type::ColumnDataType,
    query::{column::ColumnDef, fk::ForeignKeyDef, select::SelectField},
    requests::JoinClause,
};
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
    pub fn select_fields(&self) -> HashMap<String, Vec<SelectField>> {
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
                    data_type: table.columns[col_name].data_type.to_string(),
                })
                .collect();

            grouped.insert(table.name.clone(), fields);
        }

        grouped
    }

    pub fn joins(&self) -> Vec<JoinClause> {
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

    pub fn enums(table: &TableMetadata) -> Vec<&ColumnMetadata> {
        table
            .columns
            .iter()
            .filter(|(_name, col)| col.data_type == ColumnDataType::Enum)
            .map(|(_name, col)| col)
            .collect()
    }

    pub fn tables(&self) -> Vec<TableMetadata> {
        let mut visited = HashSet::new();
        let mut tables = Vec::new();

        Self::collect_recursive_tables(self, &mut visited, &mut tables);

        tables
    }

    pub fn columns_def<F>(&self, type_converter: &F) -> Vec<ColumnDef>
    where
        F: Fn(&ColumnMetadata) -> (String, Option<usize>),
    {
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
                    default: col.default_value.as_ref().map(ToString::to_string),
                    char_max_length,
                }
            })
            .collect()
    }

    pub fn fk_defs(&self) -> Vec<ForeignKeyDef> {
        self.foreign_keys
            .iter()
            .map(|fk| ForeignKeyDef {
                column: fk.column.clone(),
                referenced_table: fk.referenced_table.clone(),
                referenced_column: fk.referenced_column.clone(),
            })
            .collect::<Vec<_>>()
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
