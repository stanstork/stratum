use super::field::FieldMetadata;
use csv::metadata::CsvMetadata;
use serde::Serialize;
use sql_adapter::metadata::table::TableMetadata;

#[derive(Debug, Clone, Serialize)]
pub enum EntityMetadata {
    Table(TableMetadata),
    Csv(CsvMetadata),
}

impl EntityMetadata {
    pub fn name(&self) -> String {
        match self {
            EntityMetadata::Table(table) => table.name.clone(),
            EntityMetadata::Csv(csv) => csv.name.clone(),
        }
    }

    pub fn columns(&self) -> Vec<FieldMetadata> {
        match self {
            EntityMetadata::Table(table) => table
                .columns
                .values()
                .map(|col| FieldMetadata::Sql(col.clone()))
                .collect(),
            EntityMetadata::Csv(csv) => csv
                .columns
                .iter()
                .map(|col| FieldMetadata::Csv(col.clone()))
                .collect(),
        }
    }

    pub fn column(&self, name: &str) -> Option<FieldMetadata> {
        match self {
            EntityMetadata::Table(table) => table
                .columns
                .get(name)
                .map(|col| FieldMetadata::Sql(col.clone())),
            EntityMetadata::Csv(csv) => csv
                .columns
                .iter()
                .find(|col| col.name.eq_ignore_ascii_case(name))
                .map(|col| FieldMetadata::Csv(col.clone())),
        }
    }

    pub fn is_valid(&self) -> bool {
        match self {
            EntityMetadata::Table(table) => table.is_valid(),
            EntityMetadata::Csv(csv) => !csv.columns.is_empty(),
        }
    }
}
