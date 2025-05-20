use super::field::FieldMetadata;
use csv::metadata::CsvMetadata;
use sql_adapter::metadata::table::TableMetadata;

#[derive(Debug, Clone)]
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
                .iter()
                .map(|(_, col)| FieldMetadata::Sql(col.clone()))
                .collect(),
            EntityMetadata::Csv(csv) => csv
                .columns
                .iter()
                .map(|col| FieldMetadata::Csv(col.clone()))
                .collect(),
        }
    }

    pub fn is_valid(&self) -> bool {
        match self {
            EntityMetadata::Table(table) => table.is_valid(),
            EntityMetadata::Csv(csv) => !csv.columns.is_empty(),
        }
    }
}
