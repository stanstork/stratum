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
}
