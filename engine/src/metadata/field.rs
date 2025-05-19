use common::types::DataType;
use csv::metadata::CsvColumnMetadata;
use postgres::data_type::PgDataType;
use sql_adapter::metadata::column::ColumnMetadata;

#[derive(Debug, Clone)]
pub enum FieldMetadata {
    Sql(ColumnMetadata),
    Csv(CsvColumnMetadata),
}

impl FieldMetadata {
    pub fn pg_type(&self) -> (String, Option<usize>) {
        match self {
            FieldMetadata::Sql(col) => DataType::to_pg_type(col),
            FieldMetadata::Csv(col) => (col.data_type.to_string(), None),
        }
    }
}
