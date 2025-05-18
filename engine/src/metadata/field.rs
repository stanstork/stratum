use csv::metadata::CsvColumnMetadata;
use sql_adapter::metadata::column::ColumnMetadata;

#[derive(Debug, Clone)]
pub enum FieldMetadata {
    Sql(ColumnMetadata),
    Csv(CsvColumnMetadata),
}
