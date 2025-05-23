use crate::data_type::MySqlColumnDataType;
use common::types::DataType;
use sql_adapter::metadata::column::ColumnMetadata;
use sql_adapter::row::DbRow;
use sqlx::mysql::MySqlRow;

pub trait ColumnMetadataMapper {
    fn from_mysql_row(row: &MySqlRow) -> ColumnMetadata;
}

impl ColumnMetadataMapper for ColumnMetadata {
    fn from_mysql_row(row: &MySqlRow) -> ColumnMetadata {
        let data_type = DataType::from_mysql_row(row);
        ColumnMetadata::from_row(&DbRow::MySqlRow(row), data_type)
    }
}
