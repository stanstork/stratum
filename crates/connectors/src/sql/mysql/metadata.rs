use crate::sql::{
    base::{metadata::column::ColumnMetadata, row::DbRow},
    mysql::data_type::MySqlColumnDataType,
};
use model::core::data_type::DataType;
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
