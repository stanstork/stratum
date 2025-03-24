use crate::data_type::ColumnDataTypeMapper;
use sql_adapter::metadata::column::data_type::ColumnDataType;
use sql_adapter::metadata::column::metadata::ColumnMetadata;
use sql_adapter::row::db_row::DbRow;
use sqlx::mysql::MySqlRow;

pub trait ColumnMetadataMapper {
    fn from_mysql_row(row: &MySqlRow) -> ColumnMetadata;
}

impl ColumnMetadataMapper for ColumnMetadata {
    fn from_mysql_row(row: &MySqlRow) -> ColumnMetadata {
        let data_type = ColumnDataType::from_mysql_row(row);
        ColumnMetadata::from_row(&DbRow::MySqlRow(row), data_type)
    }
}
