use sql_adapter::metadata::column::data_type::ColumnDataType;
use sqlx::mysql::MySqlRow;
use sqlx::Row;

pub trait ColumnDataTypeMapper {
    fn from_mysql_row(row: &MySqlRow) -> ColumnDataType;
}

impl ColumnDataTypeMapper for ColumnDataType {
    fn from_mysql_row(row: &MySqlRow) -> ColumnDataType {
        let data_type_str: String = row.try_get("data_type").unwrap_or_default();
        ColumnDataType::try_from(data_type_str.as_str()).unwrap_or(ColumnDataType::String)
    }
}
