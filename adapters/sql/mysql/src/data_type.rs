use data_model::core::types::DataType;
use sqlx::mysql::MySqlRow;
use sqlx::Row;

pub trait MySqlColumnDataType {
    fn from_mysql_row(row: &MySqlRow) -> DataType;
}

impl MySqlColumnDataType for DataType {
    fn from_mysql_row(row: &MySqlRow) -> DataType {
        let data_type_str: String = row.try_get("data_type").unwrap_or_default();
        DataType::try_from(data_type_str.as_str()).unwrap_or(DataType::String)
    }
}
