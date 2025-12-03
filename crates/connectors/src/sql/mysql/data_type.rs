use model::core::data_type::DataType;
use mysql_async::Row as MySqlRow;

pub trait MySqlColumnDataType {
    fn from_mysql_row(row: &MySqlRow) -> DataType;
}

impl MySqlColumnDataType for DataType {
    fn from_mysql_row(row: &MySqlRow) -> DataType {
        let data_type_str = row
            .get_opt::<String, _>("data_type")
            .and_then(|res| res.ok())
            .unwrap_or_default();
        DataType::from_mysql_type(data_type_str.as_str()).unwrap_or(DataType::Custom(data_type_str))
    }
}
