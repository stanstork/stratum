use common::types::DataType;
use sql_adapter::metadata::column::ColumnMetadata;
use sqlx::postgres::PgRow;
use sqlx::Row;

pub trait PgDataType {
    fn parse_from_row(row: &PgRow) -> DataType;
    fn as_pg_type_info(col: &ColumnMetadata) -> (DataType, Option<usize>);
}

impl PgDataType for DataType {
    fn parse_from_row(row: &PgRow) -> DataType {
        let data_type_str: String = row.try_get("data_type").ok().unwrap_or_default();
        DataType::try_from(data_type_str.as_str()).unwrap_or(DataType::Custom(data_type_str))
    }

    fn as_pg_type_info(col: &ColumnMetadata) -> (DataType, Option<usize>) {
        match &col.data_type {
            // PostgreSQL ENUMs are custom types, often named after the table/column.
            DataType::Enum => (DataType::Custom(col.name.clone()), None),

            // A common way to represent a SET is as a TEXT array in PostgreSQL.
            DataType::Set => (DataType::Custom("TEXT[]".to_string()), None),

            // These types don't have a character length limit in this context.
            DataType::String
            | DataType::TinyBlob
            | DataType::MediumBlob
            | DataType::LongBlob
            | DataType::Blob => (col.data_type.clone(), None),

            // For all other types, we use their defined data type and max length.
            _ => (col.data_type.clone(), col.char_max_length),
        }
    }
}
