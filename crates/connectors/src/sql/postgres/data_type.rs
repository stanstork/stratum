use crate::sql::base::metadata::column::ColumnMetadata;
use model::core::data_type::DataType;
use tokio_postgres::Row as PgRow;

pub trait PgDataType {
    fn parse_from_row(row: &PgRow) -> DataType;
    fn as_pg_type_info(col: &ColumnMetadata) -> (DataType, Option<usize>);
}

impl PgDataType for DataType {
    fn parse_from_row(row: &PgRow) -> DataType {
        let data_type_str: String = row.try_get("data_type").ok().unwrap_or_default();
        DataType::from_postgres_type(data_type_str.as_str())
            .unwrap_or_else(|_| DataType::Custom(data_type_str))
    }

    fn as_pg_type_info(col: &ColumnMetadata) -> (DataType, Option<usize>) {
        match &col.data_type {
            // PostgreSQL ENUMs are custom types, often named after the table/column.
            DataType::Enum => (DataType::Custom(col.name.clone()), None),

            // A common way to represent a SET is as a TEXT array in PostgreSQL.
            DataType::Set => (DataType::Array(None), None),

            // These types don't have a character length limit in this context.
            DataType::String
            | DataType::TinyBlob
            | DataType::MediumBlob
            | DataType::LongBlob
            | DataType::Blob
            | DataType::Binary
            | DataType::VarBinary
            | DataType::Bytea
            | DataType::Geometry
            | DataType::Array(_) => (col.data_type.clone(), None),

            // For all other types, we use their defined data type and max length.
            _ => (col.data_type.clone(), col.char_max_length),
        }
    }
}
