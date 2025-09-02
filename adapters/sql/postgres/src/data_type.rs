use common::types::DataType;
use sql_adapter::metadata::column::ColumnMetadata;
use sqlx::postgres::PgRow;
use sqlx::Row;

pub trait PgDataType {
    fn from_pg_row(row: &PgRow) -> DataType;
    fn to_pg_string(&self) -> String;
    fn to_pg_type(col: &ColumnMetadata) -> (DataType, Option<usize>);
}

impl PgDataType for DataType {
    fn from_pg_row(row: &PgRow) -> DataType {
        let data_type_str: String = row.try_get("data_type").unwrap_or_default();
        DataType::try_from(data_type_str.as_str()).unwrap_or(DataType::Custom(data_type_str))
    }

    fn to_pg_string(&self) -> String {
        match self {
            DataType::Decimal => "DECIMAL".to_string(),
            DataType::Short => "SMALLINT".to_string(),
            DataType::Long => "INTEGER".to_string(),
            DataType::Float => "REAL".to_string(),
            DataType::Double => "DOUBLE PRECISION".to_string(),
            DataType::Null => "NULL".to_string(),
            DataType::Timestamp => "TIMESTAMP".to_string(),
            DataType::LongLong => "BIGINT".to_string(),
            DataType::Int => "INTEGER".to_string(),
            DataType::Time => "TIME".to_string(),
            DataType::Year => "INTEGER".to_string(),
            DataType::VarChar => "VARCHAR".to_string(),
            DataType::Bit => "BIT".to_string(),
            DataType::Json => "JSON".to_string(),
            DataType::NewDecimal => "DECIMAL".to_string(),
            DataType::Enum => "ENUM".to_string(),
            DataType::Set => "SET".to_string(),
            DataType::TinyBlob => "BYTEA".to_string(),
            DataType::MediumBlob => "BYTEA".to_string(),
            DataType::LongBlob => "BYTEA".to_string(),
            DataType::Blob => "BYTEA".to_string(),
            DataType::VarString => "VARCHAR".to_string(),
            DataType::String => "TEXT".to_string(),
            DataType::Geometry => "BYTEA".to_string(),
            DataType::Boolean => "BOOLEAN".to_string(),
            DataType::ShortUnsigned => "SMALLINT".to_string(),
            DataType::IntUnsigned => "INTEGER".to_string(),
            DataType::Bytea => "BYTEA".to_string(),
            DataType::Array => "ARRAY".to_string(),
            DataType::Char => "CHAR".to_string(),
            DataType::Date => "DATE".to_string(),
            DataType::Custom(name) => name.clone(),
        }
    }

    fn to_pg_type(col: &ColumnMetadata) -> (DataType, Option<usize>) {
        let data_type = match &col.data_type {
            DataType::Enum => DataType::Custom(col.name.clone()),
            DataType::Set => DataType::Custom("TEXT[]".to_string()),
            _ => col.data_type.clone(),
        };

        let type_len = if col.data_type == DataType::Enum {
            None
        } else {
            match data_type {
                DataType::Custom(ref s) if s == "TEXT[]" => None,
                DataType::TinyBlob | DataType::MediumBlob | DataType::LongBlob | DataType::Blob => {
                    None
                }
                DataType::String => None,
                _ => col.char_max_length,
            }
        };

        (data_type, type_len)
    }
}
