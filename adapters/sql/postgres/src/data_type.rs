use sql_adapter::metadata::column::data_type::ColumnDataType;
use sqlx::postgres::PgRow;
use sqlx::Row;

pub trait ColumnDataTypeMapper {
    fn from_pg_row(row: &PgRow) -> ColumnDataType;
    fn to_pg_string(&self) -> String;
}

impl ColumnDataTypeMapper for ColumnDataType {
    fn from_pg_row(row: &PgRow) -> ColumnDataType {
        let data_type_str: String = row.try_get("data_type").unwrap_or_default();
        ColumnDataType::try_from(data_type_str.as_str()).unwrap_or(ColumnDataType::String)
    }

    fn to_pg_string(&self) -> String {
        match self {
            ColumnDataType::Decimal => "DECIMAL".to_string(),
            ColumnDataType::Short => "SMALLINT".to_string(),
            ColumnDataType::Long => "INTEGER".to_string(),
            ColumnDataType::Float => "REAL".to_string(),
            ColumnDataType::Double => "DOUBLE PRECISION".to_string(),
            ColumnDataType::Null => "NULL".to_string(),
            ColumnDataType::Timestamp => "TIMESTAMP".to_string(),
            ColumnDataType::LongLong => "BIGINT".to_string(),
            ColumnDataType::Int => "INTEGER".to_string(),
            ColumnDataType::Time => "TIME".to_string(),
            ColumnDataType::Year => "INTEGER".to_string(),
            ColumnDataType::VarChar => "VARCHAR".to_string(),
            ColumnDataType::Bit => "BIT".to_string(),
            ColumnDataType::Json => "JSON".to_string(),
            ColumnDataType::NewDecimal => "DECIMAL".to_string(),
            ColumnDataType::Enum => "ENUM".to_string(),
            ColumnDataType::Set => "SET".to_string(),
            ColumnDataType::TinyBlob => "BYTEA".to_string(),
            ColumnDataType::MediumBlob => "BYTEA".to_string(),
            ColumnDataType::LongBlob => "BYTEA".to_string(),
            ColumnDataType::Blob => "BYTEA".to_string(),
            ColumnDataType::VarString => "VARCHAR".to_string(),
            ColumnDataType::String => "TEXT".to_string(),
            ColumnDataType::Geometry => "BYTEA".to_string(),
            ColumnDataType::Boolean => "BOOLEAN".to_string(),
            ColumnDataType::ShortUnsigned => "SMALLINT".to_string(),
            ColumnDataType::IntUnsigned => "INTEGER".to_string(),
        }
    }
}
