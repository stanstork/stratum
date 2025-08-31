//! Defines the `Dialect` trait for database-specific SQL syntax.

use common::types::DataType;

pub trait Dialect: Send + Sync {
    /// Wraps an identifier (like a table or column name) in the correct
    /// quotation marks for the dialect.
    ///
    /// - PostgreSQL uses double quotes: `"my_column"`
    /// - MySQL uses backticks: `` `my_column` ``
    fn quote_identifier(&self, ident: &str) -> String;

    /// Returns the placeholder for a parameterized query.
    ///
    /// - PostgreSQL uses `$1`, `$2`, etc.
    /// - MySQL uses `?`
    fn get_placeholder(&self, index: usize) -> String;

    /// Renders a generic `DataType` enum into a database-specific SQL type string.
    fn render_data_type(&self, data_type: &DataType) -> String;
}

#[derive(Debug, Clone)]
pub struct Postgres;

impl Dialect for Postgres {
    fn quote_identifier(&self, ident: &str) -> String {
        format!(r#""{}""#, ident)
    }

    fn get_placeholder(&self, index: usize) -> String {
        // PostgreSQL uses $1, $2, etc.
        format!("${}", index + 1)
    }

    fn render_data_type(&self, data_type: &DataType) -> String {
        match data_type {
            DataType::Decimal => "DECIMAL".into(),
            DataType::Short => "SMALLINT".into(),
            DataType::Long => "INTEGER".into(),
            DataType::Float => "REAL".into(),
            DataType::Double => "DOUBLE PRECISION".into(),
            DataType::Null => "NULL".into(),
            DataType::Timestamp => "TIMESTAMP".into(),
            DataType::LongLong => "BIGINT".into(),
            DataType::Int => "INTEGER".into(),
            DataType::Time => "TIME".into(),
            DataType::Year => "INTEGER".into(),
            DataType::VarChar => "VARCHAR".into(),
            DataType::Bit => "BIT".into(),
            DataType::Json => "JSON".into(),
            DataType::NewDecimal => "DECIMAL".into(),
            DataType::Enum => "ENUM".into(),
            DataType::Set => "TEXT[]".into(),
            DataType::TinyBlob => "BYTEA".into(),
            DataType::MediumBlob => "BYTEA".into(),
            DataType::LongBlob => "BYTEA".into(),
            DataType::Blob => "BYTEA".into(),
            DataType::VarString => "VARCHAR".into(),
            DataType::String => "TEXT".into(),
            DataType::Geometry => "BYTEA".into(),
            DataType::Boolean => "BOOLEAN".into(),
            DataType::ShortUnsigned => "SMALLINT".into(),
            DataType::IntUnsigned => "INTEGER".into(),
            DataType::Bytea => "BYTEA".into(),
            DataType::Array => "ARRAY".into(),
            DataType::Char => "CHAR".into(),
            DataType::Date => "DATE".into(),
            DataType::Custom(name) => name.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MySql;

impl Dialect for MySql {
    fn quote_identifier(&self, ident: &str) -> String {
        format!(r#"`{}`"#, ident)
    }

    fn get_placeholder(&self, _index: usize) -> String {
        // MySQL uses ?
        "?".into()
    }

    fn render_data_type(&self, data_type: &DataType) -> String {
        match data_type {
            DataType::Decimal => "DECIMAL".into(),
            DataType::Short => "SMALLINT".into(),
            DataType::Long => "INT".into(),
            DataType::Float => "FLOAT".into(),
            DataType::Double => "DOUBLE".into(),
            DataType::Null => "NULL".into(),
            DataType::Timestamp => "TIMESTAMP".into(),
            DataType::LongLong => "BIGINT".into(),
            DataType::Int => "INT".into(),
            DataType::Time => "TIME".into(),
            DataType::Year => "YEAR".into(),
            DataType::VarChar => "VARCHAR".into(),
            DataType::Bit => "BIT".into(),
            DataType::Json => "JSON".into(),
            DataType::NewDecimal => "NEWDECIMAL".into(),
            DataType::Enum => "ENUM".into(),
            DataType::Set => "SET".into(),
            DataType::TinyBlob => "TINYBLOB".into(),
            DataType::MediumBlob => "MEDIUMBLOB".into(),
            DataType::LongBlob => "LONGBLOB".into(),
            DataType::Blob => "BLOB".into(),
            DataType::VarString => "VARSTRING".into(),
            DataType::String => "VARCHAR".into(),
            DataType::Geometry => "GEOMETRY".into(),
            DataType::Boolean => "BOOLEAN".into(),
            DataType::ShortUnsigned => "SMALLINT UNSIGNED".into(),
            DataType::IntUnsigned => "INT UNSIGNED".into(),
            DataType::Bytea => "BYTEA".into(),
            DataType::Array => "ARRAY".into(),
            DataType::Char => "CHAR".into(),
            DataType::Date => "DATE".into(),
            DataType::Custom(name) => name.clone(),
        }
    }
}
