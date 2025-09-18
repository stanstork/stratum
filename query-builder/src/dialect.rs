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

    /// Renders a generic `DataType` into a database-specific SQL type string.
    fn render_data_type(&self, data_type: &DataType, max_length: Option<usize>) -> String;

    /// Returns the name of the dialect (e.g., "PostgreSQL", "MySQL").
    fn name(&self) -> String;

    /// Generates the SQL query and a corresponding list of parameters to bind
    /// for efficiently checking the existence of multiple composite keys.
    fn build_key_existence_query(
        &self,
        table_name: &str,
        key_columns: &[String],
        keys_batch: usize,
    ) -> String;
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

    fn render_data_type(&self, data_type: &DataType, max_length: Option<usize>) -> String {
        let type_name = match data_type {
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
        };

        if let Some(max_len) = max_length {
            format!("{}({})", type_name, max_len)
        } else {
            type_name
        }
    }

    fn name(&self) -> String {
        "PostgreSQL".into()
    }

    fn build_key_existence_query(
        &self,
        table_name: &str,
        key_columns: &[String],
        keys_batch: usize,
    ) -> String {
        if keys_batch == 0 || key_columns.is_empty() {
            return String::new();
        }

        let select_clause = key_columns
            .iter()
            .enumerate()
            .map(|(i, col_name)| format!("v.c{} AS {}", i + 1, self.quote_identifier(col_name)))
            .collect::<Vec<_>>()
            .join(", ");

        let value_columns: String = (1..=key_columns.len())
            .map(|i| format!("c{}", i))
            .collect::<Vec<_>>()
            .join(", ");

        let join_conditions = key_columns
            .iter()
            .enumerate()
            .map(|(i, col_name)| format!("t.{} = v.c{}", self.quote_identifier(col_name), i + 1))
            .collect::<Vec<_>>()
            .join(" AND ");

        let mut placeholder_idx = 1;
        let placeholders: String = (0..keys_batch)
            .map(|_| {
                let p = (0..key_columns.len())
                    .map(|_| {
                        let p_str = format!("${}", placeholder_idx);
                        placeholder_idx += 1;
                        p_str
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("({})", p)
            })
            .collect::<Vec<_>>()
            .join(", ");

        format!(
            "SELECT {} FROM (VALUES {}) AS v({}) INNER JOIN {} AS t ON {}",
            select_clause,
            placeholders,
            value_columns,
            self.quote_identifier(table_name),
            join_conditions
        )
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

    fn render_data_type(&self, data_type: &DataType, max_length: Option<usize>) -> String {
        let type_name = match data_type {
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
        };

        if let Some(max_len) = max_length {
            format!("{}({})", type_name, max_len)
        } else {
            type_name
        }
    }

    fn name(&self) -> String {
        "MySQL".into()
    }

    fn build_key_existence_query(
        &self,
        table_name: &str,
        key_columns: &[String],
        keys_batch: usize,
    ) -> String {
        todo!("Implement batch key existence query for MySQL")
    }
}
