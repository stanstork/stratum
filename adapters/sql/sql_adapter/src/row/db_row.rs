use core::fmt;
use sqlx::{Column, Row, TypeInfo};
use std::fmt::Formatter;

pub enum DbRow<'a> {
    MySqlRow(&'a sqlx::mysql::MySqlRow),
    PostgresRow(&'a sqlx::postgres::PgRow),
}

impl DbRow<'_> {
    pub fn columns(&self) -> Vec<&str> {
        match self {
            DbRow::MySqlRow(row) => row.columns().iter().map(|col| col.name()).collect(),
            DbRow::PostgresRow(row) => row.columns().iter().map(|col| col.name()).collect(),
        }
    }

    pub fn column_type(&self, name: &str) -> &str {
        match self {
            DbRow::MySqlRow(row) => row.column(name).type_info().name(),
            DbRow::PostgresRow(row) => row.column(name).type_info().name(),
        }
    }

    pub fn try_get_i32(&self, name: &str) -> Option<i32> {
        match self {
            DbRow::MySqlRow(row) => row.try_get::<i32, _>(name).ok(),
            DbRow::PostgresRow(row) => row.try_get::<i32, _>(name).ok(),
        }
    }

    pub fn try_get_u32(&self, name: &str) -> Option<u32> {
        match self {
            DbRow::MySqlRow(row) => row.try_get::<u32, _>(name).ok(),
            DbRow::PostgresRow(row) => row.try_get::<i32, _>(name).map(|v| v as u32).ok(),
        }
    }

    pub fn try_get_u64(&self, name: &str) -> Option<u64> {
        match self {
            DbRow::MySqlRow(row) => row.try_get::<u64, _>(name).ok(),
            DbRow::PostgresRow(row) => row.try_get::<i64, _>(name).map(|v| v as u64).ok(),
        }
    }

    pub fn try_get_i64(&self, name: &str) -> Option<i64> {
        match self {
            DbRow::MySqlRow(row) => row.try_get::<i64, _>(name).ok(),
            DbRow::PostgresRow(row) => row.try_get::<i64, _>(name).ok(),
        }
    }

    pub fn try_get_f64(&self, name: &str) -> Option<f64> {
        match self {
            DbRow::MySqlRow(row) => row.try_get::<f64, _>(name).ok(),
            DbRow::PostgresRow(row) => row.try_get::<f64, _>(name).ok(),
        }
    }

    pub fn try_get_string(&self, name: &str) -> Option<String> {
        match self {
            DbRow::MySqlRow(row) => row.try_get::<String, _>(name).ok(),
            DbRow::PostgresRow(row) => row.try_get::<String, _>(name).ok(),
        }
    }

    pub fn try_get_bool(&self, name: &str) -> Option<bool> {
        match self {
            DbRow::MySqlRow(row) => row.try_get::<bool, _>(name).ok(),
            DbRow::PostgresRow(row) => row.try_get::<bool, _>(name).ok(),
        }
    }

    pub fn try_get_json(&self, name: &str) -> Option<serde_json::Value> {
        match self {
            DbRow::MySqlRow(row) => row.try_get::<serde_json::Value, _>(name).ok(),
            DbRow::PostgresRow(row) => row.try_get::<serde_json::Value, _>(name).ok(),
        }
    }

    pub fn try_get_bigdecimal(&self, name: &str) -> Option<bigdecimal::BigDecimal> {
        match self {
            DbRow::MySqlRow(row) => row.try_get::<bigdecimal::BigDecimal, _>(name).ok(),
            DbRow::PostgresRow(row) => row.try_get::<bigdecimal::BigDecimal, _>(name).ok(),
        }
    }

    pub fn try_get_timestamp(&self, name: &str) -> Option<chrono::DateTime<chrono::Utc>> {
        match self {
            DbRow::MySqlRow(row) => row.try_get::<chrono::DateTime<chrono::Utc>, _>(name).ok(),
            DbRow::PostgresRow(row) => row.try_get::<chrono::DateTime<chrono::Utc>, _>(name).ok(),
        }
    }

    pub fn try_get_date(&self, name: &str) -> Option<chrono::NaiveDate> {
        match self {
            DbRow::MySqlRow(row) => row.try_get::<chrono::NaiveDate, _>(name).ok(),
            DbRow::PostgresRow(row) => row.try_get::<chrono::NaiveDate, _>(name).ok(),
        }
    }

    pub fn try_get_bytes(&self, name: &str) -> Option<Vec<u8>> {
        match self {
            DbRow::MySqlRow(row) => row.try_get::<Vec<u8>, _>(name).ok(),
            DbRow::PostgresRow(row) => row.try_get::<Vec<u8>, _>(name).ok(),
        }
    }
}

impl fmt::Debug for DbRow<'_> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DbRow::MySqlRow(row) => write!(f, "{:?}", row),
            DbRow::PostgresRow(row) => write!(f, "{:?}", row),
        }
    }
}
