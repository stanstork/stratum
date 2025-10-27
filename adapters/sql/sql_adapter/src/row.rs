use bigdecimal::ToPrimitive;
use core::fmt;
use data_model::{
    core::types::DataType,
    core::value::{FieldValue, Value},
    records::row_data::RowData,
};
use sqlx::{Column, Row, TypeInfo};
use std::fmt::Formatter;
use tracing::warn;

pub enum DbRow<'a> {
    MySqlRow(&'a sqlx::mysql::MySqlRow),
    PostgresRow(&'a sqlx::postgres::PgRow),
}

impl DbRow<'_> {
    pub fn get_row_data(&self, table: &str) -> RowData {
        let columns = self
            .columns()
            .iter()
            .map(|column| {
                let data_type = DataType::try_from(self.column_type(column)).unwrap_or_else(|_| {
                    warn!("Unknown column type: {}", self.column_type(column));
                    DataType::String
                });

                FieldValue {
                    name: column.to_string(),
                    value: self.get_value(&data_type, column),
                    data_type,
                }
            })
            .collect();

        RowData::new(table, columns)
    }

    pub fn get_value(&self, data_type: &DataType, name: &str) -> Option<Value> {
        match data_type {
            DataType::Int | DataType::Long | DataType::Short => {
                self.try_get_i64(name).map(Value::Int)
            }
            DataType::IntUnsigned | DataType::ShortUnsigned | DataType::Year => {
                self.try_get_u64(name).map(|v| Value::Int(v as i64))
            }
            DataType::Float => self.try_get_f64(name).map(Value::Float),
            DataType::Decimal => self
                .try_get_bigdecimal(name)
                .and_then(|v| v.to_f64().map(Value::Float)),
            DataType::String | DataType::VarChar | DataType::Char => {
                self.try_get_string(name).map(Value::String)
            }
            DataType::Boolean => self.try_get_bool(name).map(Value::Boolean),
            DataType::Json => self.try_get_json(name).map(Value::Json),
            DataType::Timestamp => self.try_get_timestamp(name).map(Value::Timestamp),
            DataType::Date => self.try_get_date(name).map(Value::Date),
            DataType::Enum => {
                let enum_value = self.try_get_string(name)?;
                Some(Value::Enum(name.to_string(), enum_value))
            }
            DataType::Bytea | DataType::Geometry => self.try_get_bytes(name).map(Value::Bytes),
            DataType::Blob | DataType::TinyBlob | DataType::MediumBlob | DataType::LongBlob => {
                self.try_get_bytes(name).map(Value::Bytes)
            }
            _ => None,
        }
    }

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
            DbRow::MySqlRow(row) => write!(f, "{row:?}"),
            DbRow::PostgresRow(row) => write!(f, "{row:?}"),
        }
    }
}
