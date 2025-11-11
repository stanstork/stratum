use bigdecimal::{BigDecimal, FromPrimitive, ToPrimitive};
use core::fmt;
use model::{
    core::{
        data_type::DataType,
        value::{FieldValue, Value},
    },
    records::row::RowData,
};
use mysql_async::Row as MySqlRow;
use std::fmt::Formatter;
use tokio_postgres::{Column as PgColumn, Row as PgRow, types::Json as PgJson};
use tracing::warn;

use crate::sql::base::utils::mysql_col_type;

pub enum DbRow<'a> {
    MySqlRow(&'a MySqlRow),
    PostgresRow(&'a PgRow),
}

impl DbRow<'_> {
    pub fn to_row_data(&self, table: &str) -> RowData {
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
            DataType::Int4 => self.try_get_i32(name).map(|v| Value::Int(v as i64)),
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

    pub fn columns(&self) -> Vec<String> {
        match self {
            DbRow::MySqlRow(row) => row
                .columns_ref()
                .iter()
                .map(|col| col.name_str().into_owned())
                .collect(),
            DbRow::PostgresRow(row) => row
                .columns()
                .iter()
                .map(|col| col.name().to_string())
                .collect(),
        }
    }

    pub fn column_type(&self, name: &str) -> &str {
        match self {
            DbRow::MySqlRow(row) => row
                .columns_ref()
                .iter()
                .find(|col| col.name_ref() == name.as_bytes())
                .map(|col| mysql_col_type(col.column_type()))
                .unwrap_or("VARCHAR"),
            DbRow::PostgresRow(row) => {
                let col: &PgColumn = row
                    .columns()
                    .iter()
                    .find(|c| c.name() == name)
                    .expect("Column not found");
                col.type_().name()
            }
        }
    }

    pub fn try_get_i32(&self, name: &str) -> Option<i32> {
        match self {
            DbRow::MySqlRow(row) => row.get_opt::<i32, _>(name).and_then(|res| res.ok()),
            DbRow::PostgresRow(row) => row.try_get::<_, i32>(name).ok(),
        }
    }

    pub fn try_get_u32(&self, name: &str) -> Option<u32> {
        match self {
            DbRow::MySqlRow(row) => row.get_opt::<u32, _>(name).and_then(|res| res.ok()),
            DbRow::PostgresRow(row) => row.try_get::<_, u32>(name).map(|v| v as u32).ok(),
        }
    }

    pub fn try_get_u64(&self, name: &str) -> Option<u64> {
        match self {
            DbRow::MySqlRow(row) => row.get_opt::<u64, _>(name).and_then(|res| res.ok()),
            DbRow::PostgresRow(row) => row.try_get::<_, i64>(name).map(|v| v as u64).ok(),
        }
    }

    pub fn try_get_i4(&self, name: &str) -> Option<i32> {
        match self {
            DbRow::MySqlRow(row) => row.get_opt::<i32, _>(name).and_then(|res| res.ok()),
            DbRow::PostgresRow(row) => row.try_get::<_, i32>(name).ok(),
        }
    }

    pub fn try_get_i64(&self, name: &str) -> Option<i64> {
        match self {
            DbRow::MySqlRow(row) => row.get_opt::<i64, _>(name).and_then(|res| res.ok()),
            DbRow::PostgresRow(row) => row.try_get::<_, i64>(name).ok(),
        }
    }

    pub fn try_get_f64(&self, name: &str) -> Option<f64> {
        match self {
            DbRow::MySqlRow(row) => row.get_opt::<f64, _>(name).and_then(|res| res.ok()),
            DbRow::PostgresRow(row) => row.try_get::<_, f64>(name).ok(),
        }
    }

    pub fn try_get_string(&self, name: &str) -> Option<String> {
        match self {
            DbRow::MySqlRow(row) => row.get_opt::<String, _>(name).and_then(|res| res.ok()),
            DbRow::PostgresRow(row) => row.try_get::<_, String>(name).ok(),
        }
    }

    pub fn try_get_bool(&self, name: &str) -> Option<bool> {
        match self {
            DbRow::MySqlRow(row) => row.get_opt::<bool, _>(name).and_then(|res| res.ok()),
            DbRow::PostgresRow(row) => row.try_get::<_, bool>(name).ok(),
        }
    }

    pub fn try_get_json(&self, name: &str) -> Option<serde_json::Value> {
        match self {
            DbRow::MySqlRow(row) => row
                .get_opt::<serde_json::Value, _>(name)
                .and_then(|res| res.ok()),
            DbRow::PostgresRow(row) => row
                .try_get::<_, PgJson<serde_json::Value>>(name)
                .ok()
                .map(|json| json.0),
        }
    }

    pub fn try_get_bigdecimal(&self, name: &str) -> Option<bigdecimal::BigDecimal> {
        match self {
            DbRow::MySqlRow(row) => row
                .get_opt::<bigdecimal::BigDecimal, _>(name)
                .and_then(|res| res.ok()),
            DbRow::PostgresRow(row) => row
                .try_get::<_, f64>(name)
                .ok()
                .and_then(BigDecimal::from_f64),
        }
    }

    pub fn try_get_timestamp(&self, name: &str) -> Option<chrono::DateTime<chrono::Utc>> {
        match self {
            DbRow::MySqlRow(row) => row
                .get_opt::<chrono::NaiveDateTime, _>(name)
                .and_then(|res| res.ok())
                .map(|naive: chrono::NaiveDateTime| {
                    chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(naive, chrono::Utc)
                }),
            DbRow::PostgresRow(row) => row.try_get::<_, chrono::DateTime<chrono::Utc>>(name).ok(),
        }
    }

    pub fn try_get_date(&self, name: &str) -> Option<chrono::NaiveDate> {
        match self {
            DbRow::MySqlRow(row) => row
                .get_opt::<chrono::NaiveDate, _>(name)
                .and_then(|res| res.ok()),
            DbRow::PostgresRow(row) => row.try_get::<_, chrono::NaiveDate>(name).ok(),
        }
    }

    pub fn try_get_bytes(&self, name: &str) -> Option<Vec<u8>> {
        match self {
            DbRow::MySqlRow(row) => row.get_opt::<Vec<u8>, _>(name).and_then(|res| res.ok()),
            DbRow::PostgresRow(row) => row.try_get::<_, Vec<u8>>(name).ok(),
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
