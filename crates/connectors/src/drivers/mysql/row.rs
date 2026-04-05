use crate::traits::row_decoder::RowDecoder;
use bigdecimal::BigDecimal;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use model::{
    core::{
        types::Type,
        value::{FieldValue, Value},
    },
    records::{OpType, Record},
};
use mysql_async::{Row as MySqlRow, Value as MySqlValue, consts::ColumnType};
use mysql_async::{consts::ColumnFlags, prelude::FromValue};
use std::str::FromStr;

impl RowDecoder for MySqlRow {
    fn decode(&self, table: &str) -> Record {
        let fields = self
            .columns_ref()
            .iter()
            .enumerate()
            .map(|(idx, col)| {
                let name = col.name_str().into_owned();
                let col_type = col.column_type();
                let flags = col.flags();
                let is_unsigned = flags.contains(ColumnFlags::UNSIGNED_FLAG);
                let data_type = mysql_col_type_to_canonical(col_type, is_unsigned);
                let value = extract_value(self, idx, col_type, is_unsigned, flags);

                FieldValue {
                    name,
                    value,
                    data_type,
                }
            })
            .collect();

        Record {
            schema: table.to_string(),
            fields,
            op_type: OpType::default(),
        }
    }

    fn columns(&self) -> Vec<String> {
        self.columns_ref()
            .iter()
            .map(|col| col.name_str().into_owned())
            .collect()
    }

    fn get_string(&self, column: &str) -> Option<String> {
        self.get_opt::<String, _>(column)?.ok()
    }

    fn get_i32(&self, column: &str) -> Option<i32> {
        self.get_opt::<i32, _>(column)?.ok()
    }

    fn get_u32(&self, column: &str) -> Option<u32> {
        self.get_opt::<u32, _>(column)?.ok()
    }

    fn get_i64(&self, column: &str) -> Option<i64> {
        self.get_opt::<i64, _>(column)?.ok()
    }

    fn get_bool(&self, column: &str) -> Option<bool> {
        // MySQL stores booleans as TINYINT(1)
        let val: Option<i8> = self.get_opt::<i8, _>(column)?.ok();
        val.map(|v| v != 0)
    }

    fn get_value(&self, column: &str) -> Option<Value> {
        let idx = self
            .columns_ref()
            .iter()
            .position(|c| c.name_str() == column)?;
        let col = &self.columns_ref()[idx];
        let col_type = col.column_type();
        let flags = col.flags();
        let is_unsigned = flags.contains(ColumnFlags::UNSIGNED_FLAG);
        extract_value(self, idx, col_type, is_unsigned, flags)
    }
}

/// Convert MySQL column type to canonical Type
fn mysql_col_type_to_canonical(col_type: ColumnType, is_unsigned: bool) -> Type {
    use model::core::types::{FloatSize, IntSize};

    match col_type {
        // Integer types
        ColumnType::MYSQL_TYPE_TINY => Type::Int {
            bits: IntSize::I8,
            unsigned: is_unsigned,
            auto_increment: false,
        },
        ColumnType::MYSQL_TYPE_SHORT => Type::Int {
            bits: IntSize::I16,
            unsigned: is_unsigned,
            auto_increment: false,
        },
        ColumnType::MYSQL_TYPE_INT24 => Type::Int {
            bits: IntSize::I24,
            unsigned: is_unsigned,
            auto_increment: false,
        },
        ColumnType::MYSQL_TYPE_LONG => Type::Int {
            bits: IntSize::I32,
            unsigned: is_unsigned,
            auto_increment: false,
        },
        ColumnType::MYSQL_TYPE_LONGLONG => Type::Int {
            bits: IntSize::I64,
            unsigned: is_unsigned,
            auto_increment: false,
        },

        // Floating point
        ColumnType::MYSQL_TYPE_FLOAT => Type::Float {
            bits: FloatSize::F32,
        },
        ColumnType::MYSQL_TYPE_DOUBLE => Type::Float {
            bits: FloatSize::F64,
        },

        // Decimal
        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => Type::Decimal {
            precision: None,
            scale: None,
        },

        // String types
        ColumnType::MYSQL_TYPE_VARCHAR | ColumnType::MYSQL_TYPE_VAR_STRING => Type::Varchar {
            length: None,
            charset: None,
        },
        ColumnType::MYSQL_TYPE_STRING => Type::Char {
            length: None,
            charset: None,
        },
        ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB
        | ColumnType::MYSQL_TYPE_BLOB => Type::Blob { max_bytes: None },

        // Temporal
        ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => Type::Date,
        ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => Type::Time {
            precision: None,
            with_tz: false,
        },
        ColumnType::MYSQL_TYPE_DATETIME | ColumnType::MYSQL_TYPE_DATETIME2 => Type::Timestamp {
            precision: None,
            with_tz: false,
        },
        ColumnType::MYSQL_TYPE_TIMESTAMP | ColumnType::MYSQL_TYPE_TIMESTAMP2 => Type::Timestamp {
            precision: None,
            with_tz: true,
        },
        ColumnType::MYSQL_TYPE_YEAR => Type::Year,

        // JSON
        ColumnType::MYSQL_TYPE_JSON => Type::Json { binary: true },

        // Bit
        ColumnType::MYSQL_TYPE_BIT => Type::Bit { length: None },

        // Enum and Set
        ColumnType::MYSQL_TYPE_ENUM => Type::Enum {
            name: String::new(),
            values: vec![],
        },
        ColumnType::MYSQL_TYPE_SET => Type::Set { values: vec![] },

        // Geometry
        ColumnType::MYSQL_TYPE_GEOMETRY => Type::Geometry {
            kind: None,
            srid: None,
        },

        // Null
        ColumnType::MYSQL_TYPE_NULL => Type::Unknown {
            source_name: "NULL".to_string(),
            fallback_ddl: "VARCHAR(255)".to_string(),
        },

        // Unknown
        _ => Type::Unknown {
            source_name: format!("{:?}", col_type),
            fallback_ddl: "VARCHAR(255)".to_string(),
        },
    }
}

/// Extract a value from a MySQL row at the given index
fn extract_value(
    row: &MySqlRow,
    idx: usize,
    col_type: ColumnType,
    is_unsigned: bool,
    col_flags: ColumnFlags,
) -> Option<Value> {
    // Get raw MySQL value
    let mysql_value: MySqlValue = row.get(idx)?;

    if mysql_value == MySqlValue::NULL {
        return Some(Value::Null);
    }

    match col_type {
        // Integer types
        ColumnType::MYSQL_TYPE_TINY => {
            if is_unsigned {
                let v: u8 = FromValue::from_value(mysql_value);
                Some(Value::UInt(v as u64))
            } else {
                let v: i8 = FromValue::from_value(mysql_value);
                Some(Value::Int(v as i64))
            }
        }
        ColumnType::MYSQL_TYPE_SHORT => {
            if is_unsigned {
                let v: u16 = FromValue::from_value(mysql_value);
                Some(Value::UInt(v as u64))
            } else {
                let v: i16 = FromValue::from_value(mysql_value);
                Some(Value::Int(v as i64))
            }
        }
        ColumnType::MYSQL_TYPE_INT24 | ColumnType::MYSQL_TYPE_LONG => {
            if is_unsigned {
                let v: u32 = FromValue::from_value(mysql_value);
                Some(Value::UInt(v as u64))
            } else {
                let v: i32 = FromValue::from_value(mysql_value);
                Some(Value::Int(v as i64))
            }
        }
        ColumnType::MYSQL_TYPE_LONGLONG => {
            if is_unsigned {
                let v: u64 = FromValue::from_value(mysql_value);
                Some(Value::UInt(v))
            } else {
                let v: i64 = FromValue::from_value(mysql_value);
                Some(Value::Int(v))
            }
        }

        // Floating point
        ColumnType::MYSQL_TYPE_FLOAT => {
            let v: f32 = FromValue::from_value(mysql_value);
            Some(Value::Float(v as f64))
        }
        ColumnType::MYSQL_TYPE_DOUBLE => {
            let v: f64 = FromValue::from_value(mysql_value);
            Some(Value::Float(v))
        }

        // Decimal
        ColumnType::MYSQL_TYPE_DECIMAL | ColumnType::MYSQL_TYPE_NEWDECIMAL => {
            let v: String = FromValue::from_value(mysql_value);
            match BigDecimal::from_str(&v) {
                Ok(d) => Some(Value::Decimal(d)),
                Err(_) => Some(Value::String(v)),
            }
        }

        // String types — BINARY_FLAG distinguishes BINARY/VARBINARY from CHAR/VARCHAR
        ColumnType::MYSQL_TYPE_VARCHAR
        | ColumnType::MYSQL_TYPE_VAR_STRING
        | ColumnType::MYSQL_TYPE_STRING => {
            if col_flags.contains(ColumnFlags::BINARY_FLAG) {
                let v: Vec<u8> = FromValue::from_value(mysql_value);
                Some(Value::Binary(v))
            } else {
                let v: String = FromValue::from_value(mysql_value);
                Some(Value::String(v))
            }
        }

        // Binary/Blob types
        ColumnType::MYSQL_TYPE_TINY_BLOB
        | ColumnType::MYSQL_TYPE_MEDIUM_BLOB
        | ColumnType::MYSQL_TYPE_LONG_BLOB
        | ColumnType::MYSQL_TYPE_BLOB => {
            let v: Vec<u8> = FromValue::from_value(mysql_value);
            Some(Value::Binary(v))
        }

        // Date
        ColumnType::MYSQL_TYPE_DATE | ColumnType::MYSQL_TYPE_NEWDATE => {
            let v: NaiveDate = FromValue::from_value(mysql_value);
            Some(Value::Date(v))
        }

        // Time
        ColumnType::MYSQL_TYPE_TIME | ColumnType::MYSQL_TYPE_TIME2 => {
            let v: NaiveTime = FromValue::from_value(mysql_value);
            Some(Value::Time {
                value: v,
                offset_secs: None,
            })
        }

        // Datetime (no timezone)
        ColumnType::MYSQL_TYPE_DATETIME | ColumnType::MYSQL_TYPE_DATETIME2 => {
            let v: NaiveDateTime = FromValue::from_value(mysql_value);
            Some(Value::Timestamp {
                value: v,
                offset_secs: None,
            })
        }

        // Timestamp (stored in UTC)
        ColumnType::MYSQL_TYPE_TIMESTAMP | ColumnType::MYSQL_TYPE_TIMESTAMP2 => {
            let v: NaiveDateTime = FromValue::from_value(mysql_value);
            Some(Value::Timestamp {
                value: v,
                offset_secs: Some(0), // UTC
            })
        }

        // Year
        ColumnType::MYSQL_TYPE_YEAR => {
            let v: i32 = FromValue::from_value(mysql_value);
            Some(Value::Year(v as i16))
        }

        // JSON
        ColumnType::MYSQL_TYPE_JSON => {
            let v: String = FromValue::from_value(mysql_value);
            match serde_json::from_str(&v) {
                Ok(json) => Some(Value::Json(json)),
                Err(_) => Some(Value::String(v)),
            }
        }

        // Bit
        ColumnType::MYSQL_TYPE_BIT => {
            let v: Vec<u8> = FromValue::from_value(mysql_value);
            let bits: Vec<bool> = v
                .iter()
                .flat_map(|byte| (0..8).rev().map(move |i| (byte >> i) & 1 == 1))
                .collect();
            Some(Value::Bits(bits))
        }

        // Enum - MySQL returns as string
        ColumnType::MYSQL_TYPE_ENUM => {
            let v: String = FromValue::from_value(mysql_value);
            Some(Value::Enum {
                type_name: String::new(), // Unknown at runtime
                value: v,
            })
        }

        // Set - MySQL returns as comma-separated string
        ColumnType::MYSQL_TYPE_SET => {
            let v: String = FromValue::from_value(mysql_value);
            let values: Vec<String> = v.split(',').map(|s| s.to_string()).collect();
            Some(Value::Set(values))
        }

        // Geometry - return as binary (WKB)
        ColumnType::MYSQL_TYPE_GEOMETRY => {
            let v: Vec<u8> = FromValue::from_value(mysql_value);
            Some(Value::Geometry(v))
        }

        // Null type
        ColumnType::MYSQL_TYPE_NULL => Some(Value::Null),

        // Fallback - try to get as string
        _ => match mysql_value {
            MySqlValue::Bytes(b) => match String::from_utf8(b.clone()) {
                Ok(s) => Some(Value::String(s)),
                Err(_) => Some(Value::Binary(b)),
            },
            MySqlValue::Int(i) => Some(Value::Int(i)),
            MySqlValue::UInt(u) => Some(Value::UInt(u)),
            MySqlValue::Float(f) => Some(Value::Float(f as f64)),
            MySqlValue::Double(d) => Some(Value::Float(d)),
            MySqlValue::NULL => Some(Value::Null),
            _ => None,
        },
    }
}
