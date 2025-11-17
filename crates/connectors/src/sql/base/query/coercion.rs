use crate::sql::base::metadata::column::ColumnMetadata;
use chrono::{DateTime, NaiveDateTime, Utc};
use model::core::{data_type::DataType, value::Value};

pub(crate) fn coerce_value(value: Value, col_meta: &ColumnMetadata) -> Value {
    let value = coerce_numeric(value, &col_meta.data_type);
    let value = coerce_temporal(value, &col_meta.data_type);
    let value = coerce_enum(value, col_meta);
    coerce_text(value, &col_meta.data_type)
}

fn coerce_numeric(value: Value, data_type: &DataType) -> Value {
    match data_type {
        DataType::Short | DataType::ShortUnsigned => match value.as_i16() {
            Some(v) => Value::SmallInt(v),
            None => value,
        },
        DataType::Int | DataType::Int4 | DataType::IntUnsigned => match value.as_i32() {
            Some(v) => Value::Int32(v),
            None => value,
        },
        DataType::Year => match value.as_i32() {
            Some(v) => Value::Int32(v),
            None => value,
        },
        DataType::Long | DataType::LongLong => match value.as_i64() {
            Some(v) => Value::Int(v),
            None => value,
        },
        DataType::Decimal | DataType::NewDecimal => match value.as_big_decimal() {
            Some(v) => Value::Decimal(v),
            None => value,
        },
        _ => value,
    }
}

fn coerce_text(value: Value, data_type: &DataType) -> Value {
    match data_type {
        DataType::String | DataType::VarChar | DataType::Char => match value {
            Value::Bytes(bytes) => match String::from_utf8(bytes) {
                Ok(text) => Value::String(text),
                Err(err) => {
                    let bytes = err.into_bytes();
                    Value::String(String::from_utf8_lossy(&bytes).to_string())
                }
            },
            other => other,
        },
        _ => value,
    }
}

fn coerce_enum(value: Value, col_meta: &ColumnMetadata) -> Value {
    match &col_meta.data_type {
        DataType::Custom(type_name) => match value {
            Value::Enum(_, v) => Value::Enum(type_name.clone(), v),
            Value::String(s) => Value::Enum(type_name.clone(), s),
            other => other,
        },
        DataType::Enum => match value {
            Value::Enum(_, v) => Value::Enum(col_meta.name.clone(), v),
            Value::String(s) => Value::Enum(col_meta.name.clone(), s),
            other => other,
        },
        _ => value,
    }
}

fn coerce_temporal(value: Value, data_type: &DataType) -> Value {
    match data_type {
        DataType::Timestamp => match value {
            Value::Timestamp(ts) => Value::TimestampNaive(ts.naive_utc()),
            Value::TimestampNaive(_) => value,
            Value::String(ref s) => match parse_naive_datetime(s) {
                Some(dt) => Value::TimestampNaive(dt),
                None => value,
            },
            _ => value,
        },
        DataType::TimestampTz => match value {
            Value::Timestamp(ts) => Value::Timestamp(ts),
            Value::TimestampNaive(ts) => {
                Value::Timestamp(DateTime::<Utc>::from_naive_utc_and_offset(ts, Utc))
            }
            Value::String(ref s) => match parse_datetime(s) {
                Some(dt) => Value::Timestamp(dt),
                None => value,
            },
            _ => value,
        },
        _ => value,
    }
}

fn parse_datetime(raw: &str) -> Option<DateTime<Utc>> {
    if let Ok(dt) = DateTime::parse_from_rfc3339(raw) {
        return Some(dt.with_timezone(&Utc));
    }

    parse_naive_datetime(raw).map(|naive| DateTime::<Utc>::from_naive_utc_and_offset(naive, Utc))
}

fn parse_naive_datetime(raw: &str) -> Option<NaiveDateTime> {
    NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S%.f")
        .or_else(|_| NaiveDateTime::parse_from_str(raw, "%Y-%m-%d %H:%M:%S"))
        .or_else(|_| NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S%.f"))
        .or_else(|_| NaiveDateTime::parse_from_str(raw, "%Y-%m-%dT%H:%M:%S"))
        .ok()
        .or_else(|| {
            DateTime::parse_from_rfc3339(raw)
                .map(|dt| dt.naive_utc())
                .ok()
        })
}
