use bigdecimal::BigDecimal;
use bigdecimal::ToPrimitive;
use chrono::Utc;
use chrono::{DateTime, NaiveDate};
use common::{types::DataType, value::Value};
use std::str::FromStr;

/// The promotion sequence: start at the current type and widen until it fits.
const CHAIN: &[DataType] = &[
    DataType::Short,
    DataType::Int,
    DataType::Long,
    DataType::Decimal,
    DataType::Float,
    DataType::Double,
    DataType::Boolean,
    DataType::Date,
    DataType::Timestamp,
    DataType::Json,
    DataType::String,
    DataType::VarChar,
];

/// Check if type can parse the given string.
fn can_parse(data_type: &DataType, value: &str) -> bool {
    if value.is_empty() {
        return true; // treat empty as null
    }
    match *data_type {
        DataType::Short => value.parse::<i16>().is_ok(),
        DataType::Int => value.parse::<i32>().is_ok(),
        DataType::Long => value.parse::<i64>().is_ok(),
        DataType::Decimal => BigDecimal::from_str(value).is_ok(),
        DataType::Float => value.parse::<f32>().is_ok(),
        DataType::Double => value.parse::<f64>().is_ok(),
        DataType::Boolean => matches!(value.to_lowercase().as_str(), "true" | "false"),
        DataType::Date => NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok(),
        DataType::Timestamp => DateTime::parse_from_rfc3339(value).is_ok(),
        DataType::Json => serde_json::from_str::<serde_json::Value>(value).is_ok(),
        DataType::String | DataType::VarChar => true,
        _ => false,
    }
}

pub trait CsvType {
    fn promote(&self, value: &str) -> DataType;
    fn data_type(&self) -> DataType;
    fn get_value(&self, value: &str) -> Option<Value>;
}

impl CsvType for DataType {
    fn promote(&self, value: &str) -> DataType {
        // Find our index in the promotion chain (fallback to start)
        let start = CHAIN.iter().position(|t| t == self).unwrap_or(0);
        // Find the first type from here onward that can parse the value
        CHAIN[start..]
            .iter()
            .find(|t| can_parse(t, value))
            .cloned()
            .unwrap_or(DataType::VarChar)
    }

    fn data_type(&self) -> DataType {
        self.clone()
    }

    fn get_value(&self, value: &str) -> Option<Value> {
        if value.is_empty() {
            return None;
        }

        match *self {
            DataType::Short | DataType::Int | DataType::Long => {
                value.parse::<i64>().ok().map(Value::Int)
            }
            DataType::ShortUnsigned | DataType::IntUnsigned | DataType::Year => {
                value.parse::<u64>().ok().map(|v| Value::Int(v as i64))
            }

            DataType::Float | DataType::Double => value.parse::<f64>().ok().map(Value::Float),
            DataType::Decimal => value
                .parse::<BigDecimal>()
                .ok()
                .and_then(|d| d.to_f64().map(Value::Float)),
            DataType::String | DataType::VarChar | DataType::Char | DataType::Enum => {
                Some(Value::String(value.to_string()))
            }
            DataType::Boolean => match value.to_lowercase().as_str() {
                "true" => Some(Value::Boolean(true)),
                "false" => Some(Value::Boolean(false)),
                _ => None,
            },
            DataType::Json => serde_json::from_str::<serde_json::Value>(value)
                .ok()
                .map(Value::Json),
            DataType::Timestamp => DateTime::parse_from_rfc3339(value)
                .ok()
                .map(|dt| Value::Timestamp(dt.with_timezone(&Utc))),
            DataType::Date => NaiveDate::parse_from_str(value, "%Y-%m-%d")
                .ok()
                .map(Value::Date),
            _ => None,
        }
    }
}
