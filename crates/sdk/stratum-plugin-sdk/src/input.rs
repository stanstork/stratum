use crate::{
    error::{PluginError, PluginResult},
    exchange::json_v1,
    value::Value,
};
use bigdecimal::BigDecimal;
use chrono::{NaiveDate, NaiveDateTime};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use uuid::Uuid;

/// Input to a transform or filter plugin call.
#[derive(Debug, Clone, Default)]
pub struct PluginInput {
    fields: HashMap<String, Value>,
}

impl PluginInput {
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse input from the raw JSON bytes delivered by the host.
    pub fn from_json_bytes(bytes: &[u8]) -> PluginResult<Self> {
        let json: JsonValue = serde_json::from_slice(bytes)?;
        let obj = json
            .as_object()
            .ok_or_else(|| PluginError::invalid_input("input is not a JSON object"))?;
        let mut fields = HashMap::with_capacity(obj.len());
        for (k, v) in obj {
            fields.insert(k.clone(), json_v1::json_to_value(v)?);
        }
        Ok(Self { fields })
    }

    pub fn insert(&mut self, key: impl Into<String>, value: impl Into<Value>) {
        self.fields.insert(key.into(), value.into());
    }

    pub fn contains(&self, key: &str) -> bool {
        self.fields.contains_key(key)
    }

    pub fn get_string(&self, key: &str) -> PluginResult<&str> {
        match self.require(key)? {
            Value::String(s) => Ok(s.as_str()),
            other => Err(type_err(key, "string", other.type_tag())),
        }
    }

    pub fn get_i64(&self, key: &str) -> PluginResult<i64> {
        match self.require(key)? {
            Value::Int(i) => Ok(*i),
            Value::UInt(u) => (*u)
                .try_into()
                .map_err(|_| PluginError::invalid_input(format!("{}: u64 out of i64 range", key))),
            other => Err(type_err(key, "i64", other.type_tag())),
        }
    }

    pub fn get_u64(&self, key: &str) -> PluginResult<u64> {
        match self.require(key)? {
            Value::UInt(u) => Ok(*u),
            Value::Int(i) if *i >= 0 => Ok(*i as u64),
            other => Err(type_err(key, "u64", other.type_tag())),
        }
    }

    pub fn get_f64(&self, key: &str) -> PluginResult<f64> {
        match self.require(key)? {
            Value::Float(f) => Ok(*f),
            Value::Int(i) => Ok(*i as f64),
            Value::UInt(u) => Ok(*u as f64),
            other => Err(type_err(key, "f64", other.type_tag())),
        }
    }

    pub fn get_bool(&self, key: &str) -> PluginResult<bool> {
        match self.require(key)? {
            Value::Boolean(b) => Ok(*b),
            other => Err(type_err(key, "bool", other.type_tag())),
        }
    }

    pub fn get_decimal(&self, key: &str) -> PluginResult<&BigDecimal> {
        match self.require(key)? {
            Value::Decimal(d) => Ok(d),
            other => Err(type_err(key, "decimal", other.type_tag())),
        }
    }

    pub fn get_date(&self, key: &str) -> PluginResult<NaiveDate> {
        match self.require(key)? {
            Value::Date(d) => Ok(*d),
            other => Err(type_err(key, "date", other.type_tag())),
        }
    }

    pub fn get_timestamp(&self, key: &str) -> PluginResult<NaiveDateTime> {
        match self.require(key)? {
            Value::Timestamp { value, .. } => Ok(*value),
            other => Err(type_err(key, "timestamp", other.type_tag())),
        }
    }

    pub fn get_uuid(&self, key: &str) -> PluginResult<Uuid> {
        match self.require(key)? {
            Value::Uuid(u) => Ok(*u),
            other => Err(type_err(key, "uuid", other.type_tag())),
        }
    }

    pub fn get_bytes(&self, key: &str) -> PluginResult<&[u8]> {
        match self.require(key)? {
            Value::Binary(b) => Ok(b.as_slice()),
            other => Err(type_err(key, "bytes", other.type_tag())),
        }
    }

    pub fn get_json(&self, key: &str) -> PluginResult<&JsonValue> {
        match self.require(key)? {
            Value::Json(j) => Ok(j),
            other => Err(type_err(key, "json", other.type_tag())),
        }
    }

    pub fn is_null(&self, key: &str) -> bool {
        matches!(self.fields.get(key), Some(Value::Null))
    }

    fn require(&self, key: &str) -> PluginResult<&Value> {
        let v = self
            .fields
            .get(key)
            .ok_or_else(|| PluginError::invalid_input(format!("missing input field: '{}'", key)))?;
        if matches!(v, Value::Null) {
            return Err(PluginError::invalid_input(format!(
                "field '{}' is null; use get_optional_* to allow null",
                key
            )));
        }
        Ok(v)
    }
}

fn type_err(key: &str, expected: &str, actual: &str) -> PluginError {
    PluginError::invalid_input(format!(
        "field '{}': expected {}, got {}",
        key, expected, actual
    ))
}
