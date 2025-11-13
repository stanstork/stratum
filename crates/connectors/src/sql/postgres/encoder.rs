use crate::sql::base::encoder::CopyValueEncoder;
use chrono::SecondsFormat;
use model::core::{
    utils::{encode_bytea, escape_csv_string},
    value::Value,
};

pub struct PgCopyValueEncoder;

impl PgCopyValueEncoder {
    pub fn new() -> Self {
        Self
    }
}

impl CopyValueEncoder for PgCopyValueEncoder {
    fn encode_value(&self, value: &Value) -> String {
        match value {
            Value::Null => self.encode_null(),
            Value::String(s) => escape_csv_string(s),
            Value::Json(v) => escape_csv_string(&v.to_string()),
            Value::Enum(_, v) => escape_csv_string(v),
            Value::StringArray(values) => {
                let json = serde_json::to_string(values).unwrap_or_else(|_| "[]".to_string());
                escape_csv_string(&json)
            }
            Value::Bytes(bytes) => {
                let hex = encode_bytea(bytes);
                escape_csv_string(&hex)
            }
            Value::Boolean(v) => v.to_string(),
            Value::Int(v) => v.to_string(),
            Value::Uint(v) => v.to_string(),
            Value::Usize(v) => v.to_string(),
            Value::Float(v) => ryu::Buffer::new().format(*v).to_string(),
            Value::Uuid(v) => v.to_string(),
            Value::Date(d) => d.to_string(),
            Value::Timestamp(ts) => ts.to_rfc3339_opts(SecondsFormat::Micros, true),
        }
    }

    fn encode_null(&self) -> String {
        "\\N".to_string()
    }
}
