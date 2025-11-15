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

    fn encode_array_literal(&self, values: &[String]) -> String {
        let mut literal = String::from('{');
        for (idx, value) in values.iter().enumerate() {
            if idx > 0 {
                literal.push(',');
            }
            literal.push_str(&Self::quote_array_item(value));
        }
        literal.push('}');
        literal
    }

    fn quote_array_item(value: &str) -> String {
        let mut quoted = String::from('"');
        for ch in value.chars() {
            match ch {
                '"' => quoted.push_str("\\\""),
                '\\' => quoted.push_str("\\\\"),
                _ => quoted.push(ch),
            }
        }
        quoted.push('"');
        quoted
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
                let literal = self.encode_array_literal(values);
                escape_csv_string(&literal)
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
