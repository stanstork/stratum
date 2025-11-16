use crate::sql::base::encoder::CopyValueEncoder;
use model::core::{utils::escape_csv_string, value::Value};
use std::fmt::Write;

pub struct MySqlCopyValueEncoder;

impl MySqlCopyValueEncoder {
    pub fn new() -> Self {
        Self
    }

    fn encode_bytes(&self, bytes: &[u8]) -> String {
        let mut out = String::with_capacity(2 + 2 * bytes.len());
        out.push_str("0x");
        for byte in bytes {
            write!(&mut out, "{:02x}", byte).expect("failed to format hex byte");
        }
        out
    }
}

impl CopyValueEncoder for MySqlCopyValueEncoder {
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
            Value::Decimal(v) => v.to_string(),
            Value::SmallInt(v) => v.to_string(),
            Value::Int32(v) => v.to_string(),
            Value::Bytes(bytes) => {
                let hex = self.encode_bytes(bytes);
                escape_csv_string(&hex)
            }
            Value::Boolean(v) => (if *v { "1" } else { "0" }).to_string(),
            Value::Int(v) => v.to_string(),
            Value::Uint(v) => v.to_string(),
            Value::Usize(v) => v.to_string(),
            Value::Float(v) => ryu::Buffer::new().format(*v).to_string(),
            Value::Uuid(v) => escape_csv_string(&v.to_string()),
            Value::Date(d) => d.format("%Y-%m-%d").to_string(),
            Value::Timestamp(ts) => ts.naive_utc().format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
            Value::TimestampNaive(ts) => ts.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
        }
    }

    fn encode_null(&self) -> String {
        "\\N".to_string()
    }
}
