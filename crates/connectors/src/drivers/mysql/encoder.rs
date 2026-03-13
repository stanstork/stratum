use crate::traits::encoder::CopyValueEncoder;
use model::core::value::Value;

/// MySQL LOAD DATA INFILE encoder.
/// Encodes values for MySQL's CSV-style bulk loading format.
pub struct MySqlCopyEncoder;

impl MySqlCopyEncoder {
    /// Escapes a string for MySQL LOAD DATA INFILE format.
    /// MySQL uses backslash escaping for special characters.
    fn escape_string(s: &str) -> String {
        let mut result = String::with_capacity(s.len());
        for c in s.chars() {
            match c {
                '\\' => result.push_str("\\\\"),
                '\n' => result.push_str("\\n"),
                '\r' => result.push_str("\\r"),
                '\t' => result.push_str("\\t"),
                '\0' => result.push_str("\\0"),
                _ => result.push(c),
            }
        }
        result
    }

    /// Encodes binary data as hex string for MySQL.
    fn encode_binary(data: &[u8]) -> String {
        let hex: String = data.iter().map(|b| format!("{:02X}", b)).collect();
        format!("0x{}", hex)
    }

    /// Converts a Value to a serde_json::Value for JSON encoding.
    fn value_to_json(&self, value: &Value) -> serde_json::Value {
        match value {
            Value::Null => serde_json::Value::Null,
            Value::Int(n) => serde_json::json!(n),
            Value::UInt(n) => serde_json::json!(n),
            Value::Float(f) => serde_json::json!(f),
            Value::Decimal(d) => serde_json::json!(d.to_string()),
            Value::String(s) => serde_json::json!(s),
            Value::Boolean(b) => serde_json::json!(b),
            Value::Json(j) => j.clone(),
            Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(|v| self.value_to_json(v)).collect())
            }
            _ => serde_json::json!(self.encode_value(value)),
        }
    }
}

impl CopyValueEncoder for MySqlCopyEncoder {
    fn encode_value(&self, value: &Value) -> String {
        match value {
            Value::Null => self.encode_null(),

            // Numeric types
            Value::Int(n) => n.to_string(),
            Value::UInt(n) => n.to_string(),
            Value::Float(f) => {
                if f.is_nan() || f.is_infinite() {
                    "NULL".to_string() // MySQL doesn't support NaN or Infinity
                } else {
                    f.to_string()
                }
            }
            Value::Decimal(d) => d.to_string(),
            Value::Year(y) => y.to_string(),

            // String types
            Value::String(s) => Self::escape_string(s),

            // Binary
            Value::Binary(b) => Self::encode_binary(b),

            // Temporal types
            Value::Date(d) => d.format("%Y-%m-%d").to_string(),
            Value::Time { value, .. } => value.format("%H:%M:%S%.6f").to_string(),
            Value::Timestamp { value, .. } => value.format("%Y-%m-%d %H:%M:%S%.6f").to_string(),
            Value::Interval(iv) => {
                // MySQL doesn't have native interval type, encode as string
                let hours = iv.microseconds / 3_600_000_000;
                let mins = (iv.microseconds % 3_600_000_000) / 60_000_000;
                let secs = (iv.microseconds % 60_000_000) / 1_000_000;
                let micros = iv.microseconds % 1_000_000;
                if iv.months != 0 || iv.days != 0 {
                    format!(
                        "{} months {} days {:02}:{:02}:{:02}.{:06}",
                        iv.months, iv.days, hours, mins, secs, micros
                    )
                } else {
                    format!("{:02}:{:02}:{:02}.{:06}", hours, mins, secs, micros)
                }
            }

            // Boolean
            Value::Boolean(b) => if *b { "1" } else { "0" }.to_string(),

            // UUID - stored as string in MySQL
            Value::Uuid(u) => u.to_string(),

            // JSON
            Value::Json(j) => Self::escape_string(&j.to_string()),

            // Bits - encode as binary string
            Value::Bits(bits) => {
                let bit_str: String = bits.iter().map(|b| if *b { '1' } else { '0' }).collect();
                format!("b'{}'", bit_str)
            }

            // Array - MySQL SET or JSON array
            Value::Array(arr) => {
                let json =
                    serde_json::Value::Array(arr.iter().map(|v| self.value_to_json(v)).collect());
                Self::escape_string(&json.to_string())
            }

            // Enum
            Value::Enum { value, .. } => Self::escape_string(value),

            // Set - comma-separated values
            Value::Set(values) => Self::escape_string(&values.join(",")),

            // Geometry - WKB format as hex
            Value::Geometry(wkb) => Self::encode_binary(wkb),

            // Network types - not native to MySQL, store as string
            Value::IpAddr(addr) => addr.to_string(),
            Value::Cidr { addr, prefix } => format!("{}/{}", addr, prefix),
            Value::MacAddr(mac) => {
                format!(
                    "{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}",
                    mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]
                )
            }

            // Composite - encode as JSON
            Value::Composite { fields, .. } => {
                let json = serde_json::Value::Object(
                    fields
                        .iter()
                        .map(|(k, v)| (k.clone(), self.value_to_json(v)))
                        .collect(),
                );
                Self::escape_string(&json.to_string())
            }
        }
    }

    fn encode_null(&self) -> String {
        "\\N".to_string()
    }
}
