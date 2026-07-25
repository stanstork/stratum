use crate::traits::encoder::TextCopyEncoder;
use model::core::value::Value;
use std::fmt::Write as _;

/// MySQL LOAD DATA INFILE encoder.
/// Encodes values for MySQL's CSV-style bulk loading format.
pub struct MySqlCopyEncoder;

impl MySqlCopyEncoder {
    /// Append the LOAD DATA encoding of `value` to `out` without allocating a
    /// `String` per value (the hot LOAD DATA path).
    pub fn write_value(&self, value: &Value, out: &mut String) {
        match value {
            Value::Null => out.push_str("\\N"),

            // Numeric types
            Value::Int(n) => {
                let _ = write!(out, "{n}");
            }
            Value::UInt(n) => {
                let _ = write!(out, "{n}");
            }
            Value::Float(f) => {
                if f.is_nan() || f.is_infinite() {
                    out.push_str("NULL"); // MySQL doesn't support NaN or Infinity
                } else {
                    let _ = write!(out, "{f}");
                }
            }
            Value::Decimal(d) => {
                let _ = write!(out, "{d}");
            }
            Value::Year(y) => {
                let _ = write!(out, "{y}");
            }

            // String types
            Value::String(s) => Self::write_escaped(s, out),

            // Binary
            Value::Binary(b) => Self::write_binary_hex(b, out),

            // Temporal types
            Value::Date(d) => {
                let _ = write!(out, "{}", d.format("%Y-%m-%d"));
            }
            Value::Time { value, .. } => {
                let _ = write!(out, "{}", value.format("%H:%M:%S%.6f"));
            }
            Value::Timestamp { value, .. } => {
                let _ = write!(out, "{}", value.format("%Y-%m-%d %H:%M:%S%.6f"));
            }
            Value::Interval(iv) => {
                // MySQL doesn't have native interval type, encode as string
                let hours = iv.microseconds / 3_600_000_000;
                let mins = (iv.microseconds % 3_600_000_000) / 60_000_000;
                let secs = (iv.microseconds % 60_000_000) / 1_000_000;
                let micros = iv.microseconds % 1_000_000;
                if iv.months != 0 || iv.days != 0 {
                    let _ = write!(
                        out,
                        "{} months {} days {:02}:{:02}:{:02}.{:06}",
                        iv.months, iv.days, hours, mins, secs, micros
                    );
                } else {
                    let _ = write!(out, "{:02}:{:02}:{:02}.{:06}", hours, mins, secs, micros);
                }
            }

            // Boolean
            Value::Boolean(b) => out.push(if *b { '1' } else { '0' }),

            // UUID - stored as string in MySQL
            Value::Uuid(u) => {
                let _ = write!(out, "{u}");
            }

            // JSON
            Value::Json(j) => Self::write_escaped(&j.to_string(), out),

            // Bits - encode as binary string
            Value::Bits(bits) => {
                out.push_str("b'");
                out.reserve(bits.len());
                for b in bits {
                    out.push(if *b { '1' } else { '0' });
                }
                out.push('\'');
            }

            // Array - JSON array
            Value::Array(arr) => {
                let json =
                    serde_json::Value::Array(arr.iter().map(|v| self.value_to_json(v)).collect());
                Self::write_escaped(&json.to_string(), out);
            }

            // Enum
            Value::Enum { value, .. } => Self::write_escaped(value, out),

            // Set - comma-separated values
            Value::Set(values) => Self::write_escaped(&values.join(","), out),

            // Geometry - WKB format as hex
            Value::Geometry(wkb) => Self::write_binary_hex(wkb, out),

            // Network types - not native to MySQL, store as string
            Value::IpAddr(addr) => {
                let _ = write!(out, "{addr}");
            }
            Value::Cidr { addr, prefix } => {
                let _ = write!(out, "{addr}/{prefix}");
            }
            Value::MacAddr(mac) => {
                let _ = write!(
                    out,
                    "{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}",
                    mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]
                );
            }

            // Composite - encode as JSON object
            Value::Composite { fields, .. } => {
                let json = serde_json::Value::Object(
                    fields
                        .iter()
                        .map(|(k, v)| (k.clone(), self.value_to_json(v)))
                        .collect(),
                );
                Self::write_escaped(&json.to_string(), out);
            }
        }
    }

    /// Escape a string for MySQL LOAD DATA INFILE directly into `out`. MySQL uses
    /// backslash escaping for the field/row delimiters and special characters.
    fn write_escaped(s: &str, out: &mut String) {
        for c in s.chars() {
            match c {
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                '\0' => out.push_str("\\0"),
                _ => out.push(c),
            }
        }
    }

    /// Append binary data as a `0x`-prefixed hex string directly into `out`.
    fn write_binary_hex(data: &[u8], out: &mut String) {
        out.push_str("0x");
        for b in data {
            let _ = write!(out, "{b:02X}");
        }
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

impl TextCopyEncoder for MySqlCopyEncoder {
    fn encode_value(&self, value: &Value) -> String {
        let mut out = String::new();
        self.write_value(value, &mut out);
        out
    }

    fn encode_null(&self) -> String {
        "\\N".to_string()
    }
}
