use crate::traits::encoder::CopyValueEncoder;
use model::core::value::Value;

/// PostgreSQL COPY protocol encoder.
/// Encodes values for PostgreSQL's CSV-style COPY FROM STDIN.
pub struct PgCopyEncoder;

impl PgCopyEncoder {
    /// Escapes a string for PostgreSQL COPY CSV format.
    fn escape_csv(s: &str) -> String {
        // If the string contains special characters, quote it
        if s.contains(',') || s.contains('"') || s.contains('\n') || s.contains('\r') {
            let escaped = s.replace('"', "\"\"");
            format!("\"{}\"", escaped)
        } else {
            s.to_string()
        }
    }

    /// Encodes binary data as PostgreSQL hex-bytea format for COPY CSV.
    fn encode_bytea(data: &[u8]) -> String {
        let hex: String = data.iter().map(|b| format!("{:02x}", b)).collect();
        format!("\\x{}", hex)
    }

    /// Encodes an array in PostgreSQL array literal format.
    fn encode_array(arr: &[Value]) -> String {
        let elements: Vec<String> = arr
            .iter()
            .map(|v| match v {
                Value::String(s) => format!("\"{}\"", s.replace('"', "\\\"")),
                Value::Int(i) => i.to_string(),
                Value::UInt(u) => u.to_string(),
                Value::Float(f) => f.to_string(),
                Value::Boolean(b) => b.to_string(),
                Value::Null => "NULL".to_string(),
                _ => "NULL".to_string(),
            })
            .collect();
        format!("{{{}}}", elements.join(","))
    }
}

impl CopyValueEncoder for PgCopyEncoder {
    fn encode_value(&self, value: &Value) -> String {
        match value {
            Value::Null => self.encode_null(),

            // Numeric types
            Value::Int(n) => n.to_string(),
            Value::UInt(n) => n.to_string(),
            Value::Float(f) => {
                if f.is_nan() || f.is_infinite() {
                    self.encode_null()
                } else {
                    f.to_string()
                }
            }
            Value::Decimal(d) => d.to_string(),
            Value::Year(y) => y.to_string(),

            // String types
            Value::String(s) => Self::escape_csv(s),

            // Binary
            Value::Binary(b) => Self::encode_bytea(b),

            // Temporal types
            Value::Date(d) => d.format("%Y-%m-%d").to_string(),
            Value::Time { value, offset_secs } => {
                let base = value.format("%H:%M:%S%.6f").to_string();
                match offset_secs {
                    Some(offset) => {
                        let hours = offset / 3600;
                        let mins = (offset.abs() % 3600) / 60;
                        format!("{}{:+03}:{:02}", base, hours, mins)
                    }
                    None => base,
                }
            }
            Value::Timestamp { value, offset_secs } => {
                let base = value.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
                match offset_secs {
                    Some(offset) => {
                        let hours = offset / 3600;
                        let mins = (offset.abs() % 3600) / 60;
                        format!("{}{:+03}:{:02}", base, hours, mins)
                    }
                    None => base,
                }
            }
            Value::Interval(iv) => {
                // PostgreSQL interval format
                format!(
                    "{} months {} days {} microseconds",
                    iv.months, iv.days, iv.microseconds
                )
            }

            // Boolean
            Value::Boolean(b) => if *b { "t" } else { "f" }.to_string(),

            // UUID
            Value::Uuid(u) => u.to_string(),

            // JSON
            Value::Json(j) => Self::escape_csv(&j.to_string()),

            // Bits
            Value::Bits(bits) => {
                let bit_str: String = bits.iter().map(|b| if *b { '1' } else { '0' }).collect();
                bit_str
            }

            // Array
            Value::Array(arr) => Self::escape_csv(&Self::encode_array(arr)),

            // Enum
            Value::Enum { value, .. } => Self::escape_csv(value),

            // Set - encode as array
            Value::Set(values) => {
                let arr: Vec<Value> = values.iter().map(|s| Value::String(s.clone())).collect();
                Self::escape_csv(&Self::encode_array(&arr))
            }

            // Geometry - WKB hex format
            Value::Geometry(wkb) => Self::encode_bytea(wkb),

            // Network types
            Value::IpAddr(addr) => addr.to_string(),
            Value::Cidr { addr, prefix } => format!("{}/{}", addr, prefix),
            Value::MacAddr(mac) => {
                format!(
                    "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                    mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]
                )
            }

            // Composite - encode as row literal
            Value::Composite { fields, .. } => {
                let field_values: Vec<String> =
                    fields.iter().map(|(_, v)| self.encode_value(v)).collect();
                format!("({})", field_values.join(","))
            }
        }
    }

    fn encode_null(&self) -> String {
        "\\N".to_string()
    }
}
