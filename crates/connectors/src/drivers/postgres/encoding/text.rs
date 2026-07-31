use crate::traits::encoder::TextCopyEncoder;
use model::core::value::Value;
use std::fmt::Write as _;

/// PostgreSQL COPY protocol encoder.
/// Encodes values for PostgreSQL's CSV-style COPY FROM STDIN.
pub struct PgCopyEncoder;

impl PgCopyEncoder {
    /// Append the CSV encoding of `value` to `out` without allocating a `String` per value (the hot COPY path).
    pub fn write_value(&self, value: &Value, out: &mut String) {
        match value {
            Value::Null => out.push_str("\\N"),
            Value::Int(n) => {
                let _ = write!(out, "{n}");
            }
            Value::UInt(n) => {
                let _ = write!(out, "{n}");
            }
            Value::Float(f) => {
                if f.is_nan() || f.is_infinite() {
                    out.push_str("\\N");
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
            Value::Boolean(b) => out.push(if *b { 't' } else { 'f' }),
            Value::Uuid(u) => {
                let _ = write!(out, "{u}");
            }

            // String / Text
            Value::String(s) => Self::write_csv_escaped(s, out),
            Value::Enum { value, .. } => Self::write_csv_escaped(value, out),
            Value::Json(j) => Self::write_csv_escaped(&j.to_string(), out),

            // Binary Types
            Value::Binary(b) | Value::Geometry(b) => Self::write_bytea(b, out),

            // Temporal Types
            Value::Date(d) => {
                let _ = write!(out, "{}", d.format("%Y-%m-%d"));
            }
            Value::Time { value, offset_secs } => {
                let _ = write!(out, "{}", value.format("%H:%M:%S%.6f"));
                Self::write_tz_offset(*offset_secs, out);
            }
            Value::Timestamp { value, offset_secs } => {
                let _ = write!(out, "{}", value.format("%Y-%m-%d %H:%M:%S%.6f"));
                Self::write_tz_offset(*offset_secs, out);
            }
            Value::Interval(iv) => {
                let _ = write!(
                    out,
                    "{} months {} days {} microseconds",
                    iv.months, iv.days, iv.microseconds
                );
            }

            // Collection Types
            Value::Array(arr) => self.write_array(arr, out),
            Value::Set(values) => self.write_set(values, out),
            Value::Composite { fields, .. } => self.write_composite(fields, out),

            // Bit strings
            Value::Bits(bits) => {
                // Reserve capacity to prevent reallocation
                out.reserve(bits.len());
                for b in bits {
                    out.push(if *b { '1' } else { '0' });
                }
            }

            // Network Types
            Value::IpAddr(addr) => {
                let _ = write!(out, "{addr}");
            }
            Value::Cidr { addr, prefix } => {
                let _ = write!(out, "{addr}/{prefix}");
            }
            Value::MacAddr(mac) => {
                let _ = write!(
                    out,
                    "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                    mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]
                );
            }
        }
    }

    /// Append a `±HH:MM` timezone offset when present.
    fn write_tz_offset(offset_secs: Option<i32>, out: &mut String) {
        if let Some(offset) = offset_secs {
            let hours = offset / 3600;
            let mins = (offset.abs() % 3600) / 60;
            let _ = write!(out, "{hours:+03}:{mins:02}");
        }
    }

    /// CSV-escape `s` directly into `out`, quoting only when required.
    fn write_csv_escaped(s: &str, out: &mut String) {
        if s.contains([',', '"', '\n', '\r']) {
            out.push('"');
            for ch in s.chars() {
                if ch == '"' {
                    out.push('"');
                }
                out.push(ch);
            }
            out.push('"');
        } else {
            out.push_str(s);
        }
    }

    /// Append `\x`-prefixed hex bytea directly into `out`.
    fn write_bytea(data: &[u8], out: &mut String) {
        out.push_str("\\x");
        for b in data {
            let _ = write!(out, "{b:02x}");
        }
    }

    /// Writes an array using PostgreSQL's array literal syntax directly to the buffer.
    fn write_array(&self, arr: &[Value], out: &mut String) {
        let mut buf = String::with_capacity(arr.len() * 8);
        buf.push('{');
        for (i, v) in arr.iter().enumerate() {
            if i > 0 {
                buf.push(',');
            }
            match v {
                Value::String(s) => {
                    buf.push('"');
                    for ch in s.chars() {
                        if ch == '"' || ch == '\\' {
                            buf.push('\\');
                        }
                        buf.push(ch);
                    }
                    buf.push('"');
                }
                Value::Int(n) => {
                    let _ = write!(buf, "{n}");
                }
                Value::UInt(u) => {
                    let _ = write!(buf, "{u}");
                }
                Value::Float(f) => {
                    let _ = write!(buf, "{f}");
                }
                Value::Boolean(b) => buf.push_str(if *b { "true" } else { "false" }),
                Value::Null => buf.push_str("NULL"),
                // Fallback for unexpected nested types (ideally should be supported recursively,
                // but aligned with original behavior).
                _ => buf.push_str("NULL"),
            }
        }
        buf.push('}');

        // Pass the constructed literal to the CSV escaper to handle outer quotes
        Self::write_csv_escaped(&buf, out);
    }

    /// Writes a set using PostgreSQL's array literal syntax.
    fn write_set(&self, values: &[String], out: &mut String) {
        let mut buf = String::with_capacity(values.len() * 8);
        buf.push('{');
        for (i, s) in values.iter().enumerate() {
            if i > 0 {
                buf.push(',');
            }
            buf.push('"');
            for ch in s.chars() {
                if ch == '"' || ch == '\\' {
                    buf.push('\\');
                }
                buf.push(ch);
            }
            buf.push('"');
        }
        buf.push('}');
        Self::write_csv_escaped(&buf, out);
    }

    /// Writes a composite type using row literal syntax `(val1,val2)`. Built into
    /// a scratch buffer and CSV-escaped as one field (like `write_array`/
    /// `write_set`): the literal's own structural commas must not be seen as CSV
    /// field separators by `COPY`.
    fn write_composite(&self, fields: &[(String, Value)], out: &mut String) {
        let mut buf = String::with_capacity(fields.len() * 8);
        buf.push('(');
        for (i, (_, v)) in fields.iter().enumerate() {
            if i > 0 {
                buf.push(',');
            }
            self.write_value(v, &mut buf);
        }
        buf.push(')');
        Self::write_csv_escaped(&buf, out);
    }
}

impl TextCopyEncoder for PgCopyEncoder {
    fn encode_value(&self, value: &Value) -> String {
        let mut out = String::new();
        self.write_value(value, &mut out);
        out
    }

    fn encode_null(&self) -> String {
        "\\N".to_string()
    }
}
