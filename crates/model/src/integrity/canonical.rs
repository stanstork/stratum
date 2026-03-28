use crate::core::value::Value;
use chrono::{NaiveDate, Timelike};

pub const TAG_NULL: u8 = 0x00;
pub const TAG_INT: u8 = 0x01;
pub const TAG_UINT: u8 = 0x02;
pub const TAG_BOOL: u8 = 0x03;
pub const TAG_STRING: u8 = 0x10;
pub const TAG_DECIMAL: u8 = 0x11;
pub const TAG_FLOAT: u8 = 0x12;
pub const TAG_DATE: u8 = 0x20;
pub const TAG_TIMESTAMP: u8 = 0x21;
pub const TAG_TIME: u8 = 0x22;
pub const TAG_INTERVAL: u8 = 0x23;
pub const TAG_YEAR: u8 = 0x24;
pub const TAG_UUID: u8 = 0x30;
pub const TAG_BINARY: u8 = 0x40;
pub const TAG_GEOMETRY: u8 = 0x41;
pub const TAG_BITS: u8 = 0x42;
pub const TAG_JSON: u8 = 0x50;
pub const TAG_ARRAY: u8 = 0x60;
pub const TAG_SET: u8 = 0x61;
pub const TAG_ENUM: u8 = 0x70;
pub const TAG_INET: u8 = 0x80;
pub const TAG_CIDR: u8 = 0x81;
pub const TAG_MACADDR: u8 = 0x82;
pub const TAG_COMPOSITE: u8 = 0x90;

pub fn serialize_value(val: &Value, buf: &mut Vec<u8>) {
    match val {
        Value::Null => buf.push(TAG_NULL),

        Value::Int(i) => {
            buf.push(TAG_INT);
            buf.extend(&i.to_le_bytes());
        }

        Value::UInt(u) => {
            // PostgreSQL has no unsigned integer types - any UInt that fits in i64 will
            // be read back as Int after a round-trip through PG. Normalise here so that
            // Value::UInt(1) and Value::Int(1) produce identical canonical bytes.
            // Values that exceed i64::MAX cannot be stored in any PG integer column and
            // retain the distinct TAG_UINT encoding to flag potential data loss.
            if *u <= i64::MAX as u64 {
                buf.push(TAG_INT);
                buf.extend(&(*u as i64).to_le_bytes());
            } else {
                buf.push(TAG_UINT);
                buf.extend(&u.to_le_bytes());
            }
        }

        Value::Boolean(b) => {
            // Normalize to TAG_INT for cross-database compatibility.
            // MySQL TINYINT(1) is read as Value::Int(0/1) while PostgreSQL BOOLEAN
            // is read as Value::Boolean(false/true). Both represent the same logical
            // value; encoding both as Int ensures hashes match across a migration.
            buf.push(TAG_INT);
            buf.extend(&(*b as i64).to_le_bytes());
        }

        Value::String(s) => {
            buf.push(TAG_STRING);
            write_bytes(s.as_bytes(), buf);
        }

        Value::Decimal(d) => {
            // normalized() strips trailing zeros: 1.50 -> 1.5
            buf.push(TAG_DECIMAL);
            write_bytes(d.normalized().to_string().as_bytes(), buf);
        }

        Value::Float(f) => {
            if f.is_nan() {
                // NaN has no defined equality - treat as NULL
                buf.push(TAG_NULL);
            } else {
                buf.push(TAG_FLOAT);
                // Normalize -0.0 -> +0.0
                let f = if *f == 0.0_f64 { 0.0_f64 } else { *f };
                buf.extend(&f.to_be_bytes());
            }
        }

        Value::Date(d) => {
            buf.push(TAG_DATE);
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
            let days = d.signed_duration_since(epoch).num_days() as i32;
            buf.extend(&days.to_le_bytes());
        }

        Value::Timestamp { value, offset_secs } => {
            buf.push(TAG_TIMESTAMP);
            // Normalize to UTC: subtract the UTC offset so stored value is always UTC
            let utc = *value - chrono::Duration::seconds(offset_secs.unwrap_or(0) as i64);
            let micros = utc
                .signed_duration_since(chrono::NaiveDateTime::UNIX_EPOCH)
                .num_microseconds()
                .unwrap_or(0);
            buf.extend(&micros.to_le_bytes());
        }

        Value::Time { value, .. } => {
            // offset_secs on a NaiveTime is ambiguous - encode the raw time only
            buf.push(TAG_TIME);
            let micros = value.num_seconds_from_midnight() as i64 * 1_000_000
                + value.nanosecond() as i64 / 1_000;
            buf.extend(&micros.to_le_bytes());
        }

        Value::Interval(iv) => {
            buf.push(TAG_INTERVAL);
            buf.extend(&iv.months.to_le_bytes());
            buf.extend(&iv.days.to_le_bytes());
            buf.extend(&iv.microseconds.to_le_bytes());
        }

        Value::Year(y) => {
            // Normalize to TAG_INT for cross-database compatibility.
            // PostgreSQL has no YEAR type; MySQL YEAR columns are stored as SMALLINT
            // in PostgreSQL and read back as Value::Int. Encoding both as Int ensures
            // hashes match across a migration.
            buf.push(TAG_INT);
            buf.extend(&(*y as i64).to_le_bytes());
        }

        Value::Uuid(u) => {
            buf.push(TAG_UUID);
            buf.extend(u.as_bytes());
        }

        Value::Binary(b) => {
            // Normalize valid-UTF-8 binary to String encoding for cross-database
            // compatibility. MySQL reads VARCHAR columns with binary charset/collation
            // as Value::Binary while PostgreSQL reads the same column as Value::String.
            // Only true binary content (invalid UTF-8 bytes, e.g. image data) keeps
            // the TAG_BINARY encoding.
            if let Ok(s) = std::str::from_utf8(b) {
                buf.push(TAG_STRING);
                write_bytes(s.as_bytes(), buf);
            } else {
                buf.push(TAG_BINARY);
                write_bytes(b, buf);
            }
        }

        Value::Geometry(g) => {
            // Raw WKB bytes - same treatment as binary
            buf.push(TAG_GEOMETRY);
            write_bytes(g, buf);
        }

        Value::Bits(bits) => {
            buf.push(TAG_BITS);
            buf.extend(&(bits.len() as u32).to_le_bytes());
            // Pack bits MSB-first into bytes
            for chunk in bits.chunks(8) {
                let mut byte = 0u8;
                for (i, &bit) in chunk.iter().enumerate() {
                    if bit {
                        byte |= 1 << (7 - i); // set bit from left to right
                    }
                }
                buf.push(byte);
            }
        }

        Value::Json(j) => {
            buf.push(TAG_JSON);
            write_bytes(canonical_json(j).as_bytes(), buf);
        }

        Value::Array(arr) => {
            buf.push(TAG_ARRAY);
            buf.extend(&(arr.len() as u32).to_le_bytes());
            for elem in arr {
                serialize_value(elem, buf);
            }
        }

        Value::Set(set) => {
            // Normalize to TAG_ARRAY of String elements for cross-database compatibility.
            // MySQL SET -> PostgreSQL TEXT[] during migration; PG reads back as Value::Array.
            // MySQL returns SET elements in schema-definition order; PG preserves insertion
            // order (= schema order), so element ordering matches without sorting.
            buf.push(TAG_ARRAY);
            buf.extend(&(set.len() as u32).to_le_bytes());
            for s in set {
                buf.push(TAG_STRING);
                write_bytes(s.as_bytes(), buf);
            }
        }

        Value::Enum { value, .. } => {
            // Normalize to TAG_STRING for cross-database compatibility.
            // MySQL ENUM -> PostgreSQL VARCHAR during migration; PG reads back as Value::String.
            // Encode only the string value (type_name is schema metadata, not row data).
            buf.push(TAG_STRING);
            write_bytes(value.as_bytes(), buf);
        }

        Value::IpAddr(addr) => {
            buf.push(TAG_INET);
            match addr {
                std::net::IpAddr::V4(v4) => {
                    buf.push(4);
                    buf.extend(&v4.octets());
                }
                std::net::IpAddr::V6(v6) => {
                    buf.push(6);
                    buf.extend(&v6.octets());
                }
            }
        }

        Value::Cidr { addr, prefix } => {
            buf.push(TAG_CIDR);
            match addr {
                std::net::IpAddr::V4(v4) => {
                    buf.push(4);
                    buf.extend(&v4.octets());
                }
                std::net::IpAddr::V6(v6) => {
                    buf.push(6);
                    buf.extend(&v6.octets());
                }
            }
            buf.push(*prefix);
        }

        Value::MacAddr(m) => {
            buf.push(TAG_MACADDR);
            buf.extend(m);
        }

        Value::Composite { fields, .. } => {
            // Sort by field name for determinism - composite field order is schema-defined
            // but driver result order may vary
            buf.push(TAG_COMPOSITE);
            let mut sorted: Vec<_> = fields.iter().collect();
            sorted.sort_unstable_by(|(a, _), (b, _)| a.cmp(b));
            buf.extend(&(sorted.len() as u32).to_le_bytes());
            for (name, val) in sorted {
                write_bytes(name.as_bytes(), buf);
                serialize_value(val, buf);
            }
        }
    }
}

/// Write a length-prefixed byte slice: 4-byte LE length + raw bytes.
#[inline]
fn write_bytes(bytes: &[u8], buf: &mut Vec<u8>) {
    buf.extend(&(bytes.len() as u32).to_le_bytes());
    buf.extend(bytes);
}

/// Canonical JSON: compact, object keys sorted alphabetically, no whitespace.
fn canonical_json(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort_unstable();
            let pairs: Vec<String> = keys
                .into_iter()
                .map(|k| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(k).unwrap(),
                        canonical_json(&map[k])
                    )
                })
                .collect();
            format!("{{{}}}", pairs.join(","))
        }
        serde_json::Value::Array(arr) => {
            let elems: Vec<String> = arr.iter().map(canonical_json).collect();
            format!("[{}]", elems.join(","))
        }
        other => serde_json::to_string(other).unwrap(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(val: &Value) -> Vec<u8> {
        let mut buf = Vec::new();
        serialize_value(val, &mut buf);
        buf
    }

    #[test]
    fn null_is_single_byte() {
        assert_eq!(encode(&Value::Null), vec![TAG_NULL]);
    }

    #[test]
    fn nan_float_encodes_as_null() {
        assert_eq!(encode(&Value::Float(f64::NAN)), vec![TAG_NULL]);
    }

    #[test]
    fn negative_zero_equals_positive_zero() {
        assert_eq!(encode(&Value::Float(-0.0)), encode(&Value::Float(0.0)));
    }

    #[test]
    fn decimal_trailing_zeros_stripped() {
        use bigdecimal::BigDecimal;
        use std::str::FromStr;
        let a = encode(&Value::Decimal(BigDecimal::from_str("1.50").unwrap()));
        let b = encode(&Value::Decimal(BigDecimal::from_str("1.5").unwrap()));
        assert_eq!(a, b);
    }

    #[test]
    fn enum_normalizes_to_string() {
        // Enum with different type_names but same value -> same bytes (type_name is schema metadata).
        // Also: Value::Enum and Value::String with same value -> same bytes (cross-DB compat).
        let a = encode(&Value::Enum {
            type_name: "rating".to_string(),
            value: "PG".to_string(),
        });
        let b = encode(&Value::Enum {
            type_name: "mpaa_rating".to_string(),
            value: "PG".to_string(),
        });
        assert_eq!(a, b);
        assert_eq!(a, encode(&Value::String("PG".to_string())));
    }

    #[test]
    fn set_normalizes_to_array_of_strings() {
        // Value::Set and Value::Array of String values produce the same canonical bytes
        // (cross-DB compat: MySQL SET -> PostgreSQL TEXT[]).
        let set = encode(&Value::Set(vec!["a".to_string(), "b".to_string()]));
        let arr = encode(&Value::Array(vec![
            Value::String("a".to_string()),
            Value::String("b".to_string()),
        ]));
        assert_eq!(set, arr);
    }

    #[test]
    fn year_normalizes_to_int() {
        // Value::Year and Value::Int with same value -> same bytes (cross-DB compat:
        // MySQL YEAR -> PostgreSQL SMALLINT which reads back as Int).
        assert_eq!(encode(&Value::Year(2006)), encode(&Value::Int(2006)));
    }

    #[test]
    fn timestamp_utc_normalization() {
        use chrono::NaiveDate;
        // 10:00 +05:00 == 05:00 UTC
        let plus5 = Value::Timestamp {
            value: NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(10, 0, 0)
                .unwrap(),
            offset_secs: Some(5 * 3600),
        };
        let utc = Value::Timestamp {
            value: NaiveDate::from_ymd_opt(2024, 1, 15)
                .unwrap()
                .and_hms_opt(5, 0, 0)
                .unwrap(),
            offset_secs: None,
        };
        assert_eq!(encode(&plus5), encode(&utc));
    }

    #[test]
    fn json_keys_sorted() {
        let j: serde_json::Value = serde_json::from_str(r#"{"b":2,"a":1}"#).unwrap();
        let encoded = encode(&Value::Json(j));
        // extract the string after the 5-byte header (tag + 4-byte len)
        let s = std::str::from_utf8(&encoded[5..]).unwrap();
        assert_eq!(s, r#"{"a":1,"b":2}"#);
    }
}
