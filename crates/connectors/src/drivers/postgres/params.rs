use chrono::{DateTime, TimeZone, Utc};
use model::core::value::Value;
use rust_decimal::Decimal as RustDecimal;
use std::str::FromStr;
use tokio_postgres::types::{Json as PgJson, ToSql};

pub struct PgParam(Box<dyn ToSql + Sync + Send>);

impl PgParam {
    pub fn from_value(value: &Value) -> Self {
        match value {
            Value::Null => PgParam(Box::new(Option::<String>::None)),

            // Numeric types
            Value::Int(i) => PgParam(Box::new(*i)),
            Value::UInt(u) => PgParam(Box::new(*u as i64)),
            Value::Float(f) => PgParam(Box::new(*f)),
            Value::Decimal(d) => {
                let decimal = RustDecimal::from_str(&d.to_string()).unwrap_or_default();
                PgParam(Box::new(decimal))
            }

            // String
            Value::String(s) => PgParam(Box::new(s.clone())),

            // Binary
            Value::Binary(b) => PgParam(Box::new(b.clone())),

            // Boolean
            Value::Boolean(b) => PgParam(Box::new(*b)),

            // Temporal types
            Value::Date(d) => PgParam(Box::new(*d)),
            Value::Time { value: t, .. } => PgParam(Box::new(*t)),
            Value::Timestamp {
                value: ts,
                offset_secs: Some(offset),
            } => {
                // TIMESTAMPTZ column: send as DateTime<Utc>
                let dt: DateTime<Utc> =
                    Utc.from_utc_datetime(ts) + chrono::Duration::seconds(*offset as i64);
                PgParam(Box::new(dt))
            }
            Value::Timestamp { value: ts, .. } => PgParam(Box::new(*ts)),
            Value::Year(y) => PgParam(Box::new(*y)),

            // JSON
            Value::Json(j) => PgParam(Box::new(PgJson(j.clone()))),

            // UUID
            Value::Uuid(u) => PgParam(Box::new(*u)),

            // Enum and Set
            Value::Enum { value: v, .. } => PgParam(Box::new(v.clone())),
            Value::Set(values) => PgParam(Box::new(values.clone())),

            // Array
            Value::Array(arr) => {
                // Convert to string array for simplicity
                let strings: Vec<String> = arr
                    .iter()
                    .map(|v| match v {
                        Value::String(s) => s.clone(),
                        Value::Int(i) => i.to_string(),
                        Value::UInt(u) => u.to_string(),
                        Value::Float(f) => f.to_string(),
                        Value::Boolean(b) => b.to_string(),
                        _ => String::new(),
                    })
                    .collect();
                PgParam(Box::new(strings))
            }

            // Bits - convert to bytes
            Value::Bits(bits) => {
                let mut bytes = Vec::new();
                for chunk in bits.chunks(8) {
                    let mut byte = 0u8;
                    for (i, &bit) in chunk.iter().enumerate() {
                        if bit {
                            byte |= 1 << (7 - i);
                        }
                    }
                    bytes.push(byte);
                }
                PgParam(Box::new(bytes))
            }

            // Interval - as string (PG has native interval but complex to convert)
            Value::Interval(iv) => {
                let interval_str = format!(
                    "{} months {} days {} microseconds",
                    iv.months, iv.days, iv.microseconds
                );
                PgParam(Box::new(interval_str))
            }

            // Network types
            Value::IpAddr(addr) => PgParam(Box::new(*addr)),
            Value::Cidr { addr, prefix } => PgParam(Box::new(format!("{}/{}", addr, prefix))),
            Value::MacAddr(mac) => PgParam(Box::new(format!(
                "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]
            ))),

            // Geometry - as binary (WKB)
            Value::Geometry(g) => PgParam(Box::new(g.clone())),

            // Composite - as JSON
            Value::Composite { fields, .. } => {
                let json_obj: serde_json::Map<String, serde_json::Value> = fields
                    .iter()
                    .map(|(k, v)| {
                        let json_val = match v {
                            Value::String(s) => serde_json::Value::String(s.clone()),
                            Value::Int(i) => serde_json::json!(i),
                            Value::UInt(u) => serde_json::json!(u),
                            Value::Float(f) => serde_json::json!(f),
                            Value::Boolean(b) => serde_json::json!(b),
                            Value::Null => serde_json::Value::Null,
                            _ => serde_json::Value::Null,
                        };
                        (k.clone(), json_val)
                    })
                    .collect();
                PgParam(Box::new(PgJson(serde_json::Value::Object(json_obj))))
            }
        }
    }
}

impl AsRef<dyn ToSql + Sync> for PgParam {
    fn as_ref(&self) -> &(dyn ToSql + Sync + 'static) {
        &*self.0
    }
}

pub struct PgParamStore {
    pub params: Vec<PgParam>,
}

impl PgParamStore {
    pub fn from_values(values: &[Value]) -> Self {
        let params = values.iter().map(PgParam::from_value).collect();
        PgParamStore { params }
    }

    pub fn as_refs(&self) -> Vec<&(dyn ToSql + Sync)> {
        self.params.iter().map(|param| param.as_ref()).collect()
    }
}
