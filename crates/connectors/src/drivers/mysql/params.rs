use chrono::{Datelike, Timelike};
use model::core::value::Value;
use mysql_async::Value as MySqlValue;
use mysql_common::params::Params;

pub struct MySqlParam(MySqlValue);

impl MySqlParam {
    pub fn from_value(value: &Value) -> Self {
        let mysql_value = match value {
            Value::Null => MySqlValue::NULL,

            // Numeric types
            Value::Int(i) => MySqlValue::Int(*i),
            Value::UInt(u) => MySqlValue::UInt(*u),
            Value::Float(f) => MySqlValue::Double(*f),
            Value::Decimal(d) => {
                // MySQL expects decimal as bytes
                MySqlValue::Bytes(d.to_string().into_bytes())
            }

            // String
            Value::String(s) => MySqlValue::Bytes(s.as_bytes().to_vec()),

            // Binary
            Value::Binary(b) => MySqlValue::Bytes(b.clone()),

            // Boolean
            Value::Boolean(b) => MySqlValue::Int(if *b { 1 } else { 0 }),

            // Temporal types
            Value::Date(d) => {
                MySqlValue::Date(d.year() as u16, d.month() as u8, d.day() as u8, 0, 0, 0, 0)
            }
            Value::Time { value: t, .. } => MySqlValue::Time(
                false,
                0,
                t.hour() as u8,
                t.minute() as u8,
                t.second() as u8,
                t.nanosecond() / 1000,
            ),
            Value::Timestamp { value: ts, .. } => MySqlValue::Date(
                ts.date().year() as u16,
                ts.date().month() as u8,
                ts.date().day() as u8,
                ts.time().hour() as u8,
                ts.time().minute() as u8,
                ts.time().second() as u8,
                ts.time().nanosecond() / 1000,
            ),
            Value::Year(y) => MySqlValue::Int(*y as i64),

            // JSON
            Value::Json(j) => MySqlValue::Bytes(j.to_string().into_bytes()),

            // UUID
            Value::Uuid(u) => MySqlValue::Bytes(u.to_string().into_bytes()),

            // Enum and Set
            Value::Enum { value: v, .. } => MySqlValue::Bytes(v.as_bytes().to_vec()),
            Value::Set(values) => MySqlValue::Bytes(values.join(",").into_bytes()),

            // Array - serialize as JSON for MySQL
            Value::Array(arr) => {
                let json_arr: Vec<String> = arr
                    .iter()
                    .map(|v| match v {
                        Value::String(s) => format!("\"{}\"", s),
                        Value::Int(i) => i.to_string(),
                        Value::UInt(u) => u.to_string(),
                        Value::Float(f) => f.to_string(),
                        Value::Boolean(b) => b.to_string(),
                        _ => "null".to_string(),
                    })
                    .collect();
                MySqlValue::Bytes(format!("[{}]", json_arr.join(",")).into_bytes())
            }

            // Bits
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
                MySqlValue::Bytes(bytes)
            }

            // Interval - serialize as string
            Value::Interval(iv) => MySqlValue::Bytes(
                format!(
                    "{} months {} days {} us",
                    iv.months, iv.days, iv.microseconds
                )
                .into_bytes(),
            ),

            // Network types - serialize as string
            Value::IpAddr(addr) => MySqlValue::Bytes(addr.to_string().into_bytes()),
            Value::Cidr { addr, prefix } => {
                MySqlValue::Bytes(format!("{}/{}", addr, prefix).into_bytes())
            }
            Value::MacAddr(mac) => MySqlValue::Bytes(
                format!(
                    "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
                    mac[0], mac[1], mac[2], mac[3], mac[4], mac[5]
                )
                .into_bytes(),
            ),

            // Geometry - as binary (WKB)
            Value::Geometry(g) => MySqlValue::Bytes(g.clone()),

            // Composite - serialize as JSON
            Value::Composite { fields, .. } => {
                let json_fields: Vec<String> = fields
                    .iter()
                    .map(|(k, v)| {
                        let val_str = match v {
                            Value::String(s) => format!("\"{}\"", s),
                            Value::Int(i) => i.to_string(),
                            _ => "null".to_string(),
                        };
                        format!("\"{}\":{}", k, val_str)
                    })
                    .collect();
                MySqlValue::Bytes(format!("{{{}}}", json_fields.join(",")).into_bytes())
            }
        };
        MySqlParam(mysql_value)
    }
}

pub struct MySqlParamStore {
    pub params: Vec<MySqlParam>,
}

impl MySqlParamStore {
    pub fn from_values(values: &[Value]) -> Self {
        let params = values.iter().map(MySqlParam::from_value).collect();
        MySqlParamStore { params }
    }

    pub fn params(&self) -> Params {
        let mysql_values: Vec<MySqlValue> = self.params.iter().map(|p| p.0.clone()).collect();
        Params::Positional(mysql_values)
    }
}
