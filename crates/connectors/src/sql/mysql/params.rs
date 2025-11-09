use chrono::Datelike;
use chrono::Timelike;
use model::core::value::Value;
use mysql_async::Value as MySqlValue;
use mysql_common::params::Params;

pub struct MySqlParam(MySqlValue);

impl MySqlParam {
    pub fn from_value(value: &Value) -> Self {
        match value {
            Value::Int(i) => MySqlParam(MySqlValue::Int(*i)),
            Value::Uint(u) => MySqlParam(MySqlValue::UInt(*u)),
            Value::Usize(u) => MySqlParam(MySqlValue::UInt(*u as u64)),
            Value::Float(f) => MySqlParam(MySqlValue::Double(*f)),
            Value::String(s) => MySqlParam(MySqlValue::Bytes(s.clone().into_bytes())),
            Value::Boolean(b) => MySqlParam(MySqlValue::Int(if *b { 1 } else { 0 })),
            Value::Json(j) => MySqlParam(MySqlValue::Bytes(j.to_string().into_bytes())),
            Value::Uuid(u) => MySqlParam(MySqlValue::Bytes(u.to_string().into_bytes())),
            Value::Bytes(b) => MySqlParam(MySqlValue::Bytes(b.clone())),
            Value::Date(d) => MySqlParam(MySqlValue::Date(
                d.year() as u16,
                d.month() as u8,
                d.day() as u8,
                0,
                0,
                0,
                0,
            )),
            Value::Timestamp(ts) => {
                let naive = ts.naive_utc();
                let utc = naive.and_utc();
                MySqlParam(MySqlValue::Date(
                    naive.year() as u16,
                    naive.month() as u8,
                    naive.day() as u8,
                    naive.hour() as u8,
                    naive.minute() as u8,
                    naive.second() as u8,
                    utc.timestamp_subsec_micros(),
                ))
            }
            Value::Null => MySqlParam(MySqlValue::NULL),
            Value::Enum(_, v) => MySqlParam(MySqlValue::Bytes(v.clone().into_bytes())),
            Value::StringArray(v) => MySqlParam(MySqlValue::Bytes(format!("{v:?}").into_bytes())),
        }
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
