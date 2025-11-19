use bigdecimal::ToPrimitive;
use model::core::value::Value;
use rust_decimal::{Decimal as RustDecimal, prelude::FromPrimitive as DecimalFromPrimitive};
use std::str::FromStr;
use tokio_postgres::types::Json as PgJson;
use tokio_postgres::types::ToSql;

pub struct PgParam(Box<dyn ToSql + Sync + Send>);

impl PgParam {
    pub fn from_value(value: Value) -> Self {
        match value {
            Value::SmallInt(v) => PgParam(Box::new(v)),
            Value::Int32(v) => PgParam(Box::new(v)),
            Value::Int(v) => PgParam(Box::new(v)),
            Value::Uint(v) => PgParam(Box::new(v as i64)),
            Value::Usize(v) => PgParam(Box::new(v as i64)),
            Value::Float(v) => PgParam(Box::new(v)),
            Value::Decimal(v) => {
                let decimal = RustDecimal::from_str(&v.to_string()).unwrap_or_else(|_| {
                    DecimalFromPrimitive::from_f64(v.to_f64().unwrap_or(0.0)).unwrap_or_default()
                });
                PgParam(Box::new(decimal))
            }
            Value::String(v) => PgParam(Box::new(v)),
            Value::Boolean(v) => PgParam(Box::new(v)),
            Value::Json(v) => PgParam(Box::new(PgJson(v))),
            Value::Uuid(v) => PgParam(Box::new(v)),
            Value::Bytes(v) => PgParam(Box::new(v)),
            Value::Date(v) => PgParam(Box::new(v)),
            Value::Timestamp(v) => PgParam(Box::new(v)),
            Value::TimestampNaive(v) => PgParam(Box::new(v)),
            Value::Enum(_, v) => PgParam(Box::new(v)),
            Value::StringArray(v) => PgParam(Box::new(v)),
            Value::Null => PgParam(Box::new(Option::<std::string::String>::None)),
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
    pub fn from_values(values: Vec<Value>) -> Self {
        Self {
            params: values.into_iter().map(PgParam::from_value).collect(),
        }
    }

    pub fn as_refs(&self) -> Vec<&(dyn ToSql + Sync)> {
        self.params
            .iter()
            .map(|param| param.as_ref())
            .collect::<Vec<_>>()
    }
}
