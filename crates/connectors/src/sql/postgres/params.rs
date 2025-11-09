use model::core::value::Value;
use tokio_postgres::types::Json as PgJson;
use tokio_postgres::types::ToSql;

pub struct PgParam(Box<dyn ToSql + Sync + Send>);

impl PgParam {
    pub fn from_value(value: Value) -> Self {
        match value {
            Value::Int(v) => PgParam(Box::new(v)),
            Value::Uint(v) => PgParam(Box::new(v as i64)),
            Value::Usize(v) => PgParam(Box::new(v as i64)),
            Value::Float(v) => PgParam(Box::new(v)),
            Value::String(v) => PgParam(Box::new(v)),
            Value::Boolean(v) => PgParam(Box::new(v)),
            Value::Json(v) => PgParam(Box::new(PgJson(v))),
            Value::Uuid(v) => PgParam(Box::new(v)),
            Value::Bytes(v) => PgParam(Box::new(v)),
            Value::Date(v) => PgParam(Box::new(v)),
            Value::Timestamp(v) => PgParam(Box::new(v)),
            Value::Enum(_, v) => PgParam(Box::new(v)),
            Value::StringArray(v) => PgParam(Box::new(v)),
            Value::Null => PgParam(Box::new(Option::<std::string::String>::None)),
        }
    }

    pub fn as_ref(&self) -> &(dyn ToSql + Sync) {
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
