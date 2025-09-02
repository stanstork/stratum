use common::value::Value;
use sqlx::{query::Query, MySql};

pub mod adapter;
pub mod data_type;
pub mod metadata;
pub mod source;

pub fn bind_values<'q>(
    mut query: Query<'q, MySql, sqlx::mysql::MySqlArguments>,
    params: &'q [Value],
) -> Query<'q, MySql, sqlx::mysql::MySqlArguments> {
    for p in params {
        query = match p {
            Value::Int(i) => query.bind(*i),
            Value::Float(f) => query.bind(*f),
            Value::String(s) => query.bind(s),
            Value::Boolean(b) => query.bind(*b),
            Value::Json(j) => query.bind(j),
            Value::Uuid(u) => query.bind(*u),
            Value::Bytes(b) => query.bind(b),
            Value::Date(d) => query.bind(*d),
            Value::Timestamp(t) => query.bind(*t),
            Value::Null => query.bind(None::<i32>), // Binding NULL as an integer; adjust type as needed
            Value::Enum(_, v) => query.bind(v),
            Value::StringArray(v) => query.bind(format!("{:?}", v)), // Bind as a string representation of the array
        };
    }
    query
}
