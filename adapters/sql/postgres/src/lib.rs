use common::value::Value;
use sqlx::{query::Query, Postgres};

pub mod adapter;
pub mod data_type;
pub mod destination;
pub mod metadata;

pub fn bind_values<'q>(
    mut query: Query<'q, Postgres, sqlx::postgres::PgArguments>,
    params: &'q [Value],
) -> Query<'q, Postgres, sqlx::postgres::PgArguments> {
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
        };
    }
    query
}
