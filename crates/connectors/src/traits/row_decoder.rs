use model::{core::value::Value, records::Record};

pub trait RowDecoder: Send + Sync {
    fn decode(&self, table: &str) -> Record;
    fn columns(&self) -> Vec<String>;

    fn get_string(&self, column: &str) -> Option<String>;
    fn get_i32(&self, column: &str) -> Option<i32>;
    fn get_u32(&self, column: &str) -> Option<u32>;
    fn get_i64(&self, column: &str) -> Option<i64>;
    fn get_bool(&self, column: &str) -> Option<bool>;
    fn get_value(&self, column: &str) -> Option<Value>;
}
