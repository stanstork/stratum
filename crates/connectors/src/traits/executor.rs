use crate::{error::DriverError, traits::driver::Driver};
use async_trait::async_trait;
use model::{core::value::Value, records::Record};

#[async_trait]
pub trait QueryExecutor: Driver {
    async fn execute(&self, sql: &str) -> Result<(), DriverError>;
    async fn execute_params(&self, sql: &str, params: &[Value]) -> Result<(), DriverError>;
    async fn query(&self, sql: &str) -> Result<Vec<Record>, DriverError>;
    async fn query_params(&self, sql: &str, params: &[Value]) -> Result<Vec<Record>, DriverError>;
}
