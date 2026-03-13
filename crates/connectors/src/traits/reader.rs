use crate::{
    error::DriverError,
    sql::{filter::SqlFilter, request::FetchRowsRequest},
    traits::driver::Driver,
};
use async_trait::async_trait;
use model::records::Record;

#[async_trait]
pub trait DataReader: Driver {
    async fn fetch(&self, request: FetchRowsRequest) -> Result<Vec<Record>, DriverError>;
    async fn count(
        &self,
        table: &str,
        schema: Option<&str>,
        filter: Option<&SqlFilter>,
    ) -> Result<u64, DriverError>;
    async fn count_fast(&self, table: &str) -> Result<u64, DriverError>;
}
