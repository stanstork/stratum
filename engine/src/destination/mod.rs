use crate::database::row::RowData;
use async_trait::async_trait;
pub mod mysql;
pub mod postgres;

#[async_trait]
pub trait Destination {
    async fn write(&self, data: Vec<RowData>) -> Result<(), Box<dyn std::error::Error>>;
}
