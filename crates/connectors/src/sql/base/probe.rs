use crate::sql::base::{adapter::SqlAdapter, capabilities::DbCapabilities, error::DbError};
use async_trait::async_trait;

#[async_trait]
pub trait CapabilityProbe {
    async fn detect(adapter: &(dyn SqlAdapter + Send + Sync)) -> Result<DbCapabilities, DbError>;
}
