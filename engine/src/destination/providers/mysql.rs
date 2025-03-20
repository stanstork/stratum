use crate::{destination::data_dest::DbDataDestination, record::DataRecord};
use async_trait::async_trait;
use sql_adapter::{adapter::DbAdapter, metadata::table::TableMetadata, mysql::MySqlAdapter};

pub struct MySqlDestination {
    adapter: MySqlAdapter,
}

#[async_trait]
impl DbDataDestination for MySqlDestination {
    type Record = Box<dyn DataRecord + Send + Sync>;

    async fn write(
        &self,
        data: Vec<Box<dyn DataRecord + Send + Sync>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn infer_schema(
        &self,
        metadata: &TableMetadata,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    async fn validate_schema(
        &self,
        metadata: &TableMetadata,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    fn adapter(&self) -> Box<dyn DbAdapter + Send + Sync> {
        Box::new(self.adapter.clone())
    }

    fn set_metadata(&mut self, _metadata: TableMetadata) {
        todo!("Implement set_metadata")
    }

    fn metadata(&self) -> &TableMetadata {
        todo!("Implement metadata")
    }
}
