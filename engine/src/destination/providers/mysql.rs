use crate::{destination::data_dest::DbDataDestination, record::DataRecord};
use async_trait::async_trait;
use sql_adapter::{metadata::table::TableMetadata, mysql::MySqlAdapter};

pub struct MySqlDestination {
    manager: MySqlAdapter,
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
}
