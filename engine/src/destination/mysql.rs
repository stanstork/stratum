use crate::source::record::DataRecord;
use async_trait::async_trait;
use sql_adapter::{metadata::table::TableMetadata, mysql::MySqlAdapter};

use super::data_dest::DbDataDestination;

pub struct MySqlDestination {
    manager: MySqlAdapter,
}

#[async_trait]
impl DbDataDestination for MySqlDestination {
    type Record = Box<dyn DataRecord>;

    async fn write(
        &self,
        data: Vec<Box<dyn DataRecord>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    async fn infer_schema(
        &self,
        metadata: &TableMetadata,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}
