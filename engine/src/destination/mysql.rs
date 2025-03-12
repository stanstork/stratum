use super::data_dest::DataDestination;
use crate::source::record::DataRecord;
use async_trait::async_trait;
use sql_adapter::mysql::MySqlAdapter;

pub struct MySqlDestination {
    manager: MySqlAdapter,
}

#[async_trait]
impl DataDestination for MySqlDestination {
    type Record = Box<dyn DataRecord>;

    async fn write(
        &self,
        data: Vec<Box<dyn DataRecord>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}
