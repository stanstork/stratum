use super::data_dest::DataDestination;
use crate::{config::mapping::TableMapping, source::record::DataRecord};
use async_trait::async_trait;
use sql_adapter::{db_manager::DbManager, mysql::MySqlManager};

pub struct MySqlDestination {
    manager: MySqlManager,
}

impl MySqlDestination {
    pub async fn new(url: &str, mapping: TableMapping) -> Result<Self, Box<dyn std::error::Error>> {
        let manager = MySqlManager::connect(url).await?;
        Ok(Self { manager })
    }
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
