use super::Destination;
use crate::config::mapping::TableMapping;
use async_trait::async_trait;
use sql_adapter::{db_manager::DbManager, mysql::MySqlManager, row::RowData};

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
impl Destination for MySqlDestination {
    async fn write(&self, data: Vec<RowData>) -> Result<(), Box<dyn std::error::Error>> {
        Ok(())
    }
}
