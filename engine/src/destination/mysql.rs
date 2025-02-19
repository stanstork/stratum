use super::Destination;
use crate::{
    config::mapping::TableMapping,
    database::{
        managers::{base::DbManager, mysql::MySqlManager},
        row::RowData,
    },
};
use async_trait::async_trait;

pub struct MySqlDestination {
    manager: MySqlManager,
}

impl MySqlDestination {
    pub async fn new(url: &str, mapping: TableMapping) -> Result<Self, sqlx::Error> {
        let manager = MySqlManager::connect(url).await?;

        Ok(Self { manager })
    }
}

#[async_trait]
impl Destination for MySqlDestination {
    async fn write(&self, data: Vec<RowData>) -> Result<(), sqlx::Error> {
        Ok(())
    }
}
