use super::Destination;
use crate::{
    config::mapping::TableMapping,
    database::{
        connection::{DbConnection, MySqlConnection},
        row::RowData,
    },
};
use async_trait::async_trait;

pub struct MySqlDestination {
    conn: MySqlConnection,
}

impl MySqlDestination {
    pub async fn new(url: &str, mapping: TableMapping) -> Result<Self, sqlx::Error> {
        let conn = DbConnection::connect(url).await?;
        Ok(Self { conn })
    }
}

#[async_trait]
impl Destination for MySqlDestination {
    async fn write(&self, data: Vec<RowData>) -> Result<(), sqlx::Error> {
        Ok(())
    }
}
