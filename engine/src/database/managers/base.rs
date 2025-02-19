use async_trait::async_trait;
use sqlx::Database;

#[async_trait]
pub trait DbManager {
    type DB: Database;

    async fn connect(url: &str) -> Result<Self, sqlx::Error>
    where
        Self: Sized;

    fn pool(&self) -> &sqlx::Pool<Self::DB>;

    async fn table_exists(&self, table: &str) -> Result<bool, sqlx::Error>;
    async fn truncate_table(&self, table: &str) -> Result<(), sqlx::Error>;
}
