use async_trait::async_trait;
use sqlx::{query, MySql, Pool, Postgres};

#[async_trait]
pub trait DbConnection {
    async fn connect(url: &str) -> Result<Self, sqlx::Error>
    where
        Self: Sized;

    async fn is_connected(&self) -> bool;
}

#[async_trait]
impl DbConnection for Pool<MySql> {
    async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        Ok(Pool::connect(url).await?)
    }

    async fn is_connected(&self) -> bool {
        query("SELECT 1").fetch_one(self).await.is_ok()
    }
}

#[async_trait]
impl DbConnection for Pool<Postgres> {
    async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        Ok(Pool::connect(url).await?)
    }

    async fn is_connected(&self) -> bool {
        query("SELECT 1").fetch_one(self).await.is_ok()
    }
}
