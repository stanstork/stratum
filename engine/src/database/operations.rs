use async_trait::async_trait;
use sqlx::{Pool, Postgres, Row};

#[async_trait]
pub trait DbOperations {
    async fn table_exists(&self, table: &str) -> Result<bool, sqlx::Error>;
    async fn truncate_table(&self, table: &str) -> Result<(), sqlx::Error>;
}

#[async_trait]
impl DbOperations for Pool<Postgres> {
    async fn table_exists(&self, table: &str) -> Result<bool, sqlx::Error> {
        let query = "SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE  table_schema = 'public'
            AND    table_name   = $1
        )";

        let row = sqlx::query(query).bind(table).fetch_one(self).await?;
        let exists: bool = row.get(0);
        Ok(exists)
    }

    async fn truncate_table(&self, table: &str) -> Result<(), sqlx::Error> {
        let query = format!("TRUNCATE TABLE {}", table);
        sqlx::query(&query).execute(self).await?;
        Ok(())
    }
}
