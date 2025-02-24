use crate::db_manager::DbManager;
use async_trait::async_trait;
use sqlx::{MySql, Pool, Row};

pub struct MySqlManager {
    pool: Pool<MySql>,
}

#[async_trait]
impl DbManager for MySqlManager {
    async fn connect(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let pool = Pool::connect(url).await?;
        Ok(MySqlManager { pool })
    }

    async fn table_exists(&self, table: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let query = "SELECT EXISTS (
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'test'
            AND table_name = $1
        )";

        let row = sqlx::query(query).bind(table).fetch_one(&self.pool).await?;
        let exists: bool = row.get(0);
        Ok(exists)
    }

    async fn truncate_table(&self, table: &str) -> Result<(), Box<dyn std::error::Error>> {
        let query = format!("TRUNCATE TABLE {}", table);
        sqlx::query(&query).execute(&self.pool).await?;
        Ok(())
    }
}
