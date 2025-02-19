use super::base::DbManager;
use async_trait::async_trait;
use sqlx::{Pool, Postgres, Row};

pub struct PgManager {
    pool: Pool<Postgres>,
}

#[async_trait]
impl DbManager for PgManager {
    type DB = Postgres;

    async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        let pool = Pool::connect(url).await?;
        Ok(PgManager { pool })
    }

    fn pool(&self) -> &Pool<Self::DB> {
        &self.pool
    }

    async fn table_exists(&self, table: &str) -> Result<bool, sqlx::Error> {
        let query = "SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE  table_schema = 'public'
            AND    table_name   = $1
        )";

        let row = sqlx::query(query)
            .bind(table)
            .fetch_one(self.pool())
            .await?;
        let exists: bool = row.get(0);
        Ok(exists)
    }

    async fn truncate_table(&self, table: &str) -> Result<(), sqlx::Error> {
        let query = format!("TRUNCATE TABLE {}", table);
        sqlx::query(&query).execute(self.pool()).await?;
        Ok(())
    }
}
