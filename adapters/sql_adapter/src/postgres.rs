use crate::{
    adapter::DbAdapter, metadata::table::TableMetadata, requests::FetchRowsRequest,
    row::row::RowData,
};
use async_trait::async_trait;
use sqlx::{Pool, Postgres, Row};

pub struct PgAdapter {
    pool: Pool<Postgres>,
}

#[async_trait]
impl DbAdapter for PgAdapter {
    async fn connect(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let pool = Pool::connect(url).await?;
        Ok(PgAdapter { pool })
    }

    async fn table_exists(&self, table: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let query = "SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE  table_schema = 'public'
            AND    table_name   = $1
        )";

        let row = sqlx::query(query).bind(table).fetch_one(&self.pool).await?;
        let exists: bool = row.get(0);
        Ok(exists)
    }

    async fn execute(&self, query: &str) -> Result<(), Box<dyn std::error::Error>> {
        sqlx::query(query).execute(&self.pool).await?;
        Ok(())
    }

    async fn truncate_table(&self, table: &str) -> Result<(), Box<dyn std::error::Error>> {
        let query = format!("TRUNCATE TABLE {}", table);
        sqlx::query(&query).execute(&self.pool).await?;
        Ok(())
    }

    async fn fetch_metadata(
        &self,
        table: &str,
    ) -> Result<TableMetadata, Box<dyn std::error::Error>> {
        todo!("Implement fetch_metadata for Postgres")
    }

    async fn fetch_rows(
        &self,
        request: FetchRowsRequest,
    ) -> Result<Vec<RowData>, Box<dyn std::error::Error>> {
        todo!("Implement fetch_all for Postgres")
    }
}
