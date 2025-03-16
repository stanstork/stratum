use crate::{
    adapter::DbAdapter,
    db_type::DbType,
    metadata::{provider::MetadataProvider, table::TableMetadata},
    query::loader::QueryLoader,
    requests::FetchRowsRequest,
    row::row::{DbRow, RowData},
};
use async_trait::async_trait;
use sqlx::{Pool, Postgres, Row};

#[derive(Clone)]
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
        let query = QueryLoader::table_metadata_query(DbType::Postgres)
            .map_err(|_| sqlx::Error::Configuration("Table metadata query not found".into()))?
            .replace("{table}", table);

        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;
        let rows = rows.iter().map(|row| DbRow::PostgresRow(row)).collect();

        MetadataProvider::process_metadata_rows(table, &rows)
    }

    async fn fetch_rows(
        &self,
        request: FetchRowsRequest,
    ) -> Result<Vec<RowData>, Box<dyn std::error::Error>> {
        todo!("Implement fetch_all for Postgres")
    }
}
