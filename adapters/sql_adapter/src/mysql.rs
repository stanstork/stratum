use crate::{
    adapter::DbAdapter,
    db_type::DbType,
    metadata::{provider::MetadataProvider, table::TableMetadata},
    query::{builder::SqlQueryBuilder, loader::QueryLoader},
    requests::FetchRowsRequest,
    row::{
        extract::RowExtractor,
        row::{DbRow, RowData},
    },
};
use async_trait::async_trait;
use sqlx::{MySql, Pool, Row};

#[derive(Clone)]
pub struct MySqlAdapter {
    pool: Pool<MySql>,
}

#[async_trait]
impl DbAdapter for MySqlAdapter {
    async fn connect(url: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let pool = Pool::connect(url).await?;
        Ok(MySqlAdapter { pool })
    }

    async fn table_exists(&self, table: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let query = QueryLoader::table_exists_query(DbType::MySql)?;
        let row = sqlx::query(&query)
            .bind(table)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get(0))
    }

    async fn truncate_table(&self, table: &str) -> Result<(), Box<dyn std::error::Error>> {
        let query = QueryLoader::truncate_table_query(DbType::MySql)?;
        sqlx::query(&query).bind(table).execute(&self.pool).await?;
        Ok(())
    }

    async fn execute(&self, query: &str) -> Result<(), Box<dyn std::error::Error>> {
        sqlx::query(query).execute(&self.pool).await?;
        Ok(())
    }

    async fn fetch_metadata(
        &self,
        table: &str,
    ) -> Result<TableMetadata, Box<dyn std::error::Error>> {
        let query = QueryLoader::table_metadata_query(DbType::MySql)?;
        let rows = sqlx::query(&query)
            .bind(table)
            .bind(table)
            .bind(table)
            .bind(table)
            .fetch_all(&self.pool)
            .await?;
        let rows = rows.iter().map(|row| DbRow::MySqlRow(row)).collect();

        MetadataProvider::process_metadata_rows(table, &rows)
    }

    async fn fetch_rows(
        &self,
        request: FetchRowsRequest,
    ) -> Result<Vec<RowData>, Box<dyn std::error::Error>> {
        let query = SqlQueryBuilder::new()
            .select(&request.columns, request.table.as_str())
            .from(&request.table)
            .limit(request.limit)
            .offset(request.offset.unwrap_or(0))
            .build();
        let rows = sqlx::query(&query.0).fetch_all(&self.pool).await?;

        Ok(rows
            .iter()
            .map(|row| RowExtractor::from_row(&DbRow::MySqlRow(row)))
            .collect())
    }
}
