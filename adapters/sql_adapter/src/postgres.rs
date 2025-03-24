use crate::{
    adapter::DbAdapter,
    db_type::DbType,
    metadata::{
        column::metadata::COL_REFERENCING_TABLE, provider::MetadataProvider, table::TableMetadata,
    },
    query::loader::QueryLoader,
    requests::FetchRowsRequest,
    row::{db_row::DbRow, row_data::RowData},
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
        let query = QueryLoader::table_exists_query(DbType::Postgres)?;
        let row = sqlx::query(&query)
            .bind(table)
            .fetch_one(&self.pool)
            .await?;
        Ok(row.get(0))
    }

    async fn truncate_table(&self, table: &str) -> Result<(), Box<dyn std::error::Error>> {
        let query = QueryLoader::truncate_table_query(DbType::Postgres)?;
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
        let query = QueryLoader::table_metadata_query(DbType::Postgres)?.replace("{table}", table);
        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;
        let rows = rows.iter().map(DbRow::PostgresRow).collect();

        MetadataProvider::process_metadata_rows(table, &rows)
    }

    async fn fetch_referencing_tables(
        &self,
        table: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let query = QueryLoader::table_referencing_query(DbType::MySql)?;
        let rows = sqlx::query(&query)
            .bind(table)
            .fetch_all(&self.pool)
            .await?;

        let tables = rows
            .iter()
            .map(|row| row.try_get::<String, _>(COL_REFERENCING_TABLE))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(tables)
    }

    async fn fetch_rows(
        &self,
        _request: FetchRowsRequest,
    ) -> Result<Vec<RowData>, Box<dyn std::error::Error>> {
        todo!("Implement fetch_all for Postgres")
    }
}
