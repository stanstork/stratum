use crate::{
    db_type::DbType, metadata::table::TableMetadata, mysql, postgres, requests::FetchRowsRequest,
    row::row_data::RowData,
};
use async_trait::async_trait;

#[async_trait]
pub trait DbAdapter {
    async fn connect(url: &str) -> Result<Self, Box<dyn std::error::Error>>
    where
        Self: Sized;

    async fn table_exists(&self, table: &str) -> Result<bool, Box<dyn std::error::Error>>;
    async fn truncate_table(&self, table: &str) -> Result<(), Box<dyn std::error::Error>>;

    async fn execute(&self, query: &str) -> Result<(), Box<dyn std::error::Error>>;

    async fn fetch_metadata(
        &self,
        table: &str,
    ) -> Result<TableMetadata, Box<dyn std::error::Error>>;
    async fn fetch_referencing_tables(
        &self,
        table: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>>;

    async fn fetch_rows(
        &self,
        request: FetchRowsRequest,
    ) -> Result<Vec<RowData>, Box<dyn std::error::Error>>;
}

pub async fn get_db_adapter(
    db_type: DbType,
    conn_str: &str,
) -> Result<Box<dyn DbAdapter + Send + Sync>, Box<dyn std::error::Error>> {
    match db_type {
        DbType::Postgres => {
            let adapter = postgres::PgAdapter::connect(conn_str).await?;
            Ok(Box::new(adapter))
        }
        DbType::MySql => {
            let adapter = mysql::MySqlAdapter::connect(conn_str).await?;
            Ok(Box::new(adapter))
        }
        _ => Err("Unsupported database type".into()),
    }
}
