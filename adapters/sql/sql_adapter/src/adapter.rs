use crate::{
    error::{adapter::ConnectorError, db::DbError},
    metadata::table::TableMetadata,
    requests::FetchRowsRequest,
};
use async_trait::async_trait;
use common::row_data::RowData;

#[async_trait]
pub trait SqlAdapter {
    async fn connect(url: &str) -> Result<Self, ConnectorError>
    where
        Self: Sized;

    async fn table_exists(&self, table: &str) -> Result<bool, DbError>;
    async fn truncate_table(&self, table: &str) -> Result<(), DbError>;
    async fn execute(&self, query: &str) -> Result<(), DbError>;

    async fn fetch_metadata(&self, table: &str) -> Result<TableMetadata, DbError>;
    async fn fetch_referencing_tables(&self, table: &str) -> Result<Vec<String>, DbError>;
    async fn fetch_rows(&self, request: FetchRowsRequest) -> Result<Vec<RowData>, DbError>;
    async fn fetch_column_type(&self, table: &str, column: &str) -> Result<String, DbError>;
}
