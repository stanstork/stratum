use crate::sql::base::{
    capabilities::DbCapabilities,
    error::{ConnectorError, DbError},
    metadata::table::TableMetadata,
    requests::FetchRowsRequest,
};
use async_trait::async_trait;
use model::{core::value::Value, records::row::RowData};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatabaseKind {
    MySql,
    Postgres,
    Other(String),
}

#[async_trait]
pub trait SqlAdapter {
    async fn connect(url: &str) -> Result<Self, ConnectorError>
    where
        Self: Sized;

    // Exec / Params
    async fn exec(&self, query: &str) -> Result<(), DbError>;
    async fn exec_params(&self, query: &str, params: Vec<Value>) -> Result<(), DbError>;

    async fn query_rows(&self, sql: &str) -> Result<Vec<RowData>, DbError>;

    async fn fetch_rows(&self, request: FetchRowsRequest) -> Result<Vec<RowData>, DbError>;
    async fn fetch_existing_keys(
        &self,
        table: &str,
        key_columns: &[String],
        keys_batch: &[Vec<Value>],
    ) -> Result<Vec<RowData>, DbError>;

    // Introspection
    async fn table_exists(&self, table: &str) -> Result<bool, DbError>;
    async fn list_tables(&self) -> Result<Vec<String>, DbError>;
    async fn table_metadata(&self, table: &str) -> Result<TableMetadata, DbError>;
    async fn referencing_tables(&self, table: &str) -> Result<Vec<String>, DbError>;
    async fn column_db_type(&self, table: &str, column: &str) -> Result<String, DbError>;
    async fn truncate_table(&self, table: &str) -> Result<(), DbError>;

    // Dialect & capabilities
    fn kind(&self) -> DatabaseKind;
    async fn capabilities(&self) -> Result<DbCapabilities, DbError>;
}
