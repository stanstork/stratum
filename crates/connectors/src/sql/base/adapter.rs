use crate::sql::base::{
    capabilities::DbCapabilities,
    error::{ConnectorError, DbError},
    metadata::{column::ColumnMetadata, table::TableMetadata},
    requests::FetchRowsRequest,
    transaction::Transaction,
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

    // Exec
    async fn exec(&self, query: &str) -> Result<(), DbError>;
    async fn exec_params(&self, query: &str, params: Vec<Value>) -> Result<(), DbError>;

    // Exec transaction
    async fn exec_tx(&self, _tx: &Transaction<'_>, _query: &str) -> Result<(), DbError> {
        Err(DbError::Unknown(
            "Exec within transaction not implemented for this adapter".to_string(),
        ))
    }

    async fn exec_params_tx(
        &self,
        _tx: &Transaction<'_>,
        _query: &str,
        _params: Vec<Value>,
    ) -> Result<(), DbError> {
        Err(DbError::Unknown(
            "Exec within transaction not implemented for this adapter".to_string(),
        ))
    }

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

    async fn copy_rows(
        &self,
        _tx: &Transaction<'_>,
        _table: &str,
        _columns: &[ColumnMetadata],
        _rows: &[RowData],
    ) -> Result<(), DbError> {
        Err(DbError::Unknown(
            "Streaming COPY not implemented for this adapter".to_string(),
        ))
    }
}
