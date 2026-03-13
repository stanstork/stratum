use crate::{
    error::DriverError,
    sql::metadata::{
        constraint::{CheckConstraintMetadata, UniqueConstraintMetadata},
        fk::ForeignKeyMetadata,
        index::IndexMetadata,
        table::TableMetadata,
    },
    traits::driver::Driver,
};
use async_trait::async_trait;

#[async_trait]
pub trait SchemaIntrospector: Driver {
    async fn table_exists(&self, table: &str) -> Result<bool, DriverError>;
    async fn list_tables(&self, schema: Option<&str>) -> Result<Vec<String>, DriverError>;
    async fn table_metadata(&self, table: &str) -> Result<TableMetadata, DriverError>;
    async fn index_metadata(&self, table: &str) -> Result<Vec<IndexMetadata>, DriverError>;
    async fn fk_metadata(&self, table: &str) -> Result<Vec<ForeignKeyMetadata>, DriverError>;
    async fn referencing_tables(&self, table: &str) -> Result<Vec<String>, DriverError>;
    async fn table_size_bytes(&self, table: &str) -> Result<u64, DriverError>;

    async fn unique_constraint_metadata(
        &self,
        _table: &str,
    ) -> Result<Vec<UniqueConstraintMetadata>, DriverError> {
        Ok(vec![])
    }

    async fn check_constraint_metadata(
        &self,
        _table: &str,
    ) -> Result<Vec<CheckConstraintMetadata>, DriverError> {
        Ok(vec![])
    }
}
