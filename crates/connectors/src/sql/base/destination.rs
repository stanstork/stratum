use crate::sql::base::{
    capabilities::DbCapabilities,
    metadata::{provider::MetadataStore, table::TableMetadata},
    query::column::ColumnDef,
};
use async_trait::async_trait;
use model::records::row::RowData;

#[async_trait]
pub trait DbDataDestination: MetadataStore + Send + Sync {
    type Error;

    // Introspection / negotiation
    async fn capabilities(&self) -> DbCapabilities;

    async fn write_batch(
        &self,
        meta: &TableMetadata,
        rows: &Vec<RowData>,
    ) -> Result<(), Self::Error>;

    async fn toggle_trigger(&self, table: &str, enable: bool) -> Result<(), Self::Error>;
    async fn table_exists(&self, table: &str) -> Result<bool, Self::Error>;
    async fn add_column(&self, table: &str, column: &ColumnDef) -> Result<(), Self::Error>;
}
