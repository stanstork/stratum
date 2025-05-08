use crate::{
    metadata::{provider::MetadataHelper, table::TableMetadata},
    query::column::ColumnDef,
    row::row_data::RowData,
    schema::plan::SchemaPlan,
};
use async_trait::async_trait;

#[async_trait]
pub trait DbDataDestination: MetadataHelper + Send + Sync {
    type Error;

    async fn write_batch(
        &self,
        meta: &TableMetadata,
        rows: Vec<RowData>,
    ) -> Result<(), Self::Error>;

    async fn infer_schema(&self, schema_plan: &SchemaPlan<'_>) -> Result<(), Self::Error>;
    async fn toggle_trigger(&self, table: &str, enable: bool) -> Result<(), Self::Error>;
    async fn table_exists(&self, table: &str) -> Result<bool, Self::Error>;
    async fn add_column(&self, table: &str, column: &ColumnDef) -> Result<(), Self::Error>;
}
