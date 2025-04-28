use crate::{
    metadata::{provider::MetadataHelper, table::TableMetadata},
    query::column::ColumnDef,
    row::row_data::RowData,
    schema::plan::SchemaPlan,
};
use async_trait::async_trait;

#[async_trait]
pub trait DbDataDestination: MetadataHelper + Send + Sync {
    async fn write_batch(
        &self,
        meta: &TableMetadata,
        rows: Vec<RowData>,
    ) -> Result<(), Box<dyn std::error::Error>>;
    async fn infer_schema(
        &self,
        schema_plan: &SchemaPlan<'_>,
    ) -> Result<(), Box<dyn std::error::Error>>;
    async fn toggle_trigger(
        &self,
        table: &str,
        enable: bool,
    ) -> Result<(), Box<dyn std::error::Error>>;
    async fn table_exists(&self, table: &str) -> Result<bool, Box<dyn std::error::Error>>;
    async fn add_column(
        &self,
        table: &str,
        column: &ColumnDef,
    ) -> Result<(), Box<dyn std::error::Error>>;
}
