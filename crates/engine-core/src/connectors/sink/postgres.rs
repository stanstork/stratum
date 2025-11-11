use crate::{connectors::sink::Sink, error::SinkError};
use async_trait::async_trait;
use connectors::sql::{
    base::{adapter::SqlAdapter, metadata::table::TableMetadata},
    postgres::adapter::PgAdapter,
};
use model::records::batch::Batch;
use planner::query::dialect;
use uuid::Uuid;

pub struct PostgresSink {
    adapter: PgAdapter,
    dialect: dialect::Postgres,
}

impl PostgresSink {
    pub fn new(adapter: PgAdapter) -> Self {
        Self {
            adapter,
            dialect: dialect::Postgres,
        }
    }

    fn ordered_columns(&self, table: &TableMetadata) -> Vec<String> {
        let mut columns = table.columns.keys().cloned().collect::<Vec<String>>();
        columns.sort_by_key(|col| table.columns[col].ordinal);
        columns
    }
}

#[async_trait]
impl Sink for PostgresSink {
    async fn support_fast_path(&self) -> Result<bool, SinkError> {
        let capabilities = self
            .adapter
            .capabilities()
            .await
            .map_err(|_| SinkError::Capabilities)?;
        Ok(capabilities.copy_streaming && capabilities.merge_statements)
    }

    async fn write_fast_path(&self, table: &TableMetadata, batch: &Batch) -> Result<(), SinkError> {
        if batch.is_empty() {
            return Ok(());
        }

        if table.primary_keys.is_empty() {
            return Err(SinkError::FastPathNotSupported(
                "Table has no primary keys".to_string(),
            ));
        }

        let staging_table = format!("__stratum_stage_{}", Uuid::new_v4().simple());
        let ordered_cols = self.ordered_columns(table);

        println!("Staging table: {}", staging_table);
        println!("Ordered columns: {:?}", ordered_cols);

        todo!("Implement fast-path write for PostgresSink");
    }

    async fn write_batch(&self, table: &TableMetadata, batch: &Batch) -> Result<(), SinkError> {
        todo!("Implement standard write for PostgresSink");
    }
}
