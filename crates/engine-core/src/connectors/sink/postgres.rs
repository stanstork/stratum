use crate::{connectors::sink::Sink, error::SinkError};
use async_trait::async_trait;
use connectors::sql::{
    base::{
        adapter::SqlAdapter,
        metadata::{column::ColumnMetadata, table::TableMetadata},
        query::generator::QueryGenerator,
    },
    postgres::adapter::PgAdapter,
};
use model::{
    core::value::Value,
    records::{batch::Batch, row::RowData},
};
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

    fn ordered_columns(&self, table: &TableMetadata) -> Vec<ColumnMetadata> {
        let mut columns = table.columns.values().cloned().collect::<Vec<_>>();
        columns.sort_by_key(|col| col.ordinal);
        columns
    }

    async fn create_staging_table(
        &self,
        meta: &TableMetadata,
        name: &str,
    ) -> Result<(), SinkError> {
        let generator = QueryGenerator::new(&self.dialect);
        let column_defs = meta.column_defs(&|col| (col.data_type.clone(), col.char_max_length));
        let (sql, params) = generator.create_table(name, &column_defs, true);
        let temp_sql = sql.replacen("CREATE TABLE", "CREATE TEMP TABLE", 1);

        println!("Creating staging table with SQL: {}", temp_sql);

        self.exec(&temp_sql, params).await
    }

    async fn exec(&self, sql: &str, params: Vec<Value>) -> Result<(), SinkError> {
        if params.is_empty() {
            self.adapter.exec(sql).await?;
        } else {
            self.adapter.exec_params(sql, params).await?;
        }

        Ok(())
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

        self.create_staging_table(table, &staging_table).await?;

        let rows = batch
            .rows
            .values()
            .flatten()
            .filter_map(|r| r.to_row_data().cloned())
            .collect::<Vec<RowData>>();

        let copy_result = self
            .adapter
            .copy_rows(&staging_table, &ordered_cols, &rows)
            .await;

        todo!("Implement fast-path write for PostgresSink");
    }

    async fn write_batch(&self, table: &TableMetadata, batch: &Batch) -> Result<(), SinkError> {
        todo!("Implement standard write for PostgresSink");
    }
}
