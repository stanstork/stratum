use crate::{connectors::sink::Sink, error::SinkError};
use async_trait::async_trait;
use connectors::sql::{
    base::{
        adapter::SqlAdapter,
        capabilities::DbCapabilities,
        error::DbError,
        metadata::{column::ColumnMetadata, table::TableMetadata},
        query::generator::QueryGenerator,
        transaction::Transaction,
    },
    postgres::{adapter::PgAdapter, data_type::PgDataType},
};
use model::{
    core::{data_type::DataType, value::Value},
    records::batch::Batch,
};
use planner::query::dialect;
use tokio::sync::OnceCell;
use tracing::debug;
use uuid::Uuid;

pub struct PostgresSink {
    adapter: PgAdapter,
    dialect: dialect::Postgres,
    capabilities: OnceCell<DbCapabilities>,
}

impl PostgresSink {
    pub fn new(adapter: PgAdapter) -> Self {
        Self {
            adapter,
            dialect: dialect::Postgres,
            capabilities: OnceCell::new(),
        }
    }

    fn ordered_columns(&self, table: &TableMetadata) -> Vec<ColumnMetadata> {
        let mut columns = table.columns.values().cloned().collect::<Vec<_>>();
        columns.sort_by_key(|col| col.ordinal);
        columns
    }

    async fn create_staging_table(
        &self,
        tx: &Transaction<'_>,
        meta: &TableMetadata,
        name: &str,
    ) -> Result<(), SinkError> {
        let generator = QueryGenerator::new(&self.dialect);
        let column_defs = meta.column_defs(&|col| DataType::as_pg_type_info(col));
        let (sql, params) = generator.create_table(name, &column_defs, false, true);

        debug!("Creating staging table with SQL: {}", sql);
        self.exec(tx, &sql, params).await
    }

    async fn drop_staging_table(&self, tx: &Transaction<'_>, name: &str) -> Result<(), SinkError> {
        let generator = QueryGenerator::new(&self.dialect);
        let (sql, params) = generator.drop_table(name, true);

        debug!("Dropping staging table with SQL: {}", sql);
        self.exec(tx, &sql, params).await
    }

    async fn merge_staging(
        &self,
        tx: &Transaction<'_>,
        meta: &TableMetadata,
        staging_table: &str,
        columns: &[ColumnMetadata],
    ) -> Result<(), SinkError> {
        if meta.primary_keys.is_empty() {
            return Err(SinkError::FastPathNotSupported(
                "Table has no primary keys".to_string(),
            ));
        }

        let has_merge = self.cached_capabilities().await?.merge_statements;
        let generator = QueryGenerator::new(&self.dialect);

        let (sql, params) = if has_merge {
            generator.merge_from_staging(meta, staging_table, columns)
        } else {
            generator.upsert_from_staging(meta, staging_table, columns)
        };

        debug!("Merging staging table with SQL: {}", sql);

        self.exec(tx, &sql, params).await
    }

    async fn exec(
        &self,
        tx: &Transaction<'_>,
        sql: &str,
        params: Vec<Value>,
    ) -> Result<(), SinkError> {
        if params.is_empty() {
            self.adapter.exec_tx(tx, sql).await?;
        } else {
            self.adapter.exec_params_tx(tx, sql, params).await?;
        }

        Ok(())
    }

    async fn cached_capabilities(&self) -> Result<DbCapabilities, DbError> {
        let capabilities = self
            .capabilities
            .get_or_try_init(|| async { self.adapter.capabilities().await })
            .await?;

        Ok(*capabilities)
    }
}

#[async_trait]
impl Sink for PostgresSink {
    async fn support_fast_path(&self) -> Result<bool, SinkError> {
        let capabilities = self
            .cached_capabilities()
            .await
            .map_err(|_| SinkError::Capabilities)?;

        // Fast path is allowed as long as we can stream COPY.
        // merge_statements decides between MERGE or ON CONFLICT.
        Ok(capabilities.copy_streaming)
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

        debug!("Staging table: {}", staging_table);

        let mut client = self.adapter.lock_client().await;
        let tx = Transaction::PgTransaction(client.transaction().await?);

        self.create_staging_table(&tx, table, &staging_table)
            .await?;

        let copy_result = self
            .adapter
            .copy_rows(&tx, &staging_table, &ordered_cols, &batch.rows)
            .await;

        if let Err(err) = copy_result {
            let _ = self.drop_staging_table(&tx, &staging_table).await;
            return Err(err.into());
        }

        let merge_result = self
            .merge_staging(&tx, table, &staging_table, &ordered_cols)
            .await;
        let drop_result = self.drop_staging_table(&tx, &staging_table).await;

        merge_result?;
        drop_result?;

        tx.commit().await?;

        Ok(())
    }
}
