use crate::io::{error::SinkError, sink::Sink};
use async_trait::async_trait;
use connectors::{
    drivers::postgres::{driver::PgDriver, types::PgTypeConverter},
    error::DriverError,
    sql::{
        metadata::{column::ColumnMetadata, table::TableMetadata},
        query::generator::QueryGenerator,
    },
    traits::{
        driver::Driver, executor::QueryExecutor, transaction::Transactional, writer::DataWriter,
    },
};
use engine_core::schema::type_registry::{Dialect, TypeRegistry};
use model::{core::convert::IntoCanonical, records::Record};
use query_builder::dialect::Postgres as PgDialect;
use std::sync::Arc;
use tracing::{debug, info};
use uuid::Uuid;

pub struct PostgresSink {
    driver: Arc<PgDriver>,
    type_registry: TypeRegistry,
}

impl PostgresSink {
    pub fn new(driver: Arc<PgDriver>, source_dialect: Dialect) -> Self {
        Self {
            driver,
            type_registry: TypeRegistry::new(source_dialect, Dialect::Postgres),
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
        let generator = QueryGenerator::new(&PgDialect);
        let type_converter = PgTypeConverter;

        // Exclude generated columns - staging tables only hold raw data; generated columns
        // are computed automatically by PostgreSQL when we merge into the real target table.
        let column_defs: Vec<_> = meta
            .column_defs(&|col| {
                let source_type = type_converter.to_canonical(col);
                let target_type = self
                    .type_registry
                    .convert(&source_type.canonical)
                    .target_type();
                (target_type, col.char_max_length)
            })
            .into_iter()
            .filter(|c| !c.is_generated)
            .collect();
        let (sql, _) = generator.create_table(name, &column_defs, false, true);

        info!("Creating staging table with SQL: {}", sql);
        self.driver.execute(&sql).await?;
        Ok(())
    }

    async fn drop_staging_table(&self, name: &str) -> Result<(), SinkError> {
        let generator = QueryGenerator::new(&PgDialect);
        let (sql, _) = generator.drop_table(name, true);

        info!("Dropping staging table with SQL: {}", sql);
        self.driver.execute(&sql).await?;
        Ok(())
    }

    async fn merge_staging(
        &self,
        meta: &TableMetadata,
        staging_table: &str,
        columns: &[ColumnMetadata],
    ) -> Result<(), SinkError> {
        if meta.primary_keys.is_empty() {
            return Err(SinkError::FastPathNotSupported(
                "Table has no primary keys".to_string(),
            ));
        }

        let capabilities = self.driver.capabilities();
        let generator = QueryGenerator::new(&PgDialect);

        // PostgreSQL 15+ supports MERGE, earlier versions use ON CONFLICT
        // The upsert path works for both
        let (sql, params) = if capabilities.upsert {
            generator.upsert_from_staging(meta, staging_table, columns)
        } else {
            generator.merge_from_staging(meta, staging_table, columns)
        };

        debug!("Merging staging table with SQL: {}", sql);
        self.driver.execute_params(&sql, &params).await?;
        Ok(())
    }
}

#[async_trait]
impl Sink for PostgresSink {
    async fn write_batch(&self, meta: &TableMetadata, rows: &[Record]) -> Result<u64, DriverError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let ordered_cols = self.ordered_columns(meta);

        // Use copy_rows for efficient bulk insert
        let count = self
            .driver
            .copy_rows(&meta.name, &ordered_cols, rows)
            .await?;
        Ok(count)
    }

    async fn support_fast_path(&self) -> Result<bool, SinkError> {
        let capabilities = self.driver.capabilities();
        // Fast path requires COPY protocol support
        Ok(capabilities.copy_protocol)
    }

    async fn write_fast_path(
        &self,
        table: &TableMetadata,
        rows: &[Record],
    ) -> Result<(), SinkError> {
        if rows.is_empty() {
            return Ok(());
        }

        if table.primary_keys.is_empty() {
            return Err(SinkError::FastPathNotSupported(
                "Table has no primary keys".to_string(),
            ));
        }

        let staging_table = format!("__stratum_stage_{}", Uuid::new_v4().simple());

        // Exclude generated columns - they're computed by the DB and cannot be inserted directly.
        let ordered_cols: Vec<_> = self
            .ordered_columns(table)
            .into_iter()
            .filter(|c| !c.is_generated)
            .collect();

        info!("Staging table: {}", staging_table);

        // Begin transaction
        let tx = self.driver.begin().await?;

        // Create staging table
        if let Err(e) = self.create_staging_table(table, &staging_table).await {
            let _ = tx.rollback().await;
            return Err(e);
        }

        // Copy rows into staging table using DataWriter
        let copy_result = self
            .driver
            .copy_rows(&staging_table, &ordered_cols, rows)
            .await;

        if let Err(err) = copy_result {
            let _ = self.drop_staging_table(&staging_table).await;
            let _ = tx.rollback().await;
            return Err(err.into());
        }

        // Merge from staging into target
        let merge_result = self
            .merge_staging(table, &staging_table, &ordered_cols)
            .await;

        // Drop staging table
        let drop_result = self.drop_staging_table(&staging_table).await;

        // Check results before committing
        if let Err(e) = merge_result {
            let _ = tx.rollback().await;
            return Err(e);
        }

        if let Err(e) = drop_result {
            let _ = tx.rollback().await;
            return Err(e);
        }

        tx.commit().await?;
        Ok(())
    }
}
