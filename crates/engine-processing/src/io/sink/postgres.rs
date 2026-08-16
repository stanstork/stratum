use crate::io::{error::SinkError, sink::Sink};
use async_trait::async_trait;
use connectors::{
    drivers::postgres::{config::PgConflictAction, driver::PgDriver, types::PgTypeConverter},
    error::DriverError,
    sql::{
        metadata::{column::ColumnMetadata, table::TableMetadata},
        query::generator::QueryGenerator,
    },
    traits::{
        driver::Driver, executor::QueryExecutor, transaction::Transactional, writer::DataWriter,
    },
};
use engine_schema::type_registry::{Dialect, TypeRegistry};
use model::{core::convert::IntoCanonical, execution::pipeline::WriteMode, records::Record};
use query_builder::dialect::Postgres as PgDialect;
use std::sync::Arc;
use tracing::debug;
use uuid::Uuid;

pub struct PostgresSink {
    driver: Arc<PgDriver>,
    type_registry: TypeRegistry,
    on_conflict: Option<PgConflictAction>,
    use_conflict_resolution: bool,
}

impl PostgresSink {
    pub fn new(
        driver: Arc<PgDriver>,
        source_dialect: Dialect,
        write_mode: WriteMode,
        on_conflict: Option<PgConflictAction>,
    ) -> Self {
        Self {
            driver,
            type_registry: TypeRegistry::new(source_dialect, Dialect::Postgres),
            on_conflict,
            use_conflict_resolution: matches!(write_mode, WriteMode::Upsert | WriteMode::Update),
        }
    }

    /// The conflict action to apply for `table`, or `None` to COPY directly.
    ///
    /// Resolving a conflict needs a key to conflict *on*, so a table without a
    /// primary key always falls back to a direct `COPY` - the only thing
    /// possible - rather than failing the write.
    fn conflict_action_for(&self, table: &TableMetadata) -> Option<PgConflictAction> {
        if table.primary_keys.is_empty() {
            return None;
        }

        match self.on_conflict {
            // Explicit setting wins.
            Some(action) => Some(action),
            // Otherwise the write mode decides: upsert modes reconcile rows.
            None => self
                .use_conflict_resolution
                .then_some(PgConflictAction::DoUpdate),
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

        debug!(sql = %sql, "creating staging table");
        self.driver.execute(&sql).await?;
        Ok(())
    }

    async fn drop_staging_table(&self, name: &str) -> Result<(), SinkError> {
        let generator = QueryGenerator::new(&PgDialect);
        let (sql, _) = generator.drop_table(name, true);

        debug!(sql = %sql, "dropping staging table");
        self.driver.execute(&sql).await?;
        Ok(())
    }

    async fn merge_staging(
        &self,
        meta: &TableMetadata,
        staging_table: &str,
        columns: &[ColumnMetadata],
        action: PgConflictAction,
    ) -> Result<(), SinkError> {
        let capabilities = self.driver.capabilities();
        let generator = QueryGenerator::new(&PgDialect);
        let do_update = matches!(action, PgConflictAction::DoUpdate);

        // PostgreSQL 15+ supports MERGE, earlier versions use ON CONFLICT
        // The upsert path works for both
        let (sql, params) = if capabilities.upsert {
            generator.upsert_from_staging(meta, staging_table, columns, do_update)
        } else {
            generator.merge_from_staging(meta, staging_table, columns, do_update)
        };

        debug!(sql = %sql, "merging staging table");
        self.driver.execute_params(&sql, &params).await?;
        Ok(())
    }
}

#[async_trait]
impl Sink for PostgresSink {
    async fn write_batch(&self, meta: &TableMetadata, rows: &[Record]) -> Result<u64, DriverError> {
        self.driver.write_batch(meta, rows).await
    }

    async fn truncate(&self, table: &str) -> Result<(), DriverError> {
        self.driver.truncate(table).await
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

        // Exclude generated columns - they're computed by the DB and cannot be inserted directly.
        let ordered_cols: Vec<_> = self
            .ordered_columns(table)
            .into_iter()
            .filter(|c| !c.is_generated)
            .collect();

        // No conflict resolution needed (or possible): COPY straight into the target.
        let Some(action) = self.conflict_action_for(table) else {
            self.driver
                .copy_rows(&table.name, &ordered_cols, rows)
                .await?;
            return Ok(());
        };

        let staging_table = format!("__stratum_stage_{}", Uuid::new_v4().simple());

        debug!(table = %staging_table, "using staging table");

        let tx = self.driver.begin().await?;

        if let Err(e) = self.create_staging_table(table, &staging_table).await {
            let _ = tx.rollback().await;
            return Err(e);
        }

        // Copy rows into the staging table, then merge into the target.
        if let Err(err) = self
            .driver
            .copy_rows(&staging_table, &ordered_cols, rows)
            .await
        {
            let _ = self.drop_staging_table(&staging_table).await;
            let _ = tx.rollback().await;
            return Err(err.into());
        }

        let merge_result = self
            .merge_staging(table, &staging_table, &ordered_cols, action)
            .await;
        let drop_result = self.drop_staging_table(&staging_table).await;

        // Check both before committing so a failure rolls back cleanly.
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
