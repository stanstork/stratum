use crate::{
    drivers::postgres::driver::PgDriver,
    error::DriverError,
    sql::{
        metadata::table::TableMetadata,
        query::{column::ColumnDef, generator::QueryGenerator},
    },
    traits::{ddl::DdlWriter, executor::QueryExecutor},
};
use async_trait::async_trait;
use query_builder::dialect;
use tracing::debug;

#[async_trait]
impl DdlWriter for PgDriver {
    async fn add_column(&self, table: &str, column: &ColumnDef) -> Result<(), DriverError> {
        let (sql, _params) =
            QueryGenerator::new(&dialect::Postgres).add_column(table, column.clone());

        debug!(column = %column.name, table = %table, "adding column");

        self.execute(&sql).await?;

        Ok(())
    }

    /// PostgreSQL's indexes build far faster in bulk after the load, so drop
    /// each primary key first and hand back the DDL to rebuild them afterwards.
    async fn drop_primary_keys(&self, metas: &[TableMetadata]) -> Result<Vec<String>, DriverError> {
        let generator = QueryGenerator::new(&dialect::Postgres);
        let mut rebuild = Vec::with_capacity(metas.len());

        for meta in metas {
            if meta.primary_keys.is_empty() {
                continue;
            }

            debug!(table = %meta.name, "dropping primary key for bulk load");

            self.execute(&generator.drop_primary_key(&meta.name))
                .await?;

            let (add_pk, _) = generator.add_primary_key(&meta.name, &meta.primary_keys);
            rebuild.push(add_pk);
        }

        Ok(rebuild)
    }
}
