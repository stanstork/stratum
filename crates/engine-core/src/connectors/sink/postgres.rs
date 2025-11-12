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
use planner::query::dialect::{self, Dialect};
use std::collections::HashSet;
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

    fn quote_ident(&self, ident: &str) -> String {
        self.dialect.quote_identifier(ident)
    }

    fn qualify_table(&self, metadata: &TableMetadata) -> String {
        match &metadata.schema {
            Some(schema) if !schema.is_empty() => format!(
                "{}.{}",
                self.quote_ident(schema),
                self.quote_ident(&metadata.name)
            ),
            _ => self.quote_ident(&metadata.name),
        }
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

    async fn drop_staging_table(&self, name: &str) -> Result<(), SinkError> {
        let staging_ident = self.quote_ident(name);
        let drop_sql = format!("DROP TABLE IF EXISTS {staging_ident}");
        println!("Dropping staging table with SQL: {}", drop_sql);
        self.adapter.exec(&drop_sql).await?;
        Ok(())
    }

    async fn merge_staging(
        &self,
        meta: &TableMetadata,
        staging_table: &str,
        columns: &Vec<ColumnMetadata>,
    ) -> Result<(), SinkError> {
        if meta.primary_keys.is_empty() {
            return Err(SinkError::FastPathNotSupported(
                "Table has no primary keys".to_string(),
            ));
        }

        let pk_set = meta
            .primary_keys
            .iter()
            .map(|pk| pk.to_lowercase())
            .collect::<HashSet<_>>();

        let target = self.qualify_table(meta);
        let staging = self.quote_ident(staging_table);

        let has_merge = self.adapter.capabilities().await?.merge_statements;

        if has_merge {
            // Aliases
            let t = "t";
            let s = "s";

            // t.pk = s.pk AND ...
            let match_clause = meta
                .primary_keys
                .iter()
                .map(|pk| {
                    let col = self.quote_ident(pk);
                    format!("{t}.{col} = {s}.{col}")
                })
                .collect::<Vec<_>>()
                .join(" AND ");

            // Build non-PK SET assignments
            let non_pk_updates = columns
                .iter()
                .filter(|c| !pk_set.contains(&c.name.to_lowercase()))
                .map(|c| {
                    let col = self.quote_ident(&c.name);
                    format!("{col} = {s}.{col}")
                })
                .collect::<Vec<_>>();

            let update_clause = if non_pk_updates.is_empty() {
                // No non-PK columns to update - do nothing on match
                "WHEN MATCHED THEN DO NOTHING".to_string()
            } else {
                format!("WHEN MATCHED THEN UPDATE SET {}", non_pk_updates.join(", "))
            };

            // INSERT column list and VALUES
            let ordered_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
            let insert_columns = ordered_names
                .iter()
                .map(|name| self.quote_ident(name))
                .collect::<Vec<_>>()
                .join(", ");

            let insert_values = ordered_names
                .iter()
                .map(|name| format!("{s}.{}", self.quote_ident(name)))
                .collect::<Vec<_>>()
                .join(", ");

            let insert_clause =
                format!("WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})");

            // Final SQL (PostgreSQL 15+ MERGE)
            let sql = format!(
                "MERGE INTO {target} AS {t} \
         USING {staging} AS {s} \
         ON {match_clause} \
         {update_clause} \
         {insert_clause}"
            );

            println!("MERGE SQL: {}", sql);

            self.adapter.exec(&sql).await?;
        } else {
            // Fallback to separate UPDATE and INSERT statements

            let insert_columns = columns
                .iter()
                .map(|c| self.quote_ident(&c.name))
                .collect::<Vec<_>>()
                .join(", ");

            let select_columns = columns
                .iter()
                .map(|c| format!("{staging}.{}", self.quote_ident(&c.name)))
                .collect::<Vec<_>>()
                .join(", ");

            let update_assignments = columns
                .iter()
                .filter(|c| !pk_set.contains(&c.name.to_lowercase()))
                .map(|c| {
                    let col = self.quote_ident(&c.name);
                    format!("{col} = EXCLUDED.{col}")
                })
                .collect::<Vec<_>>()
                .join(", ");

            let pk_list = meta
                .primary_keys
                .iter()
                .map(|pk| self.quote_ident(pk))
                .collect::<Vec<_>>()
                .join(", ");

            let sql = if update_assignments.is_empty() {
                format!(
                    "INSERT INTO {target} ({insert_columns}) \
                 SELECT {select_columns} FROM {staging} \
                 ON CONFLICT ({pk_list}) DO NOTHING"
                )
            } else {
                format!(
                    "INSERT INTO {target} ({insert_columns}) \
                 SELECT {select_columns} FROM {staging} \
                 ON CONFLICT ({pk_list}) DO UPDATE SET {update_assignments}"
                )
            };

            println!("UPSERT SQL: {}", sql);
            self.adapter.exec(&sql).await?;
        }

        Ok(())
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

        if let Err(err) = copy_result {
            let _ = self.drop_staging_table(&staging_table).await;
            return Err(err.into());
        }

        let merge_result = self
            .merge_staging(table, &staging_table, &ordered_cols)
            .await;
        let drop_result = self.drop_staging_table(&staging_table).await;

        merge_result?;
        drop_result?;

        Ok(())
    }
}
