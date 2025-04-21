use crate::{destination::data_dest::DbDataDestination, record::Record};
use async_trait::async_trait;
use postgres::postgres::PgAdapter;
use sql_adapter::{
    adapter::SqlAdapter,
    metadata::table::TableMetadata,
    query::{builder::SqlQueryBuilder, column::ColumnDef},
    schema::plan::SchemaPlan,
};
use std::{collections::HashMap, sync::Arc};
use tracing::{error, info};

pub struct PgDestination {
    metadata: HashMap<String, TableMetadata>,
    adapter: PgAdapter,
}

impl PgDestination {
    pub fn new(adapter: PgAdapter) -> Self {
        Self {
            metadata: HashMap::new(),
            adapter,
        }
    }
}

#[async_trait]
impl DbDataDestination for PgDestination {
    async fn write_batch(
        &self,
        metadata: &TableMetadata,
        records: Vec<Record>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if records.is_empty() {
            return Ok(());
        }

        let columns = metadata
            .columns
            .values()
            .map(ColumnDef::new)
            .collect::<Vec<_>>();

        if columns.is_empty() {
            return Err("write_batch: No valid columns found in records".into());
        }

        let all_values: Vec<Vec<String>> = records
            .into_iter()
            .filter_map(|record| match record {
                Record::RowData(row) => Some(
                    columns
                        .iter()
                        .map(|col| {
                            row.columns
                                .iter()
                                .find(|rc| rc.name.eq_ignore_ascii_case(&col.name))
                                .and_then(|rc| rc.value.clone())
                                .map_or_else(|| "NULL".to_string(), |val| val.to_string())
                        })
                        .collect(),
                ),
            })
            .collect();

        let query = SqlQueryBuilder::new()
            .insert_batch(&metadata.name, columns, all_values)
            .build();

        info!("Executing insert into `{}`", metadata.name);
        self.adapter.execute(&query.0).await?;

        Ok(())
    }

    async fn infer_schema(
        &self,
        schema_plan: &SchemaPlan<'_>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let enum_queries = schema_plan.enum_queries().await?;
        let table_queries = schema_plan.table_queries().await;
        let fk_queries = schema_plan.fk_queries();

        let all_queries = enum_queries
            .iter()
            .chain(&table_queries)
            .chain(&fk_queries)
            .cloned();

        for query in all_queries {
            info!("Executing query: {}", query);
            if let Err(err) = self.adapter.execute(&query).await {
                error!("Failed to execute query: {}\nError: {:?}", query, err);
                return Err(err);
            }
        }

        info!("Schema inference completed");
        Ok(())
    }

    async fn toggle_trigger(
        &self,
        table: &str,
        enable: bool,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let query = SqlQueryBuilder::new().toggle_trigger(table, enable).build();

        info!("Executing query: {}", query.0);
        self.adapter.execute(&query.0).await?;

        Ok(())
    }

    async fn table_exists(&self, table: &str) -> Result<bool, Box<dyn std::error::Error>> {
        self.adapter.table_exists(table).await
    }

    async fn add_column(
        &self,
        table: &str,
        column: &ColumnDef,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let query = SqlQueryBuilder::new().add_column(table, column).build();

        info!("Executing query: {}", query.0);
        self.adapter.execute(&query.0).await?;

        Ok(())
    }

    fn get_metadata(&self, table: &str) -> &TableMetadata {
        self.metadata
            .get(table)
            .unwrap_or_else(|| panic!("Metadata for table {} not found", table))
    }

    fn set_metadata(&mut self, metadata: HashMap<String, TableMetadata>) {
        self.metadata = metadata;
    }

    fn get_tables(&self) -> Vec<TableMetadata> {
        self.metadata.values().cloned().collect()
    }

    fn adapter(&self) -> Arc<(dyn SqlAdapter + Send + Sync)> {
        Arc::new(self.adapter.clone())
    }
}
