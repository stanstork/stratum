use crate::{destination::data_dest::DbDataDestination, record::Record};
use async_trait::async_trait;
use postgres::postgres::PgAdapter;
use sql_adapter::{
    adapter::SqlAdapter,
    metadata::table::TableMetadata,
    query::{builder::SqlQueryBuilder, column::ColumnDef},
    schema::{manager::SchemaManager, plan::SchemaPlan},
};
use std::collections::HashMap;
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
        schema_plan: &SchemaPlan,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if self.table_exists(schema_plan.table_name()).await? {
            info!("Table '{}' already exists", schema_plan.table_name());
            return Ok(());
        }

        let queries = schema_plan
            .enum_queries
            .iter()
            .chain(&schema_plan.table_queries)
            .chain(&schema_plan.constraint_queries)
            .cloned();

        for query in queries {
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

    fn adapter(&self) -> &(dyn SqlAdapter + Send + Sync) {
        &self.adapter
    }
}

impl SchemaManager for PgDestination {
    fn get_metadata(&self, table: &str) -> &TableMetadata {
        self.metadata
            .get(table)
            .unwrap_or_else(|| panic!("Metadata for table {} not found", table))
    }

    fn set_metadata(&mut self, table: &str, metadata: TableMetadata) {
        self.metadata.insert(table.to_string(), metadata);
    }
}
