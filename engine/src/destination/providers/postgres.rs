use crate::{destination::data_dest::DbDataDestination, record::Record};
use async_trait::async_trait;
use postgres::postgres::PgAdapter;
use sql_adapter::{
    adapter::SqlAdapter,
    metadata::{provider::MetadataProvider, table::TableMetadata},
    query::builder::SqlQueryBuilder,
    schema_plan::SchemaPlan,
};
use tracing::{error, info};

pub struct PgDestination {
    metadata: Option<TableMetadata>,
    adapter: PgAdapter,
}

#[async_trait]
impl DbDataDestination for PgDestination {
    async fn write(
        &self,
        metadata: &TableMetadata,
        record: Record,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let columns = match record {
            Record::RowData(row) => row
                .columns
                .iter()
                .map(|col| {
                    let value = col
                        .value
                        .clone()
                        .map_or("NULL".to_string(), |val| val.to_string());
                    (col.name.clone(), value)
                })
                .collect::<Vec<(String, String)>>(),
        };

        let query = SqlQueryBuilder::new()
            .insert_into(&metadata.name, &columns)
            .build();

        info!("Executing query: {}", query.0);
        self.adapter.execute(&query.0).await?;

        Ok(())
    }

    async fn write_batch(
        &self,
        metadata: &TableMetadata,
        records: Vec<Record>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if records.is_empty() {
            return Ok(());
        }

        let mut column_names = Vec::new();
        let mut all_values = Vec::new();
        let prefix = format!("{}_", metadata.name);

        for record in records {
            let row = match record {
                Record::RowData(row) => row,
            };

            if column_names.is_empty() {
                column_names = row
                    .columns
                    .iter()
                    .map(|col| col.name.replacen(&prefix, "", 1))
                    .collect();
            }

            let row_values = column_names
                .iter()
                .map(|col_name| {
                    let full_name = format!("{}_{}", metadata.name, col_name);
                    row.columns
                        .iter()
                        .find(|col| col.name == full_name)
                        .and_then(|col| col.value.clone())
                        .map_or("NULL".to_string(), |val| val.to_string())
                })
                .collect();

            all_values.push(row_values);
        }

        if column_names.is_empty() {
            return Err("write_batch: No valid columns found in records".into());
        }

        let query = SqlQueryBuilder::new()
            .insert_batch(&metadata.name, column_names, all_values)
            .build();

        info!("Executing insert into `{}`: {}", metadata.name, query.0);
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
            .chain(&schema_plan.create_table_queries)
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

    fn adapter(&self) -> &(dyn SqlAdapter + Send + Sync) {
        &self.adapter
    }

    fn set_metadata(&mut self, metadata: TableMetadata) {
        self.metadata = Some(metadata);
    }

    fn metadata(&self) -> &TableMetadata {
        self.metadata.as_ref().expect("Metadata not set")
    }
}

impl PgDestination {
    pub async fn new(adapter: PgAdapter, table: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let metadata = match adapter.table_exists(table).await? {
            true => Some(MetadataProvider::build_table_metadata(&adapter, table).await?),
            false => None,
        };

        Ok(PgDestination { adapter, metadata })
    }

    pub async fn table_exists(&self, table: &str) -> Result<bool, Box<dyn std::error::Error>> {
        self.adapter.table_exists(table).await.map_err(Into::into)
    }
}
