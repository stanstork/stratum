use crate::source::{
    data_source::{DataSource, DbDataSource},
    record::DataRecord,
};
use async_trait::async_trait;
use sql_adapter::{adapter::DbAdapter, metadata::table::TableMetadata, requests::FetchRowsRequest};
use std::collections::{HashMap, HashSet};

pub struct MySqlDataSource {
    metadata: Option<TableMetadata>,
    table: String,
    adapter: Box<dyn DbAdapter + Send + Sync>,
}

impl MySqlDataSource {
    pub async fn new(
        table: &str,
        adapter: Box<dyn DbAdapter + Send + Sync>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let metadata = Self::build_metadata(&adapter, table).await.ok();
        let source = MySqlDataSource {
            metadata,
            table: table.to_string(),
            adapter,
        };
        Ok(source)
    }

    pub fn metadata(&self) -> &Option<TableMetadata> {
        &self.metadata
    }

    async fn build_metadata(
        adapter: &Box<dyn DbAdapter + Send + Sync>,
        table: &str,
    ) -> Result<TableMetadata, Box<dyn std::error::Error>> {
        let mut graph = HashMap::new();
        let mut visited = HashSet::new();
        let metadata =
            TableMetadata::build_dep_graph(table, adapter, &mut graph, &mut visited).await?;
        Ok(metadata)
    }
}

#[async_trait]
impl DataSource for MySqlDataSource {
    type Record = Box<dyn DataRecord + Send + Sync>;

    async fn fetch_data(
        &self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Result<Vec<Box<dyn DataRecord + Send + Sync>>, Box<dyn std::error::Error>> {
        let columns = self
            .metadata()
            .as_ref()
            .unwrap()
            .columns
            .keys()
            .cloned()
            .collect();
        let request = FetchRowsRequest {
            table: self.table.clone(),
            columns,
            limit: batch_size,
            offset,
        };

        let rows = self.adapter.fetch_rows(request).await?;
        let records = rows
            .into_iter()
            .map(|row| Box::new(row) as Box<dyn DataRecord + Send + Sync>)
            .collect();

        Ok(records)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[async_trait]
impl DbDataSource for MySqlDataSource {
    async fn get_metadata(&self) -> Result<TableMetadata, Box<dyn std::error::Error>> {
        let metadata = self.metadata().as_ref().unwrap();
        Ok(metadata.clone())
    }
}
