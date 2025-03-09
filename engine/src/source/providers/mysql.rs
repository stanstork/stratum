use crate::{
    config::mapping::TableMapping,
    source::{data_source::DataSource, record::DataRecord},
};
use async_trait::async_trait;
use sql_adapter::{
    adapter::DbAdapter, metadata::table::TableMetadata, mysql::MySqlAdapter,
    query::builder::SqlQueryBuilder,
};
use std::collections::{HashMap, HashSet};

pub struct MySqlDataSource {
    metadata: TableMetadata,
    manager: MySqlAdapter,
}

impl MySqlDataSource {
    pub async fn new(url: &str, mapping: TableMapping) -> Result<Self, Box<dyn std::error::Error>> {
        let manager = MySqlAdapter::connect(url).await?;
        let metadata = Self::build_metadata(&manager, &mapping).await?;

        Ok(Self { metadata, manager })
    }

    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    async fn build_metadata(
        manager: &MySqlAdapter,
        mapping: &TableMapping,
    ) -> Result<TableMetadata, Box<dyn std::error::Error>> {
        let mut graph = HashMap::new();
        let mut visited = HashSet::new();
        let metadata =
            TableMetadata::build_dep_graph(&mapping.table, manager, &mut graph, &mut visited)
                .await?;
        Ok(metadata)
    }
}

#[async_trait]
impl DataSource for MySqlDataSource {
    type Record = Box<dyn DataRecord>;

    async fn fetch_data(&self) -> Result<Vec<Box<dyn DataRecord>>, Box<dyn std::error::Error>> {
        let mut builder = SqlQueryBuilder::new();
        let columns = self
            .metadata
            .columns
            .iter()
            .map(|col| col.0.clone())
            .collect::<Vec<_>>();
        let query = builder
            .select(&columns)
            .from(self.metadata.name.clone())
            .build();
        let rows = self.manager.fetch_rows(&query.0).await?;
        let records = rows
            .into_iter()
            .map(|row| Box::new(row) as Box<dyn DataRecord>)
            .collect();

        Ok(records)
    }
}
