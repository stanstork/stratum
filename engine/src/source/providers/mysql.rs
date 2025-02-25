use crate::{config::mapping::TableMapping, source::datasource::DataSource};
use async_trait::async_trait;
use sql_adapter::{
    db_manager::DbManager, metadata::table::TableMetadata, mysql::MySqlManager,
    query::builder::SqlQueryBuilder, row::RowData,
};
use std::collections::{HashMap, HashSet};

pub struct MySqlDataSource {
    metadata: TableMetadata,
    manager: MySqlManager,
}

impl MySqlDataSource {
    pub async fn new(url: &str, mapping: TableMapping) -> Result<Self, Box<dyn std::error::Error>> {
        let manager = MySqlManager::connect(url).await?;

        let mut graph = HashMap::new();
        let mut visited = HashSet::new();
        let metadata =
            TableMetadata::build_dep_graph(&mapping.table, &manager, &mut graph, &mut visited)
                .await?;

        Ok(Self { metadata, manager })
    }

    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }
}

#[async_trait]
impl DataSource for MySqlDataSource {
    async fn fetch_rows(&self) -> Result<Vec<RowData>, Box<dyn std::error::Error>> {
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
        let rows = self.manager.fetch_all(&query.0).await?;

        Ok(rows)
    }
}
