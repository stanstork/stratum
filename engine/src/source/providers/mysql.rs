use crate::{
    config::mapping::TableMapping,
    database::row::{MySqlRowDataExt, RowData, RowDataExt},
    metadata::table::TableMetadata,
    source::datasource::DataSource,
};
use async_trait::async_trait;
use sql_adapter::{db_manager::DbManager, mysql::MySqlManager, query::builder::QueryBuilder};
use sqlx::Error;
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
            TableMetadata::build_graph(&mapping.table, &manager, &mut graph, &mut visited).await?;

        println!("Table metadata: {:?}", metadata);

        Ok(Self { metadata, manager })
    }

    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }
}

#[async_trait]
impl DataSource for MySqlDataSource {
    async fn fetch_data(&self) -> Result<Vec<RowData>, Error> {
        // let mut results = Vec::new();

        // let mut query = QueryBuilder::new();
        // let columns = self
        //     .metadata
        //     .columns
        //     .iter()
        //     .map(|col| col.0.clone())
        //     .collect::<Vec<_>>();

        // query.select(&columns).from(self.metadata.name.clone());
        // let query = query.build();

        // let rows = sqlx::query(&query.0).fetch_all(self.manager.pool()).await?;
        // for row in rows.iter() {
        //     let row_data = MySqlRowDataExt::from_row(row);
        //     results.push(row_data);
        // }

        // Ok(results)

        todo!()
    }
}
