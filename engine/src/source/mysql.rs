use super::datasource::DataSource;
use crate::{
    config::mapping::TableMapping,
    database::{
        connection::{DbConnection, MySqlConnection},
        query,
        row::{MySqlRowDataExt, RowData, RowDataExt},
    },
    metadata::table::TableMetadata,
};
use async_trait::async_trait;
use sqlx::Error;

pub struct MySqlDataSource {
    metadata: Vec<TableMetadata>,
    conn: MySqlConnection,
}

impl MySqlDataSource {
    pub async fn new(url: &str, mappings: Vec<TableMapping>) -> Result<Self, Error> {
        let conn = MySqlConnection::connect(url).await?;
        let mut metadata = Vec::new();

        for mapping in mappings {
            let table_metadata = TableMetadata::from_mapping(mapping, &conn).await?;
            metadata.push(table_metadata);
        }

        Ok(Self { metadata, conn })
    }

    pub fn metadata(&self) -> &Vec<TableMetadata> {
        &self.metadata
    }
}

#[async_trait]
impl DataSource for MySqlDataSource {
    async fn fetch_data(&self) -> Result<Vec<RowData>, Error> {
        let mut results = Vec::new();

        for table in &self.metadata {
            let mut query = query::QueryBuilder::new();
            let columns = table
                .columns
                .iter()
                .map(|col| col.name.clone())
                .collect::<Vec<_>>();

            query.select(&columns).from(table.name.clone());
            let query = query.build();

            let rows = sqlx::query(&query.0).fetch_all(self.conn.pool()).await?;
            for row in rows.iter() {
                let row_data = MySqlRowDataExt::from_row(row);
                results.push(row_data);
            }
        }

        Ok(results)
    }
}
