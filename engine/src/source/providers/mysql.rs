use crate::{
    config::mapping::TableMapping,
    database::{
        connection::{DbConnection, MySqlConnection},
        query,
        row::{MySqlRowDataExt, RowData, RowDataExt},
    },
    metadata::table::TableMetadata,
    source::datasource::DataSource,
};
use async_trait::async_trait;
use sqlx::Error;

pub struct MySqlDataSource {
    metadata: TableMetadata,
    conn: MySqlConnection,
}

impl MySqlDataSource {
    pub async fn new(url: &str, mapping: TableMapping) -> Result<Self, Error> {
        let conn = MySqlConnection::connect(url).await?;
        let metadata = TableMetadata::from_mapping(mapping, &conn).await?;

        Ok(Self { metadata, conn })
    }

    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }
}

#[async_trait]
impl DataSource for MySqlDataSource {
    async fn fetch_data(&self) -> Result<Vec<RowData>, Error> {
        let mut results = Vec::new();

        let mut query = query::QueryBuilder::new();
        let columns = self
            .metadata
            .columns
            .iter()
            .map(|col| col.name.clone())
            .collect::<Vec<_>>();

        query.select(&columns).from(self.metadata.name.clone());
        let query = query.build();

        let rows = sqlx::query(&query.0).fetch_all(self.conn.pool()).await?;
        for row in rows.iter() {
            let row_data = MySqlRowDataExt::from_row(row);
            results.push(row_data);
        }

        Ok(results)
    }
}
