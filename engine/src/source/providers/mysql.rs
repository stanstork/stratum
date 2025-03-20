use crate::{record::DataRecord, source::data_source::DbDataSource};
use async_trait::async_trait;
use sql_adapter::{
    adapter::DbAdapter,
    metadata::{provider::MetadataProvider, table::TableMetadata},
    mysql::MySqlAdapter,
    requests::FetchRowsRequest,
};

pub struct MySqlDataSource {
    metadata: TableMetadata,
    table: String,
    adapter: MySqlAdapter,
}

impl MySqlDataSource {
    pub async fn new(
        table: &str,
        adapter: MySqlAdapter,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let boxed_adapter: Box<dyn DbAdapter + Send + Sync> = Box::new(adapter.clone());
        let metadata = MetadataProvider::build_table_metadata(&boxed_adapter, table).await?;

        let source = MySqlDataSource {
            metadata,
            table: table.to_string(),
            adapter,
        };
        Ok(source)
    }

    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }
}

#[async_trait]
impl DbDataSource for MySqlDataSource {
    type Record = Box<dyn DataRecord + Send + Sync>;

    async fn fetch_data(
        &self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Result<Vec<Box<dyn DataRecord + Send + Sync>>, Box<dyn std::error::Error>> {
        let metadata = self.metadata();
        let columns = metadata.collect_columns();
        let joins = metadata.collect_joins();

        let request = FetchRowsRequest {
            table: self.table.clone(),
            alias: self.table.clone(),
            joins,
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

    async fn get_metadata(&self) -> Result<TableMetadata, Box<dyn std::error::Error>> {
        let metadata = self.metadata();
        Ok(metadata.clone())
    }
}
