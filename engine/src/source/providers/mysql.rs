use crate::{record::Record, source::data_source::DbDataSource};
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
        let metadata = MetadataProvider::build_table_metadata(&adapter, table).await?;
        Ok(MySqlDataSource {
            metadata,
            table: table.to_string(),
            adapter,
        })
    }

    pub fn metadata(&self) -> &TableMetadata {
        &self.metadata
    }
}

#[async_trait]
impl DbDataSource for MySqlDataSource {
    async fn fetch_data(
        &self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
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
            .iter()
            .map(|row| Record::RowData(row.clone()))
            .collect::<Vec<_>>();
        Ok(records)
    }

    async fn get_metadata(&self) -> Result<TableMetadata, Box<dyn std::error::Error>> {
        let metadata = self.metadata();
        Ok(metadata.clone())
    }

    fn table_name(&self) -> &str {
        &self.table
    }
}
