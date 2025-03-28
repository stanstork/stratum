use crate::{record::Record, source::data_source::DbDataSource};
use async_trait::async_trait;
use mysql::mysql::MySqlAdapter;
use sql_adapter::adapter::SqlAdapter;
use sql_adapter::{metadata::table::TableMetadata, requests::FetchRowsRequest};

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
        Ok(MySqlDataSource {
            metadata: adapter.fetch_metadata(table).await?,
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
        let grouped_columns = self.metadata().select_fields();
        let mut records = Vec::new();

        for (table, columns) in grouped_columns {
            let request =
                FetchRowsRequest::new(table.clone(), None, columns, vec![], batch_size, offset);
            let rows = self.adapter.fetch_rows(request).await?;
            records.extend(rows.into_iter().map(Record::RowData));
        }

        Ok(records)
    }

    fn get_metadata(&self) -> &TableMetadata {
        &self.metadata
    }

    fn set_metadata(&mut self, metadata: TableMetadata) {
        self.metadata = metadata;
    }

    fn table_name(&self) -> &str {
        &self.table
    }

    fn adapter(&self) -> &(dyn SqlAdapter + Send + Sync) {
        &self.adapter
    }
}
