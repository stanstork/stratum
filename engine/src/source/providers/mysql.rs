use crate::{record::Record, source::data_source::DbDataSource};
use async_trait::async_trait;
use mysql::mysql::MySqlAdapter;
use sql_adapter::adapter::SqlAdapter;
use sql_adapter::{metadata::table::TableMetadata, requests::FetchRowsRequest};
use std::collections::HashMap;

pub struct MySqlDataSource {
    metadata: HashMap<String, TableMetadata>,
    adapter: MySqlAdapter,
}

impl MySqlDataSource {
    pub fn new(adapter: MySqlAdapter) -> Self {
        Self {
            metadata: HashMap::new(),
            adapter,
        }
    }
}

#[async_trait]
impl DbDataSource for MySqlDataSource {
    async fn fetch_data(
        &self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
        let mut records = Vec::new();
        for table in self.metadata.keys() {
            let grouped_fields = self.get_metadata(table).select_fields();
            for (table, fields) in grouped_fields {
                let request =
                    FetchRowsRequest::new(table.clone(), None, fields, vec![], batch_size, offset);
                let rows = self.adapter.fetch_rows(request).await?;
                records.extend(rows.into_iter().map(Record::RowData));
            }
        }
        Ok(records)
    }

    fn get_metadata(&self, table: &str) -> &TableMetadata {
        self.metadata
            .get(table)
            .unwrap_or_else(|| panic!("Metadata for table {} not found", table))
    }

    fn set_metadata(&mut self, metadata: HashMap<String, TableMetadata>) {
        self.metadata = metadata;
    }

    fn adapter(&self) -> &(dyn SqlAdapter + Send + Sync) {
        &self.adapter
    }
}
