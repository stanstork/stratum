use crate::{adapter::SqlAdapter, metadata::table::TableMetadata, row::row_data::RowData};
use async_trait::async_trait;
use std::{collections::HashMap, sync::Arc};

#[async_trait]
pub trait DbDataSource: Send + Sync {
    async fn fetch(
        &self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Result<Vec<RowData>, Box<dyn std::error::Error>>;

    fn get_metadata(&self, table: &str) -> &TableMetadata;
    fn set_metadata(&mut self, metadata: HashMap<String, TableMetadata>);

    fn adapter(&self) -> Arc<(dyn SqlAdapter + Send + Sync)>;
}
