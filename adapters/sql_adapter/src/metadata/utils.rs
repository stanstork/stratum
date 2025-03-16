use super::table::TableMetadata;
use crate::adapter::DbAdapter;
use std::collections::{HashMap, HashSet};

pub async fn build_table_metadata(
    adapter: &Box<dyn DbAdapter + Send + Sync>,
    table: &str,
) -> Result<TableMetadata, Box<dyn std::error::Error>> {
    let mut graph = HashMap::new();
    let mut visited = HashSet::new();
    let metadata = TableMetadata::build_dep_graph(table, adapter, &mut graph, &mut visited).await?;
    Ok(metadata)
}
