use crate::error::MigrationError;
use connectors::{
    adapter::{Adapter, SqlDriver},
    metadata::entity::EntityMetadata,
    sql::base::metadata::provider::MetadataProvider,
};
use std::collections::HashMap;
use tracing::info;

pub async fn load_metadata(
    conn_str: &str,
    driver: SqlDriver,
) -> Result<HashMap<String, EntityMetadata>, MigrationError> {
    info!("Loading source metadata");

    let adapter = Adapter::sql(driver, conn_str).await?;
    let names = adapter.get_sql().list_tables().await?;

    info!("Found {} source tables: {:?}", names.len(), names);

    let meta_graph = MetadataProvider::build_metadata_graph(adapter.get_sql(), &names).await?;

    info!(
        "Source metadata graph built with {} tables",
        meta_graph.len()
    );

    Ok(meta_graph
        .iter()
        .map(|(name, meta)| (name.clone(), EntityMetadata::Table(meta.clone())))
        .collect())
}
