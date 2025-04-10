use crate::{
    context::MigrationContext, destination::data_dest::DataDestination,
    source::data_source::DataSource,
};
use sql_adapter::metadata::{provider::MetadataProvider, table::TableMetadata};
use std::sync::Arc;
use tokio::sync::Mutex;

pub async fn fetch_dest_metadata(
    destination: &DataDestination,
    table: &str,
) -> Result<TableMetadata, Box<dyn std::error::Error>> {
    match destination {
        DataDestination::Database(db) => {
            let db = db.lock().await.adapter();
            let metadata = db.fetch_metadata(table).await?;
            Ok(metadata)
        }
    }
}

pub async fn fetch_source_metadata(
    source: &DataSource,
    table: &str,
) -> Result<TableMetadata, Box<dyn std::error::Error>> {
    match source {
        DataSource::Database(db) => {
            let db = db.lock().await.adapter();
            let metadata = db.fetch_metadata(table).await?;
            Ok(metadata)
        }
    }
}

pub async fn set_source_metadata(
    context: &Arc<Mutex<MigrationContext>>,
    source_tables: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let context = context.lock().await;
    if let DataSource::Database(src) = &context.source {
        let mut src_guard = src.lock().await;
        let metadata =
            MetadataProvider::build_metadata_graph(src_guard.adapter().as_ref(), source_tables)
                .await?;
        src_guard.set_metadata(metadata);
    }
    Ok(())
}

pub async fn set_destination_metadata(
    context: &Arc<Mutex<MigrationContext>>,
    destination_tables: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let context = context.lock().await;
    if let DataDestination::Database(dest) = &context.destination {
        let mut dest_guard = dest.lock().await;
        let metadata = MetadataProvider::build_metadata_graph(
            dest_guard.adapter().as_ref(),
            destination_tables,
        )
        .await?;
        dest_guard.set_metadata(metadata);
    }
    Ok(())
}
