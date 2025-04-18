use crate::{
    context::MigrationContext, destination::data_dest::DataDestination,
    source::data_source::DataSource,
};
use sql_adapter::{
    adapter::SqlAdapter,
    metadata::{provider::MetadataProvider, table::TableMetadata},
};
use std::{collections::HashMap, sync::Arc};
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
    let state = context.state.lock().await;

    if let DataSource::Database(ref src) = context.source.primary {
        let metadata = get_metadata(
            src.lock().await.adapter(),
            source_tables,
            state.infer_schema,
        )
        .await?;
        src.lock().await.set_metadata(metadata);
    }

    Ok(())
}

pub async fn set_destination_metadata(
    context: &Arc<Mutex<MigrationContext>>,
    destination_tables: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let context = context.lock().await;
    let state = context.state.lock().await;

    if let DataDestination::Database(ref dest) = context.destination {
        let metadata = get_metadata(
            dest.lock().await.adapter(),
            destination_tables,
            state.infer_schema,
        )
        .await?;
        dest.lock().await.set_metadata(metadata);
    }

    Ok(())
}

async fn get_metadata(
    adapter: Arc<(dyn SqlAdapter + Send + Sync)>,
    tables: &[String],
    infer_schema: bool,
) -> Result<HashMap<String, TableMetadata>, Box<dyn std::error::Error>> {
    let adapter = adapter.as_ref();

    if infer_schema {
        MetadataProvider::build_metadata_graph(adapter, tables).await
    } else {
        let mut metadata = HashMap::new();
        for table in tables {
            let table_metadata = adapter.fetch_metadata(table).await?;
            metadata.insert(table.clone(), table_metadata);
        }
        Ok(metadata)
    }
}
