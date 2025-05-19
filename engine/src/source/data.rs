use super::linked::LinkedSource;
use crate::{
    adapter::Adapter, error::MigrationError, filter::Filter, metadata::entity::EntityMetadata,
};
use csv::{
    error::FileError,
    source::{CsvDataSource, FileDataSource},
};
use mysql::source::MySqlDataSource;
use smql::statements::connection::DataFormat;
use sql_adapter::{error::db::DbError, source::DbDataSource};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub enum DataSource {
    Database(Arc<Mutex<dyn DbDataSource<Error = DbError>>>),
    File(Arc<Mutex<dyn FileDataSource<Error = FileError>>>),
}

impl DataSource {
    pub fn from_adapter(
        format: DataFormat,
        adapter: &Option<Adapter>,
        linked: &Option<LinkedSource>,
        filter: &Option<Filter>,
    ) -> Result<Self, MigrationError> {
        match (format, adapter) {
            // MySQL + MySqlAdapter -> build a MySqlDataSource
            (DataFormat::MySql, Some(Adapter::MySql(mysql_adapter))) => {
                let sql_filter = filter.as_ref().map(|f| {
                    let Filter::Sql(sf) = f;
                    sf.clone()
                });
                let join = linked.as_ref().and_then(|ls| {
                    if let LinkedSource::Table(j) = ls {
                        Some((**j).clone())
                    } else {
                        None
                    }
                });

                let ds = MySqlDataSource::new(mysql_adapter.clone(), join, sql_filter);
                Ok(DataSource::Database(Arc::new(Mutex::new(ds))))
            }

            // Postgres + PostgresAdapter -> stub for future implementation
            (DataFormat::Postgres, Some(Adapter::Postgres(_pg_adapter))) => {
                // TODO: implement PostgresDataSource
                panic!("Postgres data source is not implemented yet")
            }

            // CSV + FileAdapter -> build a CsvDataSource
            (DataFormat::Csv, Some(Adapter::Csv(file_adapter))) => {
                let ds = CsvDataSource::new(file_adapter.clone());
                Ok(DataSource::File(Arc::new(Mutex::new(ds))))
            }

            // Anything else isn’t a SQL format we support
            (fmt, _) => Err(MigrationError::UnsupportedFormat(fmt.to_string())),
        }
    }

    pub async fn fetch_meta(&self, table: String) -> Result<EntityMetadata, MigrationError> {
        match &self {
            DataSource::Database(db) => {
                let db = db.lock().await.adapter();
                let meta = db.fetch_metadata(&table).await?;
                Ok(EntityMetadata::Table(meta))
            }
            DataSource::File(file) => {
                let adapter = file.lock().await.adapter();
                let meta = adapter.fetch_metadata(&table).await?;
                Ok(EntityMetadata::Csv(meta))
            }
        }
    }
}
