use super::linked::LinkedSource;
use crate::{adapter::Adapter, error::MigrationError, filter::Filter};
use mysql::source::MySqlDataSource;
use smql::statements::connection::DataFormat;
use sql_adapter::{error::db::DbError, metadata::table::TableMetadata, source::DbDataSource};
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone)]
pub enum DataSource {
    Database(Arc<Mutex<dyn DbDataSource<Error = DbError>>>),
}

impl DataSource {
    pub fn from_adapter(
        format: DataFormat,
        adapter: &Adapter,
        linked: &Option<LinkedSource>,
        filter: &Option<Filter>,
    ) -> Result<Self, MigrationError> {
        match (format, adapter) {
            // MySQL + MySqlAdapter -> build a MySqlDataSource
            (DataFormat::MySql, Adapter::MySql(mysql_adapter)) => {
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
            (DataFormat::Postgres, Adapter::Postgres(_pg_adapter)) => {
                // TODO: implement PostgresDataSource
                panic!("Postgres data source is not implemented yet")
            }

            // Format says MySql but adapter is wrong
            (DataFormat::MySql, _) => Err(DbError::InvalidAdapter(
                "Adapter mismatch: expected MySql adapter".to_string(),
            )
            .into()),

            // Format says Postgres but adapter is wrong
            (DataFormat::Postgres, _) => Err(DbError::InvalidAdapter(
                "Adapter mismatch: expected Postgres adapter".to_string(),
            )
            .into()),

            // Anything else isn’t a SQL format we support
            (fmt, _) => Err(MigrationError::UnsupportedFormat(fmt.to_string())),
        }
    }

    pub async fn fetch_meta(&self, table: String) -> Result<TableMetadata, DbError> {
        match &self {
            DataSource::Database(db) => {
                let db = db.lock().await.adapter();
                let metadata = db.fetch_metadata(&table).await?;
                Ok(metadata)
            }
        }
    }
}
