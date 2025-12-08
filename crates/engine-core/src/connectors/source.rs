use crate::connectors::{filter::Filter, linked::LinkedSource};
use connectors::{
    adapter::Adapter,
    error::AdapterError,
    file::csv::{
        error::FileError,
        source::{CsvDataSource, FileDataSource},
    },
    metadata::entity::EntityMetadata,
    sql::{
        base::{error::DbError, source::DbDataSource},
        mysql::source::MySqlDataSource,
    },
};
use model::pagination::{cursor::Cursor, page::FetchResult};
use planner::query::{
    dialect::{self, Dialect},
    offsets::OffsetStrategy,
};
use std::{fmt::Display, sync::Arc};
use tokio::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum DataFormat {
    MySql,
    Postgres,
    Csv,
}

impl DataFormat {
    pub fn intersects(&self, other: &DataFormat) -> bool {
        todo!()
    }
}

impl Display for DataFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataFormat::MySql => write!(f, "MySQL"),
            DataFormat::Postgres => write!(f, "Postgres"),
            DataFormat::Csv => write!(f, "CSV"),
        }
    }
}

/// Represents a data source,
/// such as a database table or file to be read from.
#[derive(Clone)]
pub enum DataSource {
    Database(Arc<Mutex<dyn DbDataSource<Error = DbError>>>),
    File(Arc<Mutex<dyn FileDataSource<Error = FileError>>>),
}
/// Represents a migration source,
/// such as a database table, file, or API to be transformed and written to a destination.
#[derive(Clone)]
pub struct Source {
    pub name: String,
    pub format: DataFormat,
    pub primary: DataSource,
    pub linked: Option<LinkedSource>,
    pub filter: Option<Filter>,
}

impl DataSource {
    pub fn from_adapter(
        format: DataFormat,
        adapter: &Adapter,
        linked: &Option<LinkedSource>,
        filter: &Option<Filter>,
        offset_strategy: Arc<dyn OffsetStrategy>,
    ) -> Result<Self, AdapterError> {
        match (format, adapter) {
            // MySQL + MySqlAdapter -> build a MySqlDataSource
            (DataFormat::MySql, Adapter::MySql(mysql_adapter)) => {
                let sql_filter = filter.as_ref().map(|f| match f {
                    Filter::Sql(sql_filter) => sql_filter.clone(),
                    _ => panic!("Invalid filter type for MySQL"),
                });
                let join = linked.as_ref().and_then(|ls| {
                    if let LinkedSource::Table(j) = ls {
                        Some((**j).clone())
                    } else {
                        None
                    }
                });

                let ds =
                    MySqlDataSource::new(mysql_adapter.clone(), join, sql_filter, offset_strategy);
                Ok(DataSource::Database(Arc::new(Mutex::new(ds))))
            }

            // Postgres + PostgresAdapter -> stub for future implementation
            (DataFormat::Postgres, Adapter::Postgres(_pg_adapter)) => {
                // TODO: implement PostgresDataSource
                panic!("Postgres data source is not implemented yet")
            }

            // CSV + FileAdapter -> build a CsvDataSource
            (DataFormat::Csv, Adapter::Csv(file_adapter)) => {
                let csv_filter = filter.as_ref().map(|f| match f {
                    Filter::Csv(csv_filter) => csv_filter.clone(),
                    _ => panic!("Invalid filter type for CSV"),
                });
                let ds = CsvDataSource::new(file_adapter.clone(), csv_filter);
                Ok(DataSource::File(Arc::new(Mutex::new(ds))))
            }

            // Anything else isn't a SQL format we support
            (fmt, _) => Err(AdapterError::UnsupportedFormat(fmt.to_string())),
        }
    }

    pub async fn fetch_meta(&self, entity: String) -> Result<EntityMetadata, AdapterError> {
        match &self {
            DataSource::Database(db) => {
                let db = db.lock().await.adapter();
                let meta = db.table_metadata(&entity).await?;
                Ok(EntityMetadata::Table(meta))
            }
            DataSource::File(file) => {
                let adapter = file.lock().await.adapter();
                let meta = adapter.fetch_metadata(&entity).await?;
                Ok(EntityMetadata::Csv(meta))
            }
        }
    }
}

impl Source {
    pub fn new(
        name: String,
        format: DataFormat,
        primary: DataSource,
        linked: Option<LinkedSource>,
        filter: Option<Filter>,
    ) -> Self {
        Source {
            name,
            format,
            primary,
            linked,
            filter,
        }
    }

    pub async fn fetch_data(
        &self,
        batch_size: usize,
        cursor: Cursor,
    ) -> Result<FetchResult, AdapterError> {
        match &self.primary {
            DataSource::Database(db) => {
                let db = db.lock().await;
                let res = db.fetch(batch_size, cursor).await?;
                Ok(res)
            }
            DataSource::File(file) => {
                let mut file = file.lock().await;
                let cursor = match cursor {
                    Cursor::None => Cursor::Default { offset: 0 },
                    other => other,
                };
                let res = file.fetch(batch_size, cursor)?;
                Ok(res)
            }
        }
    }

    pub fn format(&self) -> DataFormat {
        self.format
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn dialect(&self) -> Box<dyn Dialect> {
        match self.format {
            DataFormat::MySql => Box::new(dialect::MySql),
            DataFormat::Postgres => Box::new(dialect::Postgres),
            _ => panic!("Unsupported dialect for source"),
        }
    }
}
