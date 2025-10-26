use crate::{error::MigrationError, filter::Filter};
use chrono::Duration;
use common::record::Record;
use data::DataSource;
use linked::LinkedSource;
use query_builder::{
    dialect::{self, Dialect},
    offsets::{Cursor, OffsetStrategy},
};
use smql::statements::connection::DataFormat;
use tokio_util::sync::CancellationToken;

pub mod data;
pub mod linked;

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
        max_ms: Duration,
        cancel: &CancellationToken,
        cursor: Option<Cursor>,
        start: &dyn OffsetStrategy,
    ) -> Result<Vec<Record>, MigrationError> {
        match &self.primary {
            DataSource::Database(db) => {
                let db = db.lock().await;
                let rows = db
                    .fetch(
                        batch_size,
                        max_ms,
                        cancel,
                        cursor.unwrap_or(Cursor::None),
                        start,
                    )
                    .await?;
                let records = rows.into_iter().map(Record::RowData).collect();
                Ok(records)
            }
            DataSource::File(file) => {
                let mut file = file.lock().await;
                let rows = file.fetch(batch_size)?;
                let records = rows.into_iter().map(Record::RowData).collect();
                Ok(records)
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
