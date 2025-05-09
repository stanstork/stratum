use crate::{filter::Filter, record::Record};
use data::DataSource;
use linked::LinkedSource;
use smql::statements::connection::DataFormat;

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
        offset: Option<usize>,
    ) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
        match &self.primary {
            DataSource::Database(db) => {
                let db = db.lock().await;
                let rows = db.fetch(batch_size, offset).await?;
                let records = rows.into_iter().map(Record::RowData).collect();
                Ok(records)
            }
        }
    }
}
