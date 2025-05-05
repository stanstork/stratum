use super::{data_source::DataSource, linked_source::LinkedSource};
use crate::{filter::filter::Filter, record::Record};
use smql_v02::statements::connection::DataFormat;

/// Represents a migration source,
/// such as a database table, file, or API to be transformed and written to a destination.
#[derive(Clone)]
pub struct Source {
    /// Format of the source data (e.g., SQL, CSV, JSON)
    pub format: DataFormat,
    pub name: String,
    pub primary: DataSource,
    pub linked: Vec<LinkedSource>,
    pub filter: Option<Filter>,
}

impl Source {
    pub fn new(
        format: DataFormat,
        name: String,
        primary: DataSource,
        linked: Vec<LinkedSource>,
        filter: Option<Filter>,
    ) -> Self {
        Source {
            format,
            name,
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
