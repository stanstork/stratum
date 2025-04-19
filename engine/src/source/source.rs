use super::{data_source::DataSource, linked_source::LinkedSource};
use crate::record::Record;
use smql::statements::connection::DataFormat;
use sql_adapter::join::source::JoinSource;

/// Represents a migration source,
/// such as a database table, file, or API to be transformed and written to a destination.
#[derive(Clone)]
pub struct Source {
    /// Format of the source data (e.g., SQL, CSV, JSON)
    pub format: DataFormat,
    pub primary: DataSource,
    pub linked: Vec<LinkedSource>,
    pub joins: Vec<JoinSource>,
}

impl Source {
    pub fn new(format: DataFormat, primary: DataSource, linked: Vec<LinkedSource>) -> Self {
        let joins = linked
            .iter()
            .filter_map(|linked| {
                if let LinkedSource::Table(join) = linked {
                    Some(join.clone())
                } else {
                    None
                }
            })
            .collect();

        Source {
            format,
            primary,
            linked,
            joins,
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
                db.fetch_data(batch_size, &self.joins, offset).await
            }
            _ => Err("Unsupported primary data source".into()),
        }
    }
}
