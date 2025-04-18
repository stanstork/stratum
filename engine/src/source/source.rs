use super::{data_source::DataSource, linked_source::LinkedSource};
use crate::record::Record;
use sql_adapter::join::source::JoinSource;

/// Represents a migration source,
/// such as a database table, file, or API to be transformed and written to a destination.
#[derive(Clone)]
pub struct Source {
    pub primary: DataSource,
    pub linked: Vec<LinkedSource>,
}

impl Source {
    pub fn new(data_source: DataSource, load: Vec<LinkedSource>) -> Self {
        Source {
            primary: data_source,
            linked: load,
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
                let joins: Vec<JoinSource> = self
                    .linked
                    .iter()
                    .filter_map(|linked| {
                        if let LinkedSource::Table(join) = linked {
                            Some(join.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                db.fetch_data(batch_size, joins, offset).await
            }
            _ => Err("Unsupported primary data source".into()),
        }
    }
}
