use super::{data_source::DataSource, load::LoadSource};
use crate::record::Record;

/// Represents a source of data, which can be either a database or a file.
#[derive(Clone)]
pub struct Source {
    pub data_source: DataSource,
    pub load_source: Option<LoadSource>,
}

impl Source {
    /// Creates a new `Source` instance from a `DataSource` and a `LoadSource`.
    pub fn new(data_source: DataSource, load_source: Option<LoadSource>) -> Self {
        Source {
            data_source,
            load_source,
        }
    }

    /// Returns the data source.
    pub fn data_source(&self) -> &DataSource {
        &self.data_source
    }

    /// Returns the load source.
    pub fn load_source(&self) -> &Option<LoadSource> {
        &self.load_source
    }

    pub async fn fetch_data(
        &self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
        if let DataSource::Database(db) = &self.data_source {
            let db = db.lock().await;
            let join = self.load_source.as_ref().and_then(|load| match load {
                LoadSource::TableJoin(join) => Some(join.clone()),
                _ => None,
            });

            db.fetch_data(batch_size, join, offset).await
        } else {
            Err("Unsupported data source".into())
        }
    }
}
