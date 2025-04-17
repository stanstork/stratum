use super::{data_source::DataSource, load::LoadSource};
use crate::record::Record;

/// Represents a source of data, which can be either a database or a file.
#[derive(Clone)]
pub struct Source {
    pub data_source: DataSource,
    pub load_sources: Vec<LoadSource>,
}

impl Source {
    /// Creates a new `Source` instance from a `DataSource` and a `LoadSource`.
    pub fn new(data_source: DataSource, load_source: Vec<LoadSource>) -> Self {
        Source {
            data_source,
            load_sources: load_source,
        }
    }

    /// Returns the data source.
    pub fn data_source(&self) -> &DataSource {
        &self.data_source
    }

    /// Returns the load source.
    pub fn load_source(&self) -> &Vec<LoadSource> {
        &self.load_sources
    }

    pub async fn fetch_data(
        &self,
        batch_size: usize,
        offset: Option<usize>,
    ) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
        if let DataSource::Database(db) = &self.data_source {
            let db = db.lock().await;
            let mut joins = Vec::new();

            // Iterate over load sources and collect joins
            for load in &self.load_sources {
                if let LoadSource::TableJoin(join) = load {
                    joins.push(join.clone());
                }
            }

            db.fetch_data(batch_size, joins, offset).await
        } else {
            Err("Unsupported data source".into())
        }
    }
}
