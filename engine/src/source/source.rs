use super::{data_source::DataSource, load::LoadSource};

/// Represents a source of data, which can be either a database or a file.
#[derive(Clone)]
pub struct Source {
    pub data_source: DataSource,
    pub load_source: LoadSource,
}
