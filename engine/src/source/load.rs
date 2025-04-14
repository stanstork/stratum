use smql::statements::load::Load;

#[derive(Debug, Clone)]
pub enum LoadSource {
    TableJoin {
        alias: String,
        left: String,
        right: String,
        join_columns: Vec<(String, String)>,
    },
    File {
        path: String,
        format: String,
    },
}

impl From<Load> for LoadSource {
    fn from(value: Load) -> Self {
        LoadSource::TableJoin {
            alias: value.name,
            left: value.source,
            right: value.join,
            join_columns: value.mappings,
        }
    }
}
