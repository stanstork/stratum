use crate::metadata::column::value::ColumnData;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowData {
    pub columns: Vec<ColumnData>,
}

pub enum DbRow<'a> {
    MySqlRow(&'a sqlx::mysql::MySqlRow),
    PostgresRow(&'a sqlx::postgres::PgRow),
}
