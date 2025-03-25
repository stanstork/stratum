use super::db_row::DbRow;
use crate::metadata::column::{
    data_type::ColumnDataType,
    value::{ColumnData, ColumnValue},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowData {
    pub columns: Vec<ColumnData>,
}

impl RowData {
    pub fn new(columns: Vec<ColumnData>) -> Self {
        RowData { columns }
    }

    pub fn from_db_row(row: &DbRow) -> RowData {
        let columns = row
            .columns()
            .iter()
            .map(|column| {
                let column_type =
                    ColumnDataType::try_from(row.column_type(column)).unwrap_or_else(|_| {
                        eprintln!("Unknown column type: {}", row.column_type(column));
                        ColumnDataType::String
                    });

                ColumnData {
                    name: column.to_string(),
                    value: ColumnValue::from_row(row, column_type, column),
                    type_info: column_type,
                }
            })
            .collect();

        RowData { columns }
    }

    pub fn extract_columns(&self, table_name: &str) -> Vec<ColumnData> {
        let prefix = format!("{}_", table_name);
        self.columns
            .iter()
            .filter(|col| col.name.starts_with(&prefix))
            .cloned()
            .map(|mut col| {
                col.name = col.name.replacen(&prefix, "", 1);
                col
            })
            .collect()
    }
}
