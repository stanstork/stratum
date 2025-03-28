use super::db_row::DbRow;
use crate::metadata::column::{
    data_type::ColumnDataType,
    value::{ColumnData, ColumnValue},
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowData {
    pub table: String,
    pub columns: Vec<ColumnData>,
}

impl RowData {
    pub fn new(table_name: &str, columns: Vec<ColumnData>) -> Self {
        RowData {
            table: table_name.to_string(),
            columns,
        }
    }

    pub fn from_db_row(table_name: &str, row: &DbRow) -> RowData {
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

        RowData::new(table_name, columns)
    }
}
