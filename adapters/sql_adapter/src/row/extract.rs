use super::row::{DbRow, RowData};
use crate::metadata::column::{
    data_type::ColumnDataType,
    value::{ColumnData, ColumnValue},
};
pub struct RowExtractor;

impl RowExtractor {
    pub fn from_row(row: &DbRow) -> RowData {
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
}
