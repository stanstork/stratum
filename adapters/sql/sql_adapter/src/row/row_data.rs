use super::db_row::DbRow;
use crate::metadata::column::metadata::from_row;
use common::{record::DataRecord, types::DataType, value::FieldValue};
use serde::{Deserialize, Serialize};
use tracing::warn;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowData {
    pub table: String,
    pub columns: Vec<FieldValue>,
}

impl RowData {
    pub fn new(table_name: &str, columns: Vec<FieldValue>) -> Self {
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
                    DataType::try_from(row.column_type(column)).unwrap_or_else(|_| {
                        warn!("Unknown column type: {}", row.column_type(column));
                        DataType::String
                    });

                FieldValue {
                    name: column.to_string(),
                    value: from_row(row, column_type, column),
                }
            })
            .collect();

        RowData::new(table_name, columns)
    }

    pub fn get(&self, column_name: &str) -> Option<&FieldValue> {
        self.columns
            .iter()
            .find(|col| col.name.eq_ignore_ascii_case(column_name))
    }
}

impl DataRecord for RowData {
    fn debug(&self) {
        println!("{:#?}", self);
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(self).unwrap_or_else(|_| {
            panic!("Failed to serialize: {:?}", self);
        })
    }

    fn deserialize(data: Vec<u8>) -> Self {
        serde_json::from_slice(&data).unwrap_or_else(|_| {
            panic!("Failed to deserialize: {:?}", data);
        })
    }
}
