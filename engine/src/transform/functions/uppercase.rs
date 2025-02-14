use crate::{
    database::row::RowData, metadata::column::ColumnValue, transform::pipeline::Transform,
};
use std::collections::HashSet;

pub struct UpperCaseFunction {
    columns: HashSet<String>,
}

impl UpperCaseFunction {
    pub fn new(columns: Vec<String>) -> Self {
        Self {
            columns: columns.into_iter().collect(),
        }
    }
}

impl Transform for UpperCaseFunction {
    fn apply(&self, row: &RowData) -> RowData {
        let mut row = row.clone();
        for column in &mut row.columns {
            if self.columns.contains(&column.name) {
                if let Some(value) = &mut column.value {
                    if let ColumnValue::String(ref mut text) = value {
                        *text = text.to_uppercase();
                    }
                }
            }
        }
        row
    }
}
