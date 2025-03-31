use crate::{record::Record, transform::pipeline::Transform};
use async_trait::async_trait;
use sql_adapter::metadata::column::value::ColumnValue;
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

#[async_trait]
impl Transform for UpperCaseFunction {
    fn apply(&self, record: &Record) -> Record {
        let mut record = match record {
            Record::RowData(row) => row.clone(),
        };
        for column in &mut record.columns {
            if self.columns.contains(&column.name) {
                if let Some(value) = &mut column.value {
                    if let ColumnValue::String(ref mut text) = value {
                        *text = text.to_uppercase();
                    }
                }
            }
        }
        Record::RowData(record)
    }
}
