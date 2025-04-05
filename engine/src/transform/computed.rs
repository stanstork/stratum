use super::pipeline::Transform;
use crate::{expr::eval::Evaluator, record::Record};
use common::computed::ComputedField;
use sql_adapter::metadata::column::{data_type::ColumnDataType, value::ColumnData};
use std::collections::HashMap;
use tracing::warn;

pub struct ComputedTransform {
    computed: HashMap<String, Vec<ComputedField>>,
}

impl ComputedTransform {
    pub fn new(computed: HashMap<String, Vec<ComputedField>>) -> Self {
        Self { computed }
    }
}

impl Transform for ComputedTransform {
    fn apply(&self, record: &Record) -> Record {
        match record {
            Record::RowData(row) => {
                let mut row = row.clone();
                let table = row.table.clone();

                if let Some(computed_fields) = self.computed.get(&table) {
                    for computed in computed_fields {
                        if let Some(value) = computed.expression.evaluate(&row) {
                            row.columns.push(ColumnData {
                                name: computed.name.clone(),
                                value: Some(value),
                            });
                        } else {
                            warn!(
                                "Failed to evaluate computed column `{}` in `{}`",
                                computed.name, table
                            );
                        }
                    }
                }
                Record::RowData(row.clone())
            }
        }
    }
}
