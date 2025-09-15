use super::pipeline::Transform;
use crate::expr::eval::Evaluator;
use common::{
    mapping::EntityMapping,
    record::Record,
    row_data::RowData,
    value::{FieldValue, Value},
};
use smql::statements::expr::Expression;
use tracing::warn;

pub struct ComputedTransform {
    mapping: EntityMapping,
}

impl ComputedTransform {
    pub fn new(mapping: EntityMapping) -> Self {
        Self { mapping }
    }
}

impl Transform for ComputedTransform {
    fn apply(&self, record: &Record) -> Record {
        match record {
            Record::RowData(row) => {
                let mut row = row.clone();
                let table = row.entity.clone();

                if let Some(computed_fields) =
                    self.mapping.field_mappings.computed_fields.get(&table)
                {
                    for computed in computed_fields {
                        if let Some(value) = computed.expression.evaluate(&row, &self.mapping) {
                            if let Expression::Lookup { .. } = computed.expression {
                                // Skip lookup expressions as they are handled during data loading
                                continue;
                            }
                            update_row(&mut row, &computed.name, &value);
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

// TODO: Optimize this function to avoid searching for the column multiple times
// and to handle the case where the column is not found.
fn update_row(row: &mut RowData, column: &str, column_value: &Value) {
    if let Some(col) = row
        .field_values
        .iter_mut()
        .find(|col| col.name.eq_ignore_ascii_case(column))
    {
        col.value = Some(column_value.clone());
    } else {
        row.field_values.push(FieldValue {
            name: column.to_string(),
            value: Some(column_value.clone()),
            data_type: column_value.get_data_type(),
        });
    }
}
