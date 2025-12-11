use super::pipeline::Transform;
use crate::error::TransformError;
use engine_core::context::env::get_env;
use expression_engine::Evaluator;
use model::{
    core::value::{FieldValue, Value},
    records::row::RowData,
    transform::mapping::TransformationMetadata,
};

pub struct ComputedTransform {
    mapping: TransformationMetadata,
}

impl ComputedTransform {
    pub fn new(mapping: TransformationMetadata) -> Self {
        Self { mapping }
    }
}

impl Transform for ComputedTransform {
    fn apply(&self, row: &mut RowData) -> Result<(), TransformError> {
        let table = row.entity.clone();

        if let Some(computed_fields) = self.mapping.field_mappings.computed_fields.get(&table) {
            for computed in computed_fields {
                if let Some(value) = computed.expression.evaluate(row, &self.mapping, get_env) {
                    update_row(row, &computed.name, &value);
                } else {
                    return Err(TransformError::Transformation(format!(
                        "Failed to evaluate computed column `{}` in `{}`",
                        computed.name, table
                    )));
                }
            }
        }
        Ok(())
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
            data_type: column_value.data_type(),
        });
    }
}
