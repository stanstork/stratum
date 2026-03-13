use super::pipeline::Transform;
use crate::transform::error::TransformError;
use engine_core::context::env::EnvContext;
use expression_engine::Evaluator;
use model::{
    core::value::{FieldValue, Value},
    records::Record,
    transform::mapping::TransformationMetadata,
};
use std::sync::Arc;

pub struct ComputedTransform {
    mapping: TransformationMetadata,
    env: Arc<EnvContext>,
}

impl ComputedTransform {
    pub fn new(mapping: TransformationMetadata, env: Arc<EnvContext>) -> Self {
        Self { mapping, env }
    }
}

impl Transform for ComputedTransform {
    fn apply(&self, row: &mut Record) -> Result<(), TransformError> {
        let table = row.schema.clone();
        let env = self.env.clone();
        let env_getter = move |key: &str| env.get(key);
        if let Some(computed_fields) = self.mapping.field_mappings.computed_fields.get(&table) {
            for computed in computed_fields {
                if let Some(value) = computed
                    .expression
                    .evaluate(row, &self.mapping, &env_getter)
                {
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
fn update_row(row: &mut Record, column: &str, column_value: &Value) {
    if let Some(col) = row
        .fields
        .iter_mut()
        .find(|col| col.name.eq_ignore_ascii_case(column))
    {
        col.value = Some(column_value.clone());
    } else {
        row.fields.push(FieldValue {
            name: column.to_string(),
            value: Some(column_value.clone()),
            data_type: column_value.data_type(),
        });
    }
}
