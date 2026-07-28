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
        let Some(computed_fields) = self.mapping.field_mappings.computed_fields.get(&row.schema)
        else {
            return Ok(());
        };

        // Reserve once so appending the computed columns doesn't reallocate the
        // field vector on every row.
        row.fields.reserve(computed_fields.len());

        let env = &self.env;
        let env_getter = |key: &str| env.get(key);

        for computed in computed_fields {
            match computed
                .expression
                .evaluate(row, &self.mapping, &env_getter)
            {
                Some(value) => update_row(row, &computed.name, value),
                None => {
                    return Err(TransformError::Transformation(format!(
                        "Failed to evaluate computed column `{}` in `{}`",
                        computed.name, row.schema
                    )));
                }
            }
        }
        Ok(())
    }
}

/// Store a freshly computed value into `column`,
/// overwriting an existing field or appending a new one.
#[inline]
fn update_row(row: &mut Record, column: &str, column_value: Value) {
    if let Some(col) = row
        .fields
        .iter_mut()
        .find(|col| col.name == column || col.name.eq_ignore_ascii_case(column))
    {
        col.value = Some(column_value);
    } else {
        let data_type = column_value.data_type();
        row.fields.push(FieldValue {
            name: column.to_owned(),
            value: Some(column_value),
            data_type,
        });
    }
}
