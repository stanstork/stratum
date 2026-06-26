use crate::transform::{error::TransformError, pipeline::Transform};
use engine_wasm::{exchange::types::PluginInput, runtime::instance::PluginInstance};
use model::{
    core::value::{FieldValue, Value},
    records::Record,
};
use std::{collections::HashMap, sync::Mutex};

pub struct WasmTransform {
    plugin_name: String,
    plugin: Mutex<PluginInstance>,
    output_column: String,
    input_mapping: HashMap<String, String>,
}

impl WasmTransform {
    pub fn new(
        plugin: PluginInstance,
        output_column: String,
        input_mapping: HashMap<String, String>,
    ) -> Self {
        Self {
            plugin_name: plugin.plugin_name().to_string(),
            plugin: Mutex::new(plugin),
            output_column,
            input_mapping,
        }
    }
}

impl Transform for WasmTransform {
    fn apply(&self, row: &mut Record) -> Result<(), TransformError> {
        let input = PluginInput::from_record(row, &self.input_mapping);
        let output = {
            let mut guard = self.plugin.lock().expect("plugin mutex poisoned");
            guard.call_transform(&input).map_err(|e| {
                TransformError::Transformation(format!(
                    "wasm transform '{}' failed: {}",
                    self.plugin_name, e
                ))
            })?
        };
        update_row(row, &self.output_column, &output.value);
        Ok(())
    }
}

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
