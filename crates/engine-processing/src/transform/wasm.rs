use crate::{
    profile,
    transform::{error::TransformError, pipeline::Transform},
};
use engine_wasm::{exchange::types::PluginInput, runtime::instance::PluginInstance};
use model::{
    core::value::Value,
    records::{Record, RecordSchema, SchemaColumn},
};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Instant,
};

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

/// Where a plugin's output value goes.
enum Target {
    /// Overwrite the column already at this position.
    At(usize),
    /// Append a new column; all rows share this one appended schema.
    Append(Arc<RecordSchema>),
}

impl Transform for WasmTransform {
    fn kind(&self) -> &'static str {
        "wasm"
    }

    // The plugin ABI is batch-native. A single row is just a one-element batch.
    fn apply(&self, row: &mut Record) -> Result<(), TransformError> {
        let input = PluginInput::from_record(row, &self.input_mapping);

        let outputs = {
            let mut guard = self.plugin.lock().expect("plugin mutex poisoned");
            guard.call_transform(&[input]).map_err(|e| {
                TransformError::Transformation(format!(
                    "wasm transform '{}' failed: {}",
                    self.plugin_name, e
                ))
            })?
        };

        if let Some(output) = outputs.into_iter().next() {
            update_row(row, &self.output_column, &output.value);
        }

        Ok(())
    }

    fn apply_batch(&self, rows: &mut [Record], failures: &mut Vec<(usize, TransformError)>) {
        let result = {
            let mut guard = self.plugin.lock().expect("plugin mutex poisoned");
            let t = Instant::now();
            let r = guard.call_transform_records(rows, &self.input_mapping);
            profile::record(&profile::PLUGIN_CALL, t.elapsed());
            r
        };

        match result {
            Ok(outputs) => {
                let mut outputs_iter = outputs.into_iter().peekable();

                // Extract the first row and first output to determine the schema plan.
                // If either is missing, there's nothing to write.
                let (Some(first_row), Some(first_out)) = (rows.first(), outputs_iter.peek()) else {
                    return;
                };

                // Resolve where the output column goes ONCE for the whole run.
                let target = match first_row.index_of(&self.output_column) {
                    Some(i) => Target::At(i),
                    None => Target::Append(first_row.schema().with_appended(SchemaColumn::new(
                        self.output_column.as_str(),
                        first_out.value.data_type(),
                    ))),
                };

                for (row, output) in rows.iter_mut().zip(outputs_iter) {
                    match &target {
                        Target::At(i) => row.set_value_at(*i, Some(output.value)),
                        Target::Append(schema) => {
                            row.push_value(Some(output.value));
                            row.set_schema(Arc::clone(schema));
                        }
                    }
                }
            }
            // A batch call is all-or-nothing: on failure every row in the batch is marked failed.
            Err(e) => {
                let msg = format!("wasm transform '{}' batch failed: {}", self.plugin_name, e);
                failures.extend(
                    (0..rows.len()).map(|i| (i, TransformError::Transformation(msg.clone()))),
                );
            }
        }
    }
}

fn update_row(row: &mut Record, column: &str, column_value: &Value) {
    match row.index_of(column) {
        Some(i) => row.set_value_at(i, Some(column_value.clone())),
        None => row.push_column(column, column_value.data_type(), Some(column_value.clone())),
    }
}
