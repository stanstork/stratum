use crate::{core::value::FieldValue, core::value::Value, records::Record};
use std::collections::HashMap;

/// Apply column-type coercions to a row before hashing.
///
/// This ensures the canonical hash matches regardless of whether the value
/// came from the source driver (pre-write) or was re-read from the destination
/// (verify path). Both paths must apply the same coercions.
///
/// `col_types` maps column name -> PostgreSQL type string (e.g. `"text[]"`, `"integer"`).
pub fn coerce_row_for_hash(row: &Record, col_types: &HashMap<String, String>) -> Record {
    let fields: Vec<FieldValue> = row
        .fields
        .iter()
        .map(|fv| {
            let coerced = fv.value.as_ref().map(|value| {
                let pg_type = col_types.get(&fv.name).map(|s| s.as_str()).unwrap_or("");
                coerce_value_for_hash(value.clone(), pg_type)
            });
            FieldValue {
                name: fv.name.clone(),
                value: coerced,
                data_type: fv.data_type.clone(),
            }
        })
        .collect();
    Record {
        schema: row.schema.clone(),
        fields,
    }
}

/// Coerce a single value to match what the COPY writer would write and PG would store.
fn coerce_value_for_hash(value: Value, pg_type: &str) -> Value {
    let pg_type_lc = pg_type.to_lowercase();
    if (pg_type_lc.ends_with("[]") || pg_type_lc.contains("array") || pg_type_lc == "set")
        && let Value::String(s) = &value
    {
        let elements: Vec<Value> = s
            .split(',')
            .map(|item| Value::String(item.trim_matches('"').trim_matches('\'').to_string()))
            .collect();
        return Value::Array(elements);
    }
    value
}
