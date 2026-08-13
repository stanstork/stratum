use crate::core::{
    types::Type,
    value::{FieldValue, Value},
};
use std::collections::HashMap;
use std::sync::Arc;

pub mod batch;
pub mod schema;

pub use schema::{RecordSchema, SchemaColumn};

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize, PartialEq, Eq, Default)]
pub enum OpType {
    #[default]
    Insert,
    Update,
    Delete,
}

/// A single row: a shared column [`RecordSchema`] plus this row's positional values.
#[derive(Debug, Clone)]
pub struct Record {
    schema: Arc<RecordSchema>,
    /// Aligned with `schema.columns()`. `None` is SQL `NULL`.
    values: Vec<Option<Value>>,
    pub op_type: OpType,
}

/// A borrowed view of one column of a [`Record`], for iteration.
pub struct FieldRef<'a> {
    pub name: &'a str,
    pub value: Option<&'a Value>,
    pub data_type: &'a Type,
}

impl Record {
    /// Construct from a shared schema and positional values.
    pub fn new(schema: Arc<RecordSchema>, values: Vec<Option<Value>>, op_type: OpType) -> Self {
        debug_assert_eq!(schema.len(), values.len(), "values must align with schema");

        Record {
            schema,
            values,
            op_type,
        }
    }

    /// Construct from a list of named fields, building a fresh schema.
    pub fn from_fields(table: &str, fields: Vec<FieldValue>, op_type: OpType) -> Self {
        let mut columns = Vec::with_capacity(fields.len());
        let mut values = Vec::with_capacity(fields.len());

        for f in fields {
            columns.push(SchemaColumn::new(f.name, f.data_type));
            values.push(f.value);
        }

        Record {
            schema: RecordSchema::new(table, columns),
            values,
            op_type,
        }
    }

    pub fn schema(&self) -> &Arc<RecordSchema> {
        &self.schema
    }

    /// The source/table name this row belongs to.
    pub fn table(&self) -> &str {
        self.schema.table()
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Position of a column by (case-insensitive) name.
    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.schema.index_of(name)
    }

    /// Value at a position, if present and non-null.
    pub fn value_at(&self, i: usize) -> Option<&Value> {
        self.values.get(i).and_then(|v| v.as_ref())
    }

    /// Value of a named column, if present and non-null.
    pub fn value(&self, name: &str) -> Option<&Value> {
        self.index_of(name).and_then(|i| self.value_at(i))
    }

    /// Value of a named column, cloned; missing/null → [`Value::Null`].
    pub fn get_value(&self, field: &str) -> Value {
        self.value(field).cloned().unwrap_or(Value::Null)
    }

    /// Iterate columns as `(name, value, type)` views.
    pub fn iter(&self) -> impl Iterator<Item = FieldRef<'_>> {
        self.schema
            .columns()
            .iter()
            .zip(self.values.iter())
            .map(|(c, v)| FieldRef {
                name: &c.name,
                value: v.as_ref(),
                data_type: &c.data_type,
            })
    }

    /// Replace this row's schema (e.g. after a batch-wide rename). The new schema
    /// must have the same column count/order as the current values.
    pub fn set_schema(&mut self, schema: Arc<RecordSchema>) {
        debug_assert_eq!(schema.len(), self.values.len());
        self.schema = schema;
    }

    /// Restamp this row with a different source/table name (same columns).
    pub fn set_table(&mut self, table: &str) {
        if self.schema.table() != table {
            self.schema = self.schema.with_table(table);
        }
    }

    /// Overwrite the value at a position (no-op if out of range).
    pub fn set_value_at(&mut self, i: usize, value: Option<Value>) {
        if let Some(slot) = self.values.get_mut(i) {
            *slot = value;
        }
    }

    /// Append a value, keeping it aligned with a schema that already gained the
    /// matching column (the COW-memoized computed-column path).
    pub fn push_value(&mut self, value: Option<Value>) {
        self.values.push(value);
    }

    /// Add a brand-new column + value to this row, deriving a new schema.
    pub fn push_column(&mut self, name: &str, data_type: Type, value: Option<Value>) {
        self.schema = self
            .schema
            .with_appended(SchemaColumn::new(name, data_type));
        self.values.push(value);
    }

    /// Keep only the columns at `kept` positions (in order), under `schema`.
    pub fn project(&mut self, schema: Arc<RecordSchema>, kept: &[usize]) {
        debug_assert_eq!(schema.len(), kept.len());
        debug_assert!(
            kept.windows(2).all(|w| w[0] < w[1]),
            "project requires strictly-ascending kept indices"
        );

        if kept.last().is_none_or(|&i| i < self.values.len()) {
            for (w, &i) in kept.iter().enumerate() {
                if w != i {
                    self.values.swap(w, i);
                }
            }
            self.values.truncate(kept.len());
        } else {
            // Out-of-range index (shouldn't happen): tolerant fallback.
            let mut old = std::mem::take(&mut self.values);
            self.values = kept
                .iter()
                .map(|&i| old.get_mut(i).and_then(Option::take))
                .collect();
        }

        self.schema = schema;
    }

    pub fn size_bytes(&self) -> usize {
        let mut size = self.schema.table().len();
        for (c, v) in self.schema.columns().iter().zip(self.values.iter()) {
            size += c.name.len();
            size += v.as_ref().map_or(0, |v| v.size_bytes());
        }
        size
    }

    pub fn to_map(&self) -> HashMap<String, Value> {
        self.iter()
            .filter_map(|f| f.value.map(|v| (f.name.to_string(), v.clone())))
            .collect()
    }
}

#[cfg(test)]
mod project_tests {
    use super::*;
    use crate::core::value::Value;

    /// Build a row of `Int` columns c0..cN.
    fn row(vals: &[i64]) -> Record {
        let cols: Vec<SchemaColumn> = (0..vals.len())
            .map(|i| SchemaColumn::new(format!("c{i}"), Value::Int(0).data_type()))
            .collect();
        let schema = RecordSchema::new("t", cols);
        let values = vals.iter().map(|v| Some(Value::Int(*v))).collect();
        Record::new(schema, values, OpType::Insert)
    }

    fn project_to(vals: &[i64], kept: &[usize]) -> Vec<i64> {
        let mut r = row(vals);
        let cols: Vec<SchemaColumn> = kept
            .iter()
            .map(|&i| SchemaColumn::new(format!("c{i}"), Value::Int(0).data_type()))
            .collect();
        r.project(RecordSchema::new("t", cols), kept);
        (0..kept.len())
            .map(|i| match r.value_at(i) {
                Some(Value::Int(v)) => *v,
                other => panic!("slot {i} = {other:?}"),
            })
            .collect()
    }

    #[test]
    fn project_compacts_in_place_correctly() {
        let v = [0, 1, 2, 3, 4];
        assert_eq!(project_to(&v, &[0, 1, 2, 3, 4]), vec![0, 1, 2, 3, 4]); // keep all
        assert_eq!(project_to(&v, &[1, 2]), vec![1, 2]); // drop leading
        assert_eq!(project_to(&v, &[0, 2, 4]), vec![0, 2, 4]); // drop alternating
        assert_eq!(project_to(&v, &[2, 3]), vec![2, 3]); // drop head, keep middle
        assert_eq!(project_to(&v, &[4]), vec![4]); // keep only the last
        assert_eq!(project_to(&v, &[0]), vec![0]); // keep only the first
        assert_eq!(project_to(&v, &[]), Vec::<i64>::new()); // drop all
        assert_eq!(project_to(&[7, 8, 9], &[2]), vec![9]); // keep last of 3
    }
}
