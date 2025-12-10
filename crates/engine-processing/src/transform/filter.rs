use super::pipeline::Filter;
use model::{records::row::RowData, transform::mapping::TransformationMetadata};

/// Filters out rows that have no mapped fields for their table.
pub struct EmptyRowFilter;

impl Filter for EmptyRowFilter {
    fn should_keep(&self, row: &RowData) -> bool {
        !row.field_values.is_empty()
    }
}

/// Filters out rows from tables that have no mappings defined.
pub struct UnmappedTableFilter {
    mapping: TransformationMetadata,
}

impl UnmappedTableFilter {
    pub fn new(mapping: TransformationMetadata) -> Self {
        Self { mapping }
    }

    fn has_any_mapping(&self, table: &str) -> bool {
        // Check if table has entity mapping
        if self.mapping.entities.contains_key(table) {
            return true;
        }

        // Check if table has field mappings
        if let Some(field_renames) = self.mapping.field_mappings.field_renames.get(table) {
            if !field_renames.source_to_target.is_empty() {
                return true;
            }
        }

        // Check if table has computed fields
        if let Some(computed_fields) = self.mapping.field_mappings.computed_fields.get(table) {
            if !computed_fields.is_empty() {
                return true;
            }
        }

        false
    }
}

impl Filter for UnmappedTableFilter {
    fn should_keep(&self, row: &RowData) -> bool {
        self.has_any_mapping(&row.entity)
    }
}

/// Filters rows based on a field value predicate.
/// Example: keep only rows where a specific field meets a condition.
pub struct FieldValueFilter<F>
where
    F: Fn(&RowData) -> bool + Send + Sync,
{
    predicate: F,
}

impl<F> FieldValueFilter<F>
where
    F: Fn(&RowData) -> bool + Send + Sync,
{
    pub fn new(predicate: F) -> Self {
        Self { predicate }
    }
}

impl<F> Filter for FieldValueFilter<F>
where
    F: Fn(&RowData) -> bool + Send + Sync,
{
    fn should_keep(&self, row: &RowData) -> bool {
        (self.predicate)(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::core::{
        data_type::DataType,
        value::{FieldValue, Value},
    };

    #[test]
    fn test_empty_row_filter() {
        let filter = EmptyRowFilter;

        let empty_row = RowData::new("test_table", vec![]);
        assert!(!filter.should_keep(&empty_row));

        let non_empty_row = RowData::new(
            "test_table",
            vec![FieldValue {
                name: "id".to_string(),
                value: Some(Value::Int(1)),
                data_type: DataType::LongLong,
            }],
        );
        assert!(filter.should_keep(&non_empty_row));
    }

    #[test]
    fn test_field_value_filter() {
        let filter = FieldValueFilter::new(|row: &RowData| {
            // Keep rows where 'active' field is true
            row.get("active")
                .and_then(|f| f.value.as_ref())
                .map(|v| matches!(v, Value::Boolean(true)))
                .unwrap_or(false)
        });

        let active_row = RowData::new(
            "users",
            vec![FieldValue {
                name: "active".to_string(),
                value: Some(Value::Boolean(true)),
                data_type: DataType::Boolean,
            }],
        );
        assert!(filter.should_keep(&active_row));

        let inactive_row = RowData::new(
            "users",
            vec![FieldValue {
                name: "active".to_string(),
                value: Some(Value::Boolean(false)),
                data_type: DataType::Boolean,
            }],
        );
        assert!(!filter.should_keep(&inactive_row));
    }
}
