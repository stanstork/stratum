use super::pipeline::Filter;
use model::{records::Record, transform::mapping::TransformationMetadata};
use std::collections::HashSet;

/// Filters out rows that have no mapped fields for their table.
pub struct EmptyRowFilter;

impl Filter for EmptyRowFilter {
    fn should_keep(&self, row: &Record) -> bool {
        !row.is_empty()
    }
}

/// Filters out rows from tables that have no mappings defined.
pub struct UnmappedTableFilter {
    mapped: HashSet<String>,
}

impl UnmappedTableFilter {
    pub fn new(mapping: TransformationMetadata) -> Self {
        let mut mapped = HashSet::new();

        // Tables that are a mapping target.
        mapped.extend(mapping.entities.target_to_source.keys().cloned());

        // Tables that have at least one field rename.
        for (table, renames) in &mapping.field_mappings.field_renames {
            if !renames.source_to_target.is_empty() {
                mapped.insert(table.clone());
            }
        }

        // Tables that have at least one computed field.
        for (table, computed) in &mapping.field_mappings.computed_fields {
            if !computed.is_empty() {
                mapped.insert(table.clone());
            }
        }

        Self { mapped }
    }
}

impl Filter for UnmappedTableFilter {
    fn should_keep(&self, row: &Record) -> bool {
        self.mapped.contains(row.table())
    }
}

/// Filters rows based on a field value predicate.
/// Example: keep only rows where a specific field meets a condition.
pub struct FieldValueFilter<F>
where
    F: Fn(&Record) -> bool + Send + Sync,
{
    predicate: F,
}

impl<F> FieldValueFilter<F>
where
    F: Fn(&Record) -> bool + Send + Sync,
{
    pub fn new(predicate: F) -> Self {
        Self { predicate }
    }
}

impl<F> Filter for FieldValueFilter<F>
where
    F: Fn(&Record) -> bool + Send + Sync,
{
    fn should_keep(&self, row: &Record) -> bool {
        (self.predicate)(row)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use model::{
        core::{
            types::{IntSize, Type},
            value::{FieldValue, Value},
        },
        records::OpType,
    };

    #[test]
    fn unmapped_table_filter_keeps_mapped_drops_unmapped() {
        use model::transform::mapping::{
            FieldTransformations, NameResolver, TransformationMetadata,
        };
        use std::collections::{HashMap, HashSet};

        // `users` is an entity target; `orders` has a field rename; `audit_log`
        // has nothing.
        let entities =
            NameResolver::new(HashMap::from([("users".to_string(), "users".to_string())]));
        let mut field_mappings = FieldTransformations::new();
        field_mappings.add_mapping(
            "orders",
            HashMap::from([("total".to_string(), "amount".to_string())]),
        );

        let filter = UnmappedTableFilter::new(TransformationMetadata {
            entities,
            field_mappings,
            foreign_fields: HashMap::new(),
            plugin_columns: Vec::new(),
            migrated_tables: HashSet::new(),
            has_projection: false,
        });

        let row = |t: &str| Record::from_fields(t, vec![], OpType::default());
        assert!(filter.should_keep(&row("users")), "entity target kept");
        assert!(
            filter.should_keep(&row("orders")),
            "field-renamed table kept"
        );
        assert!(
            !filter.should_keep(&row("audit_log")),
            "unmapped table dropped"
        );
    }

    #[test]
    fn test_empty_row_filter() {
        let filter = EmptyRowFilter;

        let empty_row = Record::from_fields("test_table", vec![], OpType::default());
        assert!(!filter.should_keep(&empty_row));

        let non_empty_row = Record::from_fields(
            "test_table",
            vec![FieldValue {
                name: "id".to_string(),
                value: Some(Value::Int(1)),
                data_type: Type::Int {
                    bits: IntSize::I64,
                    unsigned: false,
                    auto_increment: false,
                },
            }],
            OpType::default(),
        );
        assert!(filter.should_keep(&non_empty_row));
    }

    #[test]
    fn test_field_value_filter() {
        let filter = FieldValueFilter::new(|row: &Record| {
            // Keep rows where 'active' field is true
            row.value("active")
                .map(|v| matches!(v, Value::Boolean(true)))
                .unwrap_or(false)
        });

        let active_row = Record::from_fields(
            "users",
            vec![FieldValue {
                name: "active".to_string(),
                value: Some(Value::Boolean(true)),
                data_type: Type::Boolean,
            }],
            OpType::default(),
        );
        assert!(filter.should_keep(&active_row));

        let inactive_row = Record::from_fields(
            "users",
            vec![FieldValue {
                name: "active".to_string(),
                value: Some(Value::Boolean(false)),
                data_type: Type::Boolean,
            }],
            OpType::default(),
        );
        assert!(!filter.should_keep(&inactive_row));
    }
}
