use crate::{
    report::finding::{Finding, FindingKind},
    settings::validated::ValidatedSettings,
    validation::key::{KeyCheckPolicy, KeyChecker},
};
use connectors::sql::base::{
    error::DbError,
    metadata::{column::ColumnMetadata, provider::MetadataProvider, table::TableMetadata},
};
use engine_core::connectors::destination::Destination;
use model::{
    core::value::{FieldValue, Value},
    records::row::RowData,
    transform::mapping::TransformationMetadata,
};
use crate::settings::CopyColumns;
use std::collections::{HashMap, HashSet};

#[derive(Clone, Copy, Debug)]
enum TablePolicy {
    /// Destination table must already exist.
    RequireExisting,
    /// Destination table may be created. When creating, how do we treat columns?
    AllowCreate(NewTableCreation),
}

#[derive(Clone, Copy, Debug)]
enum NewTableCreation {
    /// Create with all columns present in transformed rows.
    CopyAll,
    /// Create only mapped/computed columns.
    MapOnly,
}

#[derive(Clone, Copy, Debug)]
enum ColumnPolicy {
    /// Columns must already exist in destination.
    RequireExisting,
    /// Missing columns may be created, but only when they're mapped/computed (not arbitrary).
    AllowCreateIfPlanned,
}

pub struct DestinationSchemaValidator {
    mapping: TransformationMetadata,
    schemas: HashMap<String, TableMetadata>,
    findings: HashSet<Finding>,

    table_policy: TablePolicy,
    column_policy: ColumnPolicy,

    key_policy: KeyCheckPolicy,
    key_checker: KeyChecker,
}

impl DestinationSchemaValidator {
    pub async fn new(
        destination: &Destination,
        mapping: TransformationMetadata,
        settings: &ValidatedSettings,
    ) -> Result<Self, DbError> {
        let adapter = destination.data_dest.adapter().await;
        let tables = [destination.name()];
        let meta_graph = MetadataProvider::build_metadata_graph(&*adapter, &tables).await?;

        let table_policy = if settings.create_missing_tables {
            match settings.copy_columns {
                CopyColumns::All => TablePolicy::AllowCreate(NewTableCreation::CopyAll),
                CopyColumns::MapOnly => TablePolicy::AllowCreate(NewTableCreation::MapOnly),
            }
        } else {
            TablePolicy::RequireExisting
        };

        let column_policy = if settings.create_missing_columns {
            ColumnPolicy::AllowCreateIfPlanned
        } else {
            ColumnPolicy::RequireExisting
        };

        let key_policy = KeyCheckPolicy::IntraBatchAndDestination { batch_size: 10 }; // TODO: make configurable

        Ok(Self {
            mapping,
            schemas: meta_graph,
            findings: HashSet::new(),
            table_policy,
            column_policy,
            key_policy,
            key_checker: KeyChecker::new(),
        })
    }

    pub fn validate(&mut self, row: &RowData) {
        let table_meta = self.schemas.get(&row.entity).cloned();
        match table_meta {
            Some(table_meta) if !table_meta.columns.is_empty() => {
                let table_name = row.entity.as_str();
                self.validate_existing_table(table_name, &table_meta, row);
                self.key_checker.record_row(
                    table_name,
                    &table_meta,
                    row,
                    self.key_policy,
                    &mut self.findings,
                );
            }
            _ => self.validate_missing_table(&row.entity, row),
        }
    }

    pub async fn validate_pending_keys(
        &mut self,
        destination: &Destination,
    ) -> Result<(), DbError> {
        if matches!(self.key_policy, KeyCheckPolicy::None) {
            return Ok(());
        }
        self.key_checker
            .check_pending(destination, 10, &mut self.findings)
            .await
    }

    pub fn findings(&self) -> Vec<Finding> {
        self.findings.iter().cloned().collect()
    }

    fn validate_existing_table(
        &mut self,
        table_name: &str,
        table_meta: &TableMetadata,
        row: &RowData,
    ) {
        // Validate every field we're writing
        for field in &row.field_values {
            self.validate_field(table_name, table_meta, field);
        }

        // Ensure required destination columns are present in row
        self.validate_required_columns(table_name, table_meta, row);
    }

    fn validate_field(&mut self, table_name: &str, table_meta: &TableMetadata, field: &FieldValue) {
        let name = &field.name;
        match table_meta.columns.get(name) {
            Some(col) => {
                self.check_nullability(table_name, name, col, field);
                self.check_type_compatibility(table_name, name, col, field);
                self.check_truncation(table_name, name, col, field);
                // TODO: numeric precision/scale, enums, etc.
            }
            None => self.handle_missing_column(table_name, name),
        }
    }

    fn check_nullability(
        &mut self,
        table_name: &str,
        field_name: &str,
        col: &ColumnMetadata,
        field: &FieldValue,
    ) {
        if !col.is_nullable && field.value.is_none() {
            self.findings.insert(Finding::error(
                "SCHEMA_NULL_VIOLATION",
                &format!(
                    "Field '{field_name}' in table '{table_name}' is null, but the destination column is not nullable."
                ),
                FindingKind::SampleData,
            ));
        }
    }

    fn check_type_compatibility(
        &mut self,
        table_name: &str,
        field_name: &str,
        col: &ColumnMetadata,
        field: &FieldValue,
    ) {
        let Some(transformed_ty) = field.value_data_type() else {
            return;
        };

        if !col.data_type.is_compatible(&transformed_ty) {
            self.findings.insert(Finding::error(
                "SCHEMA_TYPE_MISMATCH",
                &format!(
                    "Type mismatch for column '{}' in table '{}'. Transformed data has type {}, but destination expects {:?}.",
                    field_name, table_name, transformed_ty, col.data_type
                ),               FindingKind::SampleData,
            ));
        }
    }

    fn check_truncation(
        &mut self,
        table_name: &str,
        field_name: &str,
        col: &ColumnMetadata,
        field: &FieldValue,
    ) {
        let Some(max_len) = col.char_max_length else {
            return;
        };
        let Some(actual_len) = self.field_len(field) else {
            return;
        };
        if actual_len > max_len {
            self.findings.insert(Finding::warning(
                "SCHEMA_TRUNCATION_RISK",
                &format!(
                    "Data for column '{field_name}' in table '{table_name}' has length {actual_len} which exceeds the destination column limit of {max_len}. Data may be truncated."
                ), FindingKind::SampleData
            ));
        }
    }

    fn field_len(&self, field: &FieldValue) -> Option<usize> {
        match &field.value {
            Some(Value::String(s)) => Some(s.len()),
            _ => None,
        }
    }

    fn handle_missing_column(&mut self, table_name: &str, field_name: &str) {
        match self.column_policy {
            ColumnPolicy::RequireExisting => {
                self.findings.insert(Finding::error(
                    "SCHEMA_COLUMN_MISSING",
                    &format!(
                        "Transformed data contains column '{field_name}' which does not exist in destination table '{table_name}'. Missing-column creation is disabled."
                    ), FindingKind::SampleData
                ));
            }
            ColumnPolicy::AllowCreateIfPlanned => {
                if self.is_new_column(table_name, field_name) {
                    // OK: planned (mapped/computed) new column
                    return;
                }
                self.findings.insert(Finding::error(
                    "SCHEMA_COLUMN_MISSING",
                    &format!(
                        "Transformed data contains column '{field_name}' which does not exist in destination table '{table_name}' and is not a mapped or computed field."
                    ), FindingKind::SampleData
                ));
            }
        }
    }

    fn is_new_column(&self, table_name: &str, field_name: &str) -> bool {
        self.is_computed(table_name, field_name) || self.is_renamed_target(table_name, field_name)
    }

    fn is_computed(&self, table_name: &str, field_name: &str) -> bool {
        self.mapping
            .field_mappings
            .computed_fields
            .get(table_name)
            .is_some_and(|list| list.iter().any(|cf| cf.name == field_name))
    }

    fn is_renamed_target(&self, table_name: &str, field_name: &str) -> bool {
        self.mapping
            .field_mappings
            .field_renames
            .get(table_name)
            .is_some_and(|map| map.source_to_target.values().any(|t| t == field_name))
    }

    fn validate_required_columns(
        &mut self,
        table_name: &str,
        table_meta: &TableMetadata,
        row: &RowData,
    ) {
        let row_fields: HashSet<&str> = row.field_values.iter().map(|f| f.name.as_str()).collect();

        for (col_name, col_meta) in &table_meta.columns {
            if !col_meta.is_nullable
                && !col_meta.has_default
                && !row_fields.contains(col_name.as_str())
            {
                self.findings.insert(Finding::error(
                    "SCHEMA_MISSING_REQUIRED_COLUMN",
                    &format!(
                        "Required column '{col_name}' is missing from the transformed data for table '{table_name}'."
                    ),
                    FindingKind::SampleData,
                ));
            }
        }
    }

    fn validate_missing_table(&mut self, table_name: &str, row: &RowData) {
        match self.table_policy {
            TablePolicy::RequireExisting => {
                self.findings.insert(Finding::error(
                    "SCHEMA_TABLE_MISSING",
                    &format!(
                        "Destination table '{table_name}' does not exist and table creation is disabled."
                    ),
                    FindingKind::SampleData,
                ));
            }
            TablePolicy::AllowCreate(NewTableCreation::CopyAll) => {
                // Assume all provided columns will be created; nothing to validate here.
            }
            TablePolicy::AllowCreate(NewTableCreation::MapOnly) => {
                // Best-effort: ensure all columns are known to the mapping.
                for field in &row.field_values {
                    let field_name = field.name.as_str();
                    if !self.is_new_column(table_name, field_name) {
                        self.findings.insert(Finding::warning(
                            "SCHEMA_UNMAPPED_COLUMN_FOR_NEW_TABLE",
                            &format!(
                                "Transformed data for new table '{table_name}' contains column '{field_name}' which is not explicitly mapped/computed; its type/constraints cannot be validated."
                            ), FindingKind::SampleData
                        ));
                    }
                }
            }
        }
    }
}
