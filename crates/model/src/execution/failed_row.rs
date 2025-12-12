use crate::core::value::Value;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a row that failed during pipeline processing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailedRow {
    pub id: String,
    pub pipeline_name: String,
    pub stage: ProcessingStage,
    pub original_data: HashMap<String, Value>,
    pub error: FailureError,
    pub metadata: FailureMetadata,
    pub failed_at: DateTime<Utc>,
    pub table_name: Option<String>,
    pub attempt_number: Option<u32>,
}

/// The stage of pipeline processing where the failure occurred
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ProcessingStage {
    /// Failed during data extraction from source
    Extract,

    /// Failed during transformation
    Transform,

    /// Failed during validation
    Validation,

    /// Failed during load to destination
    Load,

    /// Failed during a custom operation
    Custom(String),
}

/// Error information for a failed row
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureError {
    pub error_type: String,
    pub message: String,
    pub details: Option<String>,
    pub is_retryable: bool,
}

/// Metadata about the failure context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureMetadata {
    pub run_id: String,
    pub batch_id: Option<String>,
    pub row_index: Option<usize>, // row index within the batch
    pub source: Option<String>,
    pub custom: HashMap<String, Value>,
}

impl FailedRow {
    /// Create a new FailedRow with minimal required information
    pub fn new(
        pipeline_name: String,
        stage: ProcessingStage,
        original_data: HashMap<String, Value>,
        error_type: String,
        error_message: String,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            pipeline_name,
            stage,
            original_data,
            error: FailureError {
                error_type,
                message: error_message,
                details: None,
                is_retryable: false,
            },
            metadata: FailureMetadata {
                run_id: uuid::Uuid::new_v4().to_string(),
                batch_id: None,
                row_index: None,
                source: None,
                custom: HashMap::new(),
            },
            failed_at: Utc::now(),
            table_name: None,
            attempt_number: None,
        }
    }

    /// Create a new FailedRow from a validation error
    pub fn from_validation_error(
        pipeline_name: String,
        original_data: HashMap<String, Value>,
        rule_name: String,
        validation_message: String,
    ) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            pipeline_name,
            stage: ProcessingStage::Validation,
            original_data,
            error: FailureError {
                error_type: format!("ValidationError::{}", rule_name),
                message: validation_message,
                details: None,
                is_retryable: false,
            },
            metadata: FailureMetadata {
                run_id: uuid::Uuid::new_v4().to_string(),
                batch_id: None,
                row_index: None,
                source: None,
                custom: HashMap::new(),
            },
            failed_at: Utc::now(),
            table_name: None,
            attempt_number: None,
        }
    }

    /// Add execution context to the failed row
    pub fn with_execution_context(
        mut self,
        execution_id: String,
        batch_id: Option<String>,
        row_index: Option<usize>,
    ) -> Self {
        self.metadata.run_id = execution_id;
        self.metadata.batch_id = batch_id;
        self.metadata.row_index = row_index;
        self
    }

    /// Add table name
    pub fn with_table(mut self, table_name: String) -> Self {
        self.table_name = Some(table_name);
        self
    }

    /// Add attempt number for retry tracking
    pub fn with_attempt(mut self, attempt: u32) -> Self {
        self.attempt_number = Some(attempt);
        self
    }

    /// Mark error as retryable
    pub fn with_retryable(mut self, retryable: bool) -> Self {
        self.error.is_retryable = retryable;
        self
    }

    /// Add detailed error information
    pub fn with_error_details(mut self, details: String) -> Self {
        self.error.details = Some(details);
        self
    }

    /// Add custom metadata
    pub fn with_metadata(mut self, key: String, value: Value) -> Self {
        self.metadata.custom.insert(key, value);
        self
    }

    /// Convert to a HashMap for writing to table/file
    /// This includes all fields flattened for easy storage
    pub fn to_storage_map(&self) -> HashMap<String, Value> {
        let mut map = HashMap::new();

        // Core fields
        map.insert("id".to_string(), Value::String(self.id.clone()));
        map.insert(
            "pipeline_name".to_string(),
            Value::String(self.pipeline_name.clone()),
        );
        map.insert(
            "stage".to_string(),
            Value::String(format!("{:?}", self.stage)),
        );
        map.insert(
            "error_type".to_string(),
            Value::String(self.error.error_type.clone()),
        );
        map.insert(
            "error_message".to_string(),
            Value::String(self.error.message.clone()),
        );
        map.insert(
            "is_retryable".to_string(),
            Value::Boolean(self.error.is_retryable),
        );
        map.insert(
            "failed_at".to_string(),
            Value::String(self.failed_at.to_rfc3339()),
        );
        map.insert(
            "execution_id".to_string(),
            Value::String(self.metadata.run_id.clone()),
        );

        // Optional fields
        if let Some(details) = &self.error.details {
            map.insert("error_details".to_string(), Value::String(details.clone()));
        }

        if let Some(table) = &self.table_name {
            map.insert("table_name".to_string(), Value::String(table.clone()));
        }

        if let Some(batch_id) = &self.metadata.batch_id {
            map.insert("batch_id".to_string(), Value::String(batch_id.clone()));
        }

        if let Some(row_index) = self.metadata.row_index {
            map.insert("row_index".to_string(), Value::Uint(row_index as u64));
        }

        if let Some(source) = &self.metadata.source {
            map.insert("source".to_string(), Value::String(source.clone()));
        }

        if let Some(attempt) = self.attempt_number {
            map.insert("attempt_number".to_string(), Value::Uint(attempt as u64));
        }

        // Original data as JSON string for easy storage
        if let Ok(json) = serde_json::to_string(&self.original_data) {
            map.insert("original_data".to_string(), Value::String(json));
        }

        // Custom metadata as JSON string
        if !self.metadata.custom.is_empty() {
            if let Ok(json) = serde_json::to_string(&self.metadata.custom) {
                map.insert("custom_metadata".to_string(), Value::String(json));
            }
        }

        map
    }
}

impl std::fmt::Display for ProcessingStage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProcessingStage::Extract => write!(f, "Extract"),
            ProcessingStage::Transform => write!(f, "Transform"),
            ProcessingStage::Validation => write!(f, "Validation"),
            ProcessingStage::Load => write!(f, "Load"),
            ProcessingStage::Custom(name) => write!(f, "Custom({})", name),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_failed_row_creation() {
        let mut original_data = HashMap::new();
        original_data.insert("user_id".to_string(), Value::Uint(123));
        original_data.insert(
            "email".to_string(),
            Value::String("test@example.com".to_string()),
        );

        let failed_row = FailedRow::new(
            "user_pipeline".to_string(),
            ProcessingStage::Validation,
            original_data.clone(),
            "ValidationError".to_string(),
            "Email format invalid".to_string(),
        );

        assert_eq!(failed_row.pipeline_name, "user_pipeline");
        assert_eq!(failed_row.stage, ProcessingStage::Validation);
        assert_eq!(failed_row.error.error_type, "ValidationError");
        assert_eq!(failed_row.error.message, "Email format invalid");
        assert!(!failed_row.id.is_empty());
    }

    #[test]
    fn test_failed_row_from_validation_error() {
        let mut original_data = HashMap::new();
        original_data.insert("amount".to_string(), Value::Float(-100.0));

        let failed_row = FailedRow::from_validation_error(
            "payment_pipeline".to_string(),
            original_data,
            "positive_amount".to_string(),
            "Amount must be positive".to_string(),
        );

        assert_eq!(failed_row.stage, ProcessingStage::Validation);
        assert_eq!(
            failed_row.error.error_type,
            "ValidationError::positive_amount"
        );
        assert!(!failed_row.error.is_retryable);
    }

    #[test]
    fn test_failed_row_with_context() {
        let mut original_data = HashMap::new();
        original_data.insert("id".to_string(), Value::Uint(1));

        let failed_row = FailedRow::new(
            "test_pipeline".to_string(),
            ProcessingStage::Transform,
            original_data,
            "TransformError".to_string(),
            "Field missing".to_string(),
        )
        .with_execution_context(
            "exec_123".to_string(),
            Some("batch_456".to_string()),
            Some(10),
        )
        .with_table("users".to_string())
        .with_attempt(2)
        .with_retryable(true)
        .with_error_details("Stack trace...".to_string())
        .with_metadata(
            "custom_key".to_string(),
            Value::String("custom_value".to_string()),
        );

        assert_eq!(failed_row.metadata.run_id, "exec_123");
        assert_eq!(failed_row.metadata.batch_id, Some("batch_456".to_string()));
        assert_eq!(failed_row.metadata.row_index, Some(10));
        assert_eq!(failed_row.table_name, Some("users".to_string()));
        assert_eq!(failed_row.attempt_number, Some(2));
        assert!(failed_row.error.is_retryable);
        assert_eq!(failed_row.error.details, Some("Stack trace...".to_string()));
        assert_eq!(
            failed_row.metadata.custom.get("custom_key"),
            Some(&Value::String("custom_value".to_string()))
        );
    }

    #[test]
    fn test_to_storage_map() {
        let mut original_data = HashMap::new();
        original_data.insert("id".to_string(), Value::Uint(1));
        original_data.insert("name".to_string(), Value::String("Test".to_string()));

        let failed_row = FailedRow::new(
            "test_pipeline".to_string(),
            ProcessingStage::Load,
            original_data,
            "LoadError".to_string(),
            "Connection timeout".to_string(),
        )
        .with_execution_context(
            "exec_789".to_string(),
            Some("batch_012".to_string()),
            Some(5),
        )
        .with_table("orders".to_string());

        let storage_map = failed_row.to_storage_map();

        assert_eq!(
            storage_map.get("pipeline_name"),
            Some(&Value::String("test_pipeline".to_string()))
        );
        assert_eq!(
            storage_map.get("stage"),
            Some(&Value::String("Load".to_string()))
        );
        assert_eq!(
            storage_map.get("error_type"),
            Some(&Value::String("LoadError".to_string()))
        );
        assert_eq!(
            storage_map.get("execution_id"),
            Some(&Value::String("exec_789".to_string()))
        );
        assert_eq!(
            storage_map.get("table_name"),
            Some(&Value::String("orders".to_string()))
        );
        assert!(storage_map.contains_key("original_data"));
        assert!(storage_map.contains_key("failed_at"));
    }

    #[test]
    fn test_processing_stage_display() {
        assert_eq!(ProcessingStage::Extract.to_string(), "Extract");
        assert_eq!(ProcessingStage::Transform.to_string(), "Transform");
        assert_eq!(ProcessingStage::Validation.to_string(), "Validation");
        assert_eq!(ProcessingStage::Load.to_string(), "Load");
        assert_eq!(
            ProcessingStage::Custom("dedup".to_string()).to_string(),
            "Custom(dedup)"
        );
    }
}
