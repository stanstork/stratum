use connectors::sql::base::error::DbError;
use engine_core::{connectors::destination::Destination, context::exec::ExecutionContext};
use model::{
    core::{
        data_type::DataType,
        value::{FieldValue, Value},
    },
    execution::{
        connection::Connection,
        failed_row::FailedRow,
        pipeline::{FailedRowsDestination, FileFormat},
    },
    records::row::RowData,
};
use std::{fs::OpenOptions, io::Write, path::Path, sync::Arc};
use thiserror::Error;
use tracing::{debug, error, info};

#[derive(Error, Debug)]
pub enum FailedRowWriterError {
    #[error("Failed to write to database: {0}")]
    DatabaseWrite(#[from] DbError),

    #[error("Failed to write to file: {0}")]
    FileWrite(#[from] std::io::Error),

    #[error("Failed to serialize failed row: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Unsupported file format: {0:?}")]
    UnsupportedFormat(FileFormat),

    #[error("No destination configured")]
    NoDestination,

    #[error("Adapter error: {0}")]
    Adapter(#[from] connectors::error::AdapterError),

    #[error("Connection '{0}' not found in execution context")]
    ConnectionNotFound(String),
}

/// Writer for failed rows to various destinations
pub struct FailedRowWriter {
    destination: FailedRowsDestination,
    context: Arc<ExecutionContext>,
}

impl FailedRowWriter {
    pub fn new(destination: FailedRowsDestination, context: Arc<ExecutionContext>) -> Self {
        Self {
            destination,
            context,
        }
    }

    pub async fn write(&self, failed_row: &FailedRow) -> Result<(), FailedRowWriterError> {
        match &self.destination {
            FailedRowsDestination::Table {
                connection,
                table,
                schema,
            } => {
                self.write_to_table(failed_row, connection, table, schema.as_deref())
                    .await
            }
            FailedRowsDestination::File { path, format } => {
                self.write_to_file(failed_row, path, format)
            }
        }
    }

    pub async fn write_batch(&self, failed_rows: &[FailedRow]) -> Result<(), FailedRowWriterError> {
        match &self.destination {
            FailedRowsDestination::Table {
                connection,
                table,
                schema,
            } => {
                self.write_batch_to_table(failed_rows, connection, table, schema.as_deref())
                    .await
            }
            FailedRowsDestination::File { path, format } => {
                for failed_row in failed_rows {
                    self.write_to_file(failed_row, path, format)?;
                }
                Ok(())
            }
        }
    }

    async fn write_to_table(
        &self,
        failed_row: &FailedRow,
        connection: &Connection,
        table: &str,
        schema: Option<&str>,
    ) -> Result<(), FailedRowWriterError> {
        // Get adapter from execution context using the provided connection
        let adapter = self.context.get_adapter(connection).await?;

        // Determine format from adapter type
        let format = match &adapter {
            connectors::adapter::Adapter::Postgres(_) => {
                engine_core::connectors::format::DataFormat::Postgres
            }
            connectors::adapter::Adapter::MySql(_) => {
                engine_core::connectors::format::DataFormat::MySql
            }
            connectors::adapter::Adapter::Csv(_) => {
                engine_core::connectors::format::DataFormat::Csv
            }
        };

        let data_dest =
            engine_core::connectors::destination::DataDestination::from_adapter(format, &adapter)?;
        let destination = Destination::new(connection.name.clone(), format, data_dest);

        // Get table metadata
        let full_table_name = if let Some(schema) = schema {
            format!("{}.{}", schema, table)
        } else {
            table.to_string()
        };

        let meta = destination
            .data_dest
            .fetch_meta(full_table_name.clone())
            .await?;

        // Convert FailedRow to RowData
        let row_data = self.failed_row_to_row_data(failed_row, &full_table_name);

        // Write to database
        destination.write_batch(&meta, &[row_data]).await?;

        info!(
            "Wrote failed row {} to table {}",
            failed_row.id, full_table_name
        );
        Ok(())
    }

    async fn write_batch_to_table(
        &self,
        failed_rows: &[FailedRow],
        connection: &Connection,
        table: &str,
        schema: Option<&str>,
    ) -> Result<(), FailedRowWriterError> {
        if failed_rows.is_empty() {
            return Ok(());
        }

        // Get adapter from execution context using the provided connection
        let adapter = self.context.get_adapter(connection).await?;

        // Determine format from adapter type
        let format = match &adapter {
            connectors::adapter::Adapter::Postgres(_) => {
                engine_core::connectors::format::DataFormat::Postgres
            }
            connectors::adapter::Adapter::MySql(_) => {
                engine_core::connectors::format::DataFormat::MySql
            }
            connectors::adapter::Adapter::Csv(_) => {
                engine_core::connectors::format::DataFormat::Csv
            }
        };

        let data_dest =
            engine_core::connectors::destination::DataDestination::from_adapter(format, &adapter)?;
        let destination = Destination::new(connection.name.clone(), format, data_dest);

        // Get table metadata
        let full_table_name = if let Some(schema) = schema {
            format!("{}.{}", schema, table)
        } else {
            table.to_string()
        };

        let meta = destination
            .data_dest
            .fetch_meta(full_table_name.clone())
            .await?;

        // Convert all FailedRows to RowData
        let rows: Vec<RowData> = failed_rows
            .iter()
            .map(|fr| self.failed_row_to_row_data(fr, &full_table_name))
            .collect();

        // Write batch to database
        destination.write_batch(&meta, &rows).await?;

        info!(
            "Wrote {} failed rows to table {}",
            failed_rows.len(),
            full_table_name
        );
        Ok(())
    }

    /// Convert FailedRow to RowData for database insertion
    fn failed_row_to_row_data(&self, failed_row: &FailedRow, entity: &str) -> RowData {
        let storage_map = failed_row.to_storage_map();

        let field_values: Vec<FieldValue> = storage_map
            .into_iter()
            .map(|(name, value)| {
                let data_type = Self::infer_data_type(&value);
                FieldValue {
                    name,
                    value: Some(value),
                    data_type,
                }
            })
            .collect();

        RowData::new(entity, field_values)
    }

    /// Infer DataType from Value
    fn infer_data_type(value: &Value) -> DataType {
        match value {
            Value::SmallInt(_) => DataType::Short,
            Value::Int32(_) => DataType::Int,
            Value::Uint(_) => DataType::LongLong,
            Value::Float(_) => DataType::Float,
            Value::String(_) => DataType::VarChar,
            Value::Boolean(_) => DataType::Boolean,
            Value::Timestamp(_) => DataType::Timestamp,
            Value::Null => DataType::Null,
            _ => DataType::VarChar, // Default to VarChar for unknown types
        }
    }

    fn write_to_file(
        &self,
        failed_row: &FailedRow,
        path: &str,
        format: &FileFormat,
    ) -> Result<(), FailedRowWriterError> {
        match format {
            FileFormat::Json => self.write_to_json_file(failed_row, path),
            FileFormat::Csv => {
                error!("CSV format not yet implemented for failed rows");
                Err(FailedRowWriterError::UnsupportedFormat(FileFormat::Csv))
            }
            FileFormat::Parquet => {
                error!("Parquet format not yet implemented for failed rows");
                Err(FailedRowWriterError::UnsupportedFormat(FileFormat::Parquet))
            }
        }
    }

    fn write_to_json_file(
        &self,
        failed_row: &FailedRow,
        path: &str,
    ) -> Result<(), FailedRowWriterError> {
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent)?;
        }

        let mut file = OpenOptions::new().create(true).append(true).open(path)?;

        // Serialize and write as JSONL (one JSON object per line)
        let json = serde_json::to_string(failed_row)?;
        writeln!(file, "{}", json)?;

        debug!("Wrote failed row {} to {}", failed_row.id, path);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use engine_core::{plan::execution::ExecutionPlan, state::sled_store::SledStateStore};
    use model::{core::value::Value, execution::failed_row::ProcessingStage};
    use std::collections::HashMap;
    use tempfile::NamedTempFile;

    fn create_test_failed_row() -> FailedRow {
        let mut original_data = HashMap::new();
        original_data.insert("user_id".to_string(), Value::Uint(123));
        original_data.insert(
            "email".to_string(),
            Value::String("test@example.com".to_string()),
        );

        FailedRow::new(
            "test_pipeline".to_string(),
            ProcessingStage::Transform,
            original_data,
            "TransformError".to_string(),
            "Test error message".to_string(),
        )
    }

    async fn create_test_context() -> Arc<ExecutionContext> {
        use smql_syntax::ast::{doc::SmqlDocument, span::Span};

        let doc = SmqlDocument {
            define_block: None,
            connections: vec![],
            pipelines: vec![],
            span: Span::new(0, 0, 0, 0),
        };
        let plan = ExecutionPlan::build(&doc).unwrap();
        let state = Arc::new(SledStateStore::open(tempfile::tempdir().unwrap().path()).unwrap());
        Arc::new(ExecutionContext::new(&plan, state).await.unwrap())
    }

    #[tokio::test]
    async fn test_write_to_json_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();

        let destination = FailedRowsDestination::File {
            path: path.clone(),
            format: FileFormat::Json,
        };

        let context = create_test_context().await;
        let writer = FailedRowWriter::new(destination, context);
        let failed_row = create_test_failed_row();

        let result = writer.write(&failed_row).await;
        assert!(result.is_ok());

        // Verify file contains JSON
        let contents = std::fs::read_to_string(&path).unwrap();
        assert!(contents.contains("test_pipeline"));
        assert!(contents.contains("TransformError"));
    }

    #[tokio::test]
    async fn test_write_batch_to_json_file() {
        let temp_file = NamedTempFile::new().unwrap();
        let path = temp_file.path().to_str().unwrap().to_string();

        let destination = FailedRowsDestination::File {
            path: path.clone(),
            format: FileFormat::Json,
        };

        let context = create_test_context().await;
        let writer = FailedRowWriter::new(destination, context);
        let failed_rows = vec![
            create_test_failed_row(),
            create_test_failed_row(),
            create_test_failed_row(),
        ];

        let result = writer.write_batch(&failed_rows).await;
        assert!(result.is_ok());

        // Verify file contains 3 lines of JSON
        let contents = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 3);
    }

    #[tokio::test]
    async fn test_write_to_csv_returns_unsupported() {
        let destination = FailedRowsDestination::File {
            path: "/tmp/test.csv".to_string(),
            format: FileFormat::Csv,
        };

        let context = create_test_context().await;
        let writer = FailedRowWriter::new(destination, context);
        let failed_row = create_test_failed_row();

        let result = writer.write(&failed_row).await;
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            FailedRowWriterError::UnsupportedFormat(FileFormat::Csv)
        ));
    }

    #[tokio::test]
    async fn test_write_creates_parent_directory() {
        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir
            .path()
            .join("nested")
            .join("dir")
            .join("failed.json");
        let path_str = path.to_str().unwrap().to_string();

        let destination = FailedRowsDestination::File {
            path: path_str.clone(),
            format: FileFormat::Json,
        };

        let context = create_test_context().await;
        let writer = FailedRowWriter::new(destination, context);
        let failed_row = create_test_failed_row();

        let result = writer.write(&failed_row).await;
        assert!(result.is_ok());
        assert!(path.exists());
    }
}
