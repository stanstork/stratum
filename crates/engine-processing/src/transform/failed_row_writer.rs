use connectors::{
    adapter::Adapter,
    sql::base::{error::DbError, metadata::table::TableMetadata},
};
use engine_core::{
    connectors::{
        destination::{DataDestination, Destination},
        format::DataFormat,
    },
    context::exec::ExecutionContext,
};
use model::{
    execution::{
        connection::Connection,
        failed_row::FailedRow,
        pipeline::{FailedRowsDestination, FileFormat},
    },
    records::row::RowData,
};
use std::{fs::OpenOptions, io::Write, path::Path, sync::Arc};
use thiserror::Error;
use tokio::sync::OnceCell;
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

/// Cached database destination with metadata
struct CachedDbDestination {
    destination: Destination,
    table_name: String,
    metadata: TableMetadata,
}

/// Writer for failed rows to various destinations
pub struct FailedRowWriter {
    destination: FailedRowsDestination,
    context: Arc<ExecutionContext>,
    /// Cached database destination (lazy-initialized on first write)
    cached_db_dest: OnceCell<CachedDbDestination>,
}

impl FailedRowWriter {
    pub fn new(destination: FailedRowsDestination, context: Arc<ExecutionContext>) -> Self {
        Self {
            destination,
            context,
            cached_db_dest: OnceCell::new(),
        }
    }

    pub async fn write(&self, failed_row: &FailedRow) -> Result<(), FailedRowWriterError> {
        self.write_batch(std::slice::from_ref(failed_row)).await
    }

    pub async fn write_batch(&self, failed_rows: &[FailedRow]) -> Result<(), FailedRowWriterError> {
        if failed_rows.is_empty() {
            return Ok(());
        }

        match &self.destination {
            FailedRowsDestination::Table {
                connection,
                table,
                schema,
            } => {
                self.write_to_table(failed_rows, connection, table, schema.as_deref())
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
        failed_rows: &[FailedRow],
        connection: &Connection,
        table: &str,
        schema: Option<&str>,
    ) -> Result<(), FailedRowWriterError> {
        if failed_rows.is_empty() {
            return Ok(());
        }

        let table_name = if let Some(schema) = schema {
            format!("{}.{}", schema, table)
        } else {
            table.to_string()
        };

        // Get or initialize cached destination
        let data_dest = self
            .cached_db_dest
            .get_or_try_init(|| async {
                // Initialize on first use
                let adapter = self.context.get_adapter(connection).await?;
                let format = data_format(&adapter);

                let data_dest = DataDestination::from_adapter(format, &adapter)?;
                let destination = Destination::new(connection.name.clone(), format, data_dest);
                let metadata = destination.data_dest.fetch_meta(table_name.clone()).await?;

                Ok::<CachedDbDestination, FailedRowWriterError>(CachedDbDestination {
                    destination,
                    table_name: table_name.clone(),
                    metadata,
                })
            })
            .await?;

        let rows: Vec<RowData> = failed_rows
            .iter()
            .map(|fr| fr.to_row_data(&data_dest.table_name))
            .collect();

        data_dest
            .destination
            .write_batch(&data_dest.metadata, &rows)
            .await?;

        info!(
            "Wrote {} failed rows to table {}",
            failed_rows.len(),
            data_dest.table_name
        );
        Ok(())
    }

    fn write_to_file(
        &self,
        failed_row: &FailedRow,
        path: &str,
        format: &FileFormat,
    ) -> Result<(), FailedRowWriterError> {
        match format {
            FileFormat::Json => self.write_to_json(failed_row, path),
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

    fn write_to_json(
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

fn data_format(adapter: &Adapter) -> DataFormat {
    match adapter {
        Adapter::MySql(_) => DataFormat::MySql,
        Adapter::Postgres(_) => DataFormat::Postgres,
        Adapter::Csv(_) => DataFormat::Csv,
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
