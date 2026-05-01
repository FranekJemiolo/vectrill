//! File connector for reading from local files

use crate::{error::Result, RecordBatch, VectrillError};
use arrow::csv::ReaderBuilder;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch as ArrowRecordBatch;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use super::Connector;
use async_trait::async_trait;

/// File format
#[derive(Debug, Clone, Copy)]
pub enum FileFormat {
    Csv,
    Json,
    Parquet,
}

/// File connector for reading data from local files
pub struct FileConnector {
    path: PathBuf,
    format: FileFormat,
    schema: SchemaRef,
    reader: Option<arrow::csv::Reader<File>>,
    batch_size: usize,
    current_row: usize,
    total_rows: usize,
}

impl FileConnector {
    /// Create a new file connector
    pub fn new(path: PathBuf, format: FileFormat, schema: SchemaRef) -> Result<Self> {
        let file = File::open(&path)?;

        let reader = match format {
            FileFormat::Csv => {
                let reader = ReaderBuilder::new(schema.clone())
                    .with_header(true)
                    .build(file)?;
                Some(reader)
            }
            FileFormat::Json => {
                return Err(VectrillError::Connector(
                    "JSON file connector not yet implemented".to_string(),
                ));
            }
            FileFormat::Parquet => {
                return Err(VectrillError::Connector(
                    "Parquet file connector not yet implemented".to_string(),
                ));
            }
        };

        Ok(Self {
            path,
            format,
            schema,
            reader,
            batch_size: 1000,
            current_row: 0,
            total_rows: 0,
        })
    }

    /// Create a CSV file connector with schema inference
    pub fn csv_with_inference(path: PathBuf) -> Result<Self> {
        // For now, create a simple schema with string columns
        // In a real implementation, we'd read the first row to infer column names
        let fields = vec![
            Field::new("column1", DataType::Utf8, true),
            Field::new("column2", DataType::Utf8, true),
            Field::new("column3", DataType::Utf8, true),
        ];
        let schema = Arc::new(Schema::new(fields));

        let file = File::open(&path)?;
        let reader = ReaderBuilder::new(schema.clone())
            .with_header(true)
            .build(file)?;

        Ok(Self {
            path,
            format: FileFormat::Csv,
            schema,
            reader: Some(reader),
            batch_size: 1000,
            current_row: 0,
            total_rows: 0,
        })
    }

    /// Set the batch size for reading
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Read a batch of records from the CSV file
    fn read_csv_batch(&mut self) -> Option<Result<ArrowRecordBatch>> {
        let reader = self.reader.as_mut()?;

        // Arrow CSV reader reads directly into RecordBatch
        match reader.next() {
            Some(Ok(batch)) => {
                self.current_row += batch.num_rows();
                self.total_rows += batch.num_rows();
                Some(Ok(batch))
            }
            Some(Err(e)) => Some(Err(VectrillError::ArrowError(e.to_string()))),
            None => None,
        }
    }
}

#[async_trait]
impl Connector for FileConnector {
    async fn next_batch(&mut self) -> Option<Result<RecordBatch>> {
        // For now, we'll read synchronously in the async context
        // In a real implementation, we'd use async file I/O
        match self.format {
            FileFormat::Csv => self.read_csv_batch(),
            FileFormat::Json => Some(Err(VectrillError::Connector(
                "JSON file connector not yet implemented".to_string(),
            ))),
            FileFormat::Parquet => Some(Err(VectrillError::Connector(
                "Parquet file connector not yet implemented".to_string(),
            ))),
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn name(&self) -> &str {
        self.path.to_str().unwrap_or("file")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::io::Write;
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    #[test]
    fn test_file_connector_creation() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Utf8, false)]));

        let connector = FileConnector::new(PathBuf::from("/tmp/test.csv"), FileFormat::Csv, schema);
        assert!(connector.is_err()); // File doesn't exist
    }

    #[test]
    fn test_csv_with_inference() {
        let mut temp_file = NamedTempFile::new().unwrap();
        writeln!(temp_file, "id,name,value").unwrap();
        writeln!(temp_file, "1,Alice,100").unwrap();
        writeln!(temp_file, "2,Bob,200").unwrap();

        let connector = FileConnector::csv_with_inference(temp_file.path().to_path_buf());
        assert!(connector.is_ok());

        let connector = connector.unwrap();
        assert_eq!(connector.schema.fields().len(), 3);
        assert_eq!(connector.schema.field(0).name(), "column1");
        assert_eq!(connector.schema.field(1).name(), "column2");
        assert_eq!(connector.schema.field(2).name(), "column3");
    }

    #[test]
    fn test_file_format_enum() {
        let csv = FileFormat::Csv;
        let json = FileFormat::Json;
        let parquet = FileFormat::Parquet;

        assert!(matches!(csv, FileFormat::Csv));
        assert!(matches!(json, FileFormat::Json));
        assert!(matches!(parquet, FileFormat::Parquet));
    }
}
