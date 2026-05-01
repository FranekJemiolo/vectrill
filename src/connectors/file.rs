//! File connector for reading from local files

use crate::{error::Result, RecordBatch, VectrillError};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch as ArrowRecordBatch;
use csv::ReaderBuilder;
use csv::StringRecord;
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
    reader: Option<csv::Reader<File>>,
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
                let mut reader = ReaderBuilder::new().has_headers(true).from_reader(file);

                // Validate schema matches file headers
                let headers = reader
                    .headers()
                    .map_err(|e| VectrillError::Connector(format!("CSV error: {}", e)))?;
                for (i, header) in headers.iter().enumerate() {
                    if let Some(field) = schema.fields().get(i) {
                        if field.name() != header {
                            return Err(VectrillError::InvalidSchema(format!(
                                "Header '{}' doesn't match schema field '{}'",
                                header,
                                field.name()
                            )));
                        }
                    }
                }

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
        let file = File::open(&path)?;
        let mut reader = ReaderBuilder::new().has_headers(true).from_reader(file);

        let headers = reader
            .headers()
            .map_err(|e| VectrillError::Connector(format!("CSV error: {}", e)))?;

        // Infer schema from headers (use String for all columns for simplicity)
        let fields: Vec<Field> = headers
            .iter()
            .map(|h| Field::new(h, DataType::Utf8, true))
            .collect();

        let schema = Arc::new(Schema::new(fields));

        let file = File::open(&path)?;
        let reader = ReaderBuilder::new().has_headers(true).from_reader(file);

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

        let mut records = Vec::with_capacity(self.batch_size);
        let mut record = StringRecord::new();

        for _ in 0..self.batch_size {
            match reader.read_record(&mut record) {
                Ok(true) => {
                    records.push(record.clone());
                    self.current_row += 1;
                    self.total_rows += 1;
                }
                Ok(false) => break,
                Err(e) => {
                    return Some(Err(VectrillError::Connector(format!(
                        "CSV read error: {}",
                        e
                    ))));
                }
            }
        }

        if records.is_empty() {
            None
        } else {
            // Convert CSV records to Arrow RecordBatch
            self.records_to_batch(&records).ok().map(Ok)
        }
    }

    /// Convert CSV records to Arrow RecordBatch
    fn records_to_batch(&self, records: &[csv::StringRecord]) -> Result<ArrowRecordBatch> {
        use arrow::array::StringBuilder;

        let num_columns = self.schema.fields().len();
        let mut builders: Vec<StringBuilder> =
            (0..num_columns).map(|_| StringBuilder::new()).collect();

        for record in records {
            for (i, value) in record.iter().enumerate() {
                if i < num_columns {
                    builders[i].append_value(value);
                }
            }
        }

        let arrays: Vec<Arc<dyn arrow::array::Array>> = builders
            .into_iter()
            .map(|mut b| Arc::new(b.finish()) as Arc<dyn arrow::array::Array>)
            .collect();

        ArrowRecordBatch::try_new(self.schema.clone(), arrays)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))
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
        assert_eq!(connector.schema.field(0).name(), "id");
        assert_eq!(connector.schema.field(1).name(), "name");
        assert_eq!(connector.schema.field(2).name(), "value");
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
