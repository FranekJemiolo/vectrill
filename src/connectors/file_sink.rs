//! File sink for writing data to local files

use crate::{error::Result, RecordBatch, VectrillError};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch as ArrowRecordBatch;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use super::sink::Sink;
use async_trait::async_trait;

/// File format for output
#[derive(Debug, Clone, Copy)]
pub enum FileSinkFormat {
    Csv,
    Json,
    Parquet,
}

/// File sink for writing data to local files
pub struct FileSink {
    path: PathBuf,
    format: FileSinkFormat,
    schema: SchemaRef,
    writer: Option<Arc<Mutex<Box<dyn Write + Send>>>>,
    header_written: bool,
}

impl FileSink {
    /// Create a new file sink
    pub fn new(path: PathBuf, format: FileSinkFormat, schema: SchemaRef) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| {
                VectrillError::Connector(format!("Failed to open file {}: {}", path.display(), e))
            })?;

        let writer: Box<dyn Write + Send> = match format {
            FileSinkFormat::Csv => Box::new(file),
            FileSinkFormat::Json => Box::new(file),
            FileSinkFormat::Parquet => {
                return Err(VectrillError::Connector(
                    "Parquet file sink not yet implemented".to_string(),
                ));
            }
        };

        Ok(Self {
            path,
            format,
            schema,
            writer: Some(Arc::new(Mutex::new(writer))),
            header_written: false,
        })
    }

    /// Write CSV header
    fn write_csv_header(&mut self) -> Result<()> {
        if self.header_written {
            return Ok(());
        }

        let headers: Vec<String> = self
            .schema
            .fields()
            .iter()
            .map(|field| field.name().clone())
            .collect();

        let header_line = headers.join(",");
        if let Some(ref writer) = self.writer {
            let mut writer_guard = writer.lock().map_err(|e| {
                VectrillError::Connector(format!("Failed to acquire writer lock: {}", e))
            })?;
            writer_guard
                .write_all(header_line.as_bytes())
                .map_err(|e| {
                    VectrillError::Connector(format!("Failed to write CSV header: {}", e))
                })?;
            writer_guard
                .write_all(b"\n")
                .map_err(|e| VectrillError::Connector(format!("Failed to write newline: {}", e)))?;
        }

        self.header_written = true;
        Ok(())
    }

    /// Write a batch in CSV format
    fn write_csv_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.write_csv_header()?;

        if let Some(ref writer) = self.writer {
            let mut writer_guard = writer.lock().map_err(|e| {
                VectrillError::Connector(format!("Failed to acquire writer lock: {}", e))
            })?;

            let mut buffer = Vec::with_capacity(batch.num_rows() * 64);
            for row_idx in 0..batch.num_rows() {
                let mut values = Vec::with_capacity(batch.num_columns());

                for col_idx in 0..batch.num_columns() {
                    let array = batch.column(col_idx);
                    let value = if array.is_null(row_idx) {
                        String::new()
                    } else {
                        let raw = self.arrow_value_to_string(array, row_idx)?;
                        escape_csv_field(&raw)
                    };
                    values.push(value);
                }

                buffer.extend_from_slice(values.join(",").as_bytes());
                buffer.push(b'\n');
            }

            writer_guard.write_all(&buffer).map_err(|e| {
                VectrillError::Connector(format!("Failed to write CSV batch: {}", e))
            })?;
        }

        Ok(())
    }

    /// Write a batch in JSON format
    fn write_json_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        if let Some(ref writer) = self.writer {
            let mut writer_guard = writer.lock().map_err(|e| {
                VectrillError::Connector(format!("Failed to acquire writer lock: {}", e))
            })?;

            let mut buffer = Vec::with_capacity(batch.num_rows() * 128);
            for row_idx in 0..batch.num_rows() {
                let json_line = self.serialize_row_as_json(batch, row_idx)?;
                buffer.extend_from_slice(json_line.as_bytes());
                buffer.push(b'\n');
            }

            writer_guard.write_all(&buffer).map_err(|e| {
                VectrillError::Connector(format!("Failed to write JSON batch: {}", e))
            })?;
        }

        Ok(())
    }

    /// Serialize a single row as JSON
    fn serialize_row_as_json(&self, batch: &ArrowRecordBatch, row_idx: usize) -> Result<String> {
        use serde_json::{Map, Value};

        let mut map = Map::new();

        for (col_idx, field) in self.schema.fields().iter().enumerate() {
            let array = batch.column(col_idx);
            let value = if array.is_null(row_idx) {
                Value::Null
            } else {
                // Convert Arrow value to JSON value
                self.arrow_value_to_json(array, row_idx)?
            };
            map.insert(field.name().clone(), value);
        }

        serde_json::to_string(&Value::Object(map))
            .map_err(|e| VectrillError::Connector(format!("JSON serialization failed: {}", e)))
    }

    /// Convert Arrow value to JSON value
    fn arrow_value_to_json(
        &self,
        array: &dyn arrow::array::Array,
        row_idx: usize,
    ) -> Result<serde_json::Value> {
        use arrow::array::*;
        use arrow::datatypes::DataType;

        let data_type = array.data_type();

        match data_type {
            DataType::Boolean => {
                let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok(serde_json::Value::Bool(bool_array.value(row_idx)))
            }
            DataType::Int32 => {
                let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(serde_json::Value::Number(serde_json::Number::from(
                    int_array.value(row_idx),
                )))
            }
            DataType::Int64 => {
                let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(serde_json::Value::Number(serde_json::Number::from(
                    int_array.value(row_idx),
                )))
            }
            DataType::Float32 => {
                let float_array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                Ok(serde_json::Value::Number(
                    serde_json::Number::from_f64(float_array.value(row_idx) as f64).unwrap(),
                ))
            }
            DataType::Float64 => {
                let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(serde_json::Value::Number(
                    serde_json::Number::from_f64(float_array.value(row_idx)).unwrap(),
                ))
            }
            DataType::Utf8 => {
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                Ok(serde_json::Value::String(
                    string_array.value(row_idx).to_string(),
                ))
            }
            _ => Err(VectrillError::Connector(format!(
                "Unsupported data type for JSON serialization: {:?}",
                data_type
            ))),
        }
    }

    /// Convert Arrow value to string
    fn arrow_value_to_string(
        &self,
        array: &dyn arrow::array::Array,
        row_idx: usize,
    ) -> Result<String> {
        use arrow::array::*;
        use arrow::datatypes::DataType;

        let data_type = array.data_type();

        match data_type {
            DataType::Boolean => {
                let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok(bool_array.value(row_idx).to_string())
            }
            DataType::Int32 => {
                let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(int_array.value(row_idx).to_string())
            }
            DataType::Int64 => {
                let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(int_array.value(row_idx).to_string())
            }
            DataType::Float32 => {
                let float_array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                Ok(float_array.value(row_idx).to_string())
            }
            DataType::Float64 => {
                let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(float_array.value(row_idx).to_string())
            }
            DataType::Utf8 => {
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                Ok(string_array.value(row_idx).to_string())
            }
            _ => Err(VectrillError::Connector(format!(
                "Unsupported data type for string serialization: {:?}",
                data_type
            ))),
        }
    }
}

#[async_trait]
impl Sink for FileSink {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        match self.format {
            FileSinkFormat::Csv => self.write_csv_batch(batch),
            FileSinkFormat::Json => self.write_json_batch(batch),
            FileSinkFormat::Parquet => Err(VectrillError::Connector(
                "Parquet file sink not yet implemented".to_string(),
            )),
        }
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn name(&self) -> &str {
        self.path.to_str().unwrap_or("file")
    }

    async fn flush(&mut self) -> Result<()> {
        if let Some(ref writer) = self.writer {
            let mut writer_guard = writer.lock().map_err(|e| {
                VectrillError::Connector(format!("Failed to acquire writer lock: {}", e))
            })?;
            writer_guard
                .flush()
                .map_err(|e| VectrillError::Connector(format!("Failed to flush file: {}", e)))?;
        }
        Ok(())
    }
}

/// Escape a CSV field if it contains commas, quotes, or newlines
fn escape_csv_field(val: &str) -> String {
    if val.contains(',') || val.contains('"') || val.contains('\n') || val.contains('\r') {
        let escaped = val.replace('"', "\"\"");
        format!("\"{}\"", escaped)
    } else {
        val.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    use tempfile::NamedTempFile;

    #[test]
    fn test_file_sink_creation() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let temp_file = NamedTempFile::new().unwrap();
        let sink = FileSink::new(temp_file.path().to_path_buf(), FileSinkFormat::Csv, schema);
        assert!(sink.is_ok());
    }

    #[test]
    fn test_file_sink_format_enum() {
        let csv = FileSinkFormat::Csv;
        let json = FileSinkFormat::Json;
        let parquet = FileSinkFormat::Parquet;

        assert!(matches!(csv, FileSinkFormat::Csv));
        assert!(matches!(json, FileSinkFormat::Json));
        assert!(matches!(parquet, FileSinkFormat::Parquet));
    }
}
