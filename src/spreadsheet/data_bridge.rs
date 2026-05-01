//! Data Bridge Module
//!
//! Handles conversion between spreadsheet formats and Arrow data structures.

use super::api::{CellValue, DataType as SpreadsheetDataType, SpreadsheetData};
use crate::{error::Result, VectrillError};
use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::DataType as ArrowDataType;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

/// Bridge between spreadsheet data and Arrow format
pub struct DataBridge {
    /// Configuration for data type inference
    #[allow(dead_code)]
    infer_types: bool,
    /// Maximum number of rows to sample for type inference
    type_inference_sample_size: usize,
}

impl DataBridge {
    /// Create new DataBridge instance
    pub fn new() -> Self {
        Self {
            infer_types: true,
            type_inference_sample_size: 100,
        }
    }

    /// Convert spreadsheet data to Arrow RecordBatch
    pub fn spreadsheet_to_arrow(&self, data: &SpreadsheetData) -> Result<RecordBatch> {
        if data.headers.is_empty() || data.rows.is_empty() {
            return Err(VectrillError::InvalidData(
                "Empty data provided".to_string(),
            ));
        }

        // Infer data types if not provided
        let column_types =
            if data.column_types.is_empty() || data.column_types.len() != data.headers.len() {
                self.infer_column_types(data)?
            } else {
                data.column_types.clone()
            };

        // Create Arrow schema
        let schema = self.create_arrow_schema(&data.headers, &column_types)?;

        // Create Arrow arrays
        let arrays = self.create_arrow_arrays(data, &column_types)?;

        // Create RecordBatch
        RecordBatch::try_new(schema, arrays).map_err(|e| VectrillError::ArrowError(e.to_string()))
    }

    /// Convert Arrow RecordBatch to spreadsheet data
    pub fn arrow_to_spreadsheet(&self, batch: &RecordBatch) -> Result<SpreadsheetData> {
        let schema = batch.schema();
        let mut headers = Vec::new();
        let mut column_types = Vec::new();

        // Extract headers and column types from schema
        for field in schema.fields() {
            headers.push(field.name().clone());
            column_types.push(self.arrow_to_spreadsheet_type(field.data_type())?);
        }

        // Convert Arrow arrays to spreadsheet rows
        let mut rows = Vec::new();
        let num_rows = batch.num_rows();

        for row_idx in 0..num_rows {
            let mut row = Vec::new();

            for col_idx in 0..batch.num_columns() {
                let array = batch.column(col_idx);
                let cell_value = self.arrow_value_to_cell(array, row_idx)?;
                row.push(cell_value);
            }

            rows.push(row);
        }

        Ok(SpreadsheetData {
            headers,
            rows,
            column_types,
            range: None,
            sheet_name: None,
        })
    }

    /// Infer data types for columns
    fn infer_column_types(&self, data: &SpreadsheetData) -> Result<Vec<SpreadsheetDataType>> {
        let mut column_types = Vec::new();
        let num_columns = data.headers.len();

        for col_idx in 0..num_columns {
            let column_type = self.infer_column_type(data, col_idx)?;
            column_types.push(column_type);
        }

        Ok(column_types)
    }

    /// Infer data type for a single column
    fn infer_column_type(
        &self,
        data: &SpreadsheetData,
        col_idx: usize,
    ) -> Result<SpreadsheetDataType> {
        let sample_size = data.rows.len().min(self.type_inference_sample_size);
        let mut type_scores = std::collections::HashMap::new();

        // Initialize scores for each type
        type_scores.insert(SpreadsheetDataType::String, 0);
        type_scores.insert(SpreadsheetDataType::Number, 0);
        type_scores.insert(SpreadsheetDataType::Boolean, 0);
        type_scores.insert(SpreadsheetDataType::Date, 0);
        type_scores.insert(SpreadsheetDataType::Empty, 0);

        // Sample rows and score types
        for row_idx in 0..sample_size {
            if col_idx < data.rows[row_idx].len() {
                let cell = &data.rows[row_idx][col_idx];
                let inferred_type = self.infer_cell_type(cell);
                *type_scores.entry(inferred_type).or_insert(0) += 1;
            }
        }

        // Find the type with highest score
        let best_type = type_scores
            .iter()
            .max_by_key(|(_, &score)| score)
            .map(|(t, _)| t.clone())
            .unwrap_or(SpreadsheetDataType::String);

        Ok(best_type)
    }

    /// Infer type for a single cell value
    fn infer_cell_type(&self, value: &CellValue) -> SpreadsheetDataType {
        match value {
            CellValue::Empty => SpreadsheetDataType::Empty,
            CellValue::Boolean(_) => SpreadsheetDataType::Boolean,
            CellValue::Number(_) => SpreadsheetDataType::Number,
            CellValue::String(s) => {
                // Try to parse as number
                if s.parse::<f64>().is_ok() {
                    SpreadsheetDataType::Number
                } else if s.parse::<bool>().is_ok() {
                    SpreadsheetDataType::Boolean
                } else if self.is_date_string(s) {
                    SpreadsheetDataType::Date
                } else {
                    SpreadsheetDataType::String
                }
            }
            CellValue::Error(_) => SpreadsheetDataType::String, // Treat errors as strings
        }
    }

    /// Check if string looks like a date
    fn is_date_string(&self, s: &str) -> bool {
        // Simple date detection - could be enhanced with more sophisticated parsing
        s.contains('/')
            || s.contains('-')
            || s.contains(':')
            || s.to_lowercase().contains("am")
            || s.to_lowercase().contains("pm")
    }

    /// Create Arrow schema from headers and column types
    fn create_arrow_schema(
        &self,
        headers: &[String],
        column_types: &[SpreadsheetDataType],
    ) -> Result<Arc<Schema>> {
        let mut fields = Vec::new();

        for (header, col_type) in headers.iter().zip(column_types.iter()) {
            let arrow_type = self.spreadsheet_to_arrow_type(col_type)?;
            let field = Field::new(header, arrow_type, true); // Nullable fields
            fields.push(field);
        }

        Ok(Arc::new(Schema::new(fields)))
    }

    /// Convert spreadsheet data type to Arrow data type
    fn spreadsheet_to_arrow_type(&self, stype: &SpreadsheetDataType) -> Result<ArrowDataType> {
        match stype {
            SpreadsheetDataType::String => Ok(ArrowDataType::Utf8),
            SpreadsheetDataType::Number => Ok(ArrowDataType::Float64),
            SpreadsheetDataType::Boolean => Ok(ArrowDataType::Boolean),
            SpreadsheetDataType::Date => Ok(ArrowDataType::Timestamp(
                arrow::datatypes::TimeUnit::Millisecond,
                None,
            )),
            SpreadsheetDataType::Empty => Ok(ArrowDataType::Utf8), // Treat empty as string
        }
    }

    /// Convert Arrow data type to spreadsheet data type
    fn arrow_to_spreadsheet_type(&self, atype: &ArrowDataType) -> Result<SpreadsheetDataType> {
        match atype {
            ArrowDataType::Utf8 => Ok(SpreadsheetDataType::String),
            ArrowDataType::Float64
            | ArrowDataType::Float32
            | ArrowDataType::Int64
            | ArrowDataType::Int32 => Ok(SpreadsheetDataType::Number),
            ArrowDataType::Boolean => Ok(SpreadsheetDataType::Boolean),
            ArrowDataType::Timestamp(_, _) => Ok(SpreadsheetDataType::Date),
            _ => Ok(SpreadsheetDataType::String), // Default to string for other types
        }
    }

    /// Create Arrow arrays from spreadsheet data
    fn create_arrow_arrays(
        &self,
        data: &SpreadsheetData,
        column_types: &[SpreadsheetDataType],
    ) -> Result<Vec<ArrayRef>> {
        let mut arrays = Vec::new();
        let num_rows = data.rows.len();

        for (col_idx, col_type) in column_types.iter().enumerate() {
            let array = match col_type {
                SpreadsheetDataType::String => {
                    let mut string_values = Vec::with_capacity(num_rows);
                    for row in &data.rows {
                        let value = if col_idx < row.len() {
                            &row[col_idx]
                        } else {
                            &CellValue::Empty
                        };
                        string_values.push(self.cell_value_to_string(value));
                    }
                    Arc::new(StringArray::from(string_values)) as ArrayRef
                }
                SpreadsheetDataType::Number => {
                    let mut number_values = Vec::with_capacity(num_rows);
                    for row in &data.rows {
                        let value = if col_idx < row.len() {
                            &row[col_idx]
                        } else {
                            &CellValue::Empty
                        };
                        number_values.push(self.cell_value_to_number(value));
                    }
                    Arc::new(Float64Array::from(number_values)) as ArrayRef
                }
                SpreadsheetDataType::Boolean => {
                    let mut boolean_values = Vec::with_capacity(num_rows);
                    for row in &data.rows {
                        let value = if col_idx < row.len() {
                            &row[col_idx]
                        } else {
                            &CellValue::Empty
                        };
                        boolean_values.push(self.cell_value_to_boolean(value));
                    }
                    Arc::new(BooleanArray::from(boolean_values)) as ArrayRef
                }
                SpreadsheetDataType::Date => {
                    let mut date_values = Vec::with_capacity(num_rows);
                    for row in &data.rows {
                        let value = if col_idx < row.len() {
                            &row[col_idx]
                        } else {
                            &CellValue::Empty
                        };
                        date_values.push(self.cell_value_to_timestamp(value));
                    }
                    Arc::new(Int64Array::from(date_values)) as ArrayRef
                }
                SpreadsheetDataType::Empty => {
                    // Treat empty as string column
                    let mut string_values: Vec<Option<String>> = Vec::with_capacity(num_rows);
                    for _ in 0..num_rows {
                        string_values.push(None);
                    }
                    Arc::new(StringArray::from(string_values)) as ArrayRef
                }
            };

            arrays.push(array);
        }

        Ok(arrays)
    }

    /// Convert CellValue to string
    fn cell_value_to_string(&self, value: &CellValue) -> Option<String> {
        match value {
            CellValue::String(s) => Some(s.clone()),
            CellValue::Number(n) => Some(n.to_string()),
            CellValue::Boolean(b) => Some(b.to_string()),
            CellValue::Empty => None,
            CellValue::Error(e) => Some(e.clone()),
        }
    }

    /// Convert CellValue to number
    fn cell_value_to_number(&self, value: &CellValue) -> Option<f64> {
        match value {
            CellValue::Number(n) => Some(*n),
            CellValue::String(s) => s.parse::<f64>().ok(),
            CellValue::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
            CellValue::Empty => None,
            CellValue::Error(_) => None,
        }
    }

    /// Convert CellValue to boolean
    fn cell_value_to_boolean(&self, value: &CellValue) -> Option<bool> {
        match value {
            CellValue::Boolean(b) => Some(*b),
            CellValue::String(s) => {
                let s_lower = s.to_lowercase();
                match s_lower.as_str() {
                    "true" | "yes" | "1" | "on" => Some(true),
                    "false" | "no" | "0" | "off" => Some(false),
                    _ => None,
                }
            }
            CellValue::Number(n) => Some(*n != 0.0),
            CellValue::Empty => None,
            CellValue::Error(_) => None,
        }
    }

    /// Convert CellValue to timestamp
    fn cell_value_to_timestamp(&self, value: &CellValue) -> Option<i64> {
        match value {
            CellValue::String(s) => s.parse::<i64>().ok(),
            CellValue::Number(n) => Some(*n as i64),
            CellValue::Empty => None,
            _ => None,
        }
    }

    /// Convert Arrow value to CellValue
    fn arrow_value_to_cell(&self, array: &ArrayRef, row_idx: usize) -> Result<CellValue> {
        if array.is_null(row_idx) {
            return Ok(CellValue::Empty);
        }

        match array.data_type() {
            ArrowDataType::Utf8 => {
                let string_array =
                    array
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| {
                            VectrillError::ArrowError(
                                "Failed to downcast to StringArray".to_string(),
                            )
                        })?;
                Ok(CellValue::String(string_array.value(row_idx).to_string()))
            }
            ArrowDataType::Float64 => {
                let float_array =
                    array
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .ok_or_else(|| {
                            VectrillError::ArrowError(
                                "Failed to downcast to Float64Array".to_string(),
                            )
                        })?;
                Ok(CellValue::Number(float_array.value(row_idx)))
            }
            ArrowDataType::Boolean => {
                let bool_array =
                    array
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .ok_or_else(|| {
                            VectrillError::ArrowError(
                                "Failed to downcast to BooleanArray".to_string(),
                            )
                        })?;
                Ok(CellValue::Boolean(bool_array.value(row_idx)))
            }
            ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, _) => {
                let timestamp_array =
                    array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                        VectrillError::ArrowError("Failed to downcast to Int64Array".to_string())
                    })?;
                Ok(CellValue::String(
                    timestamp_array.value(row_idx).to_string(),
                ))
            }
            _ => {
                // Default to string representation
                Ok(CellValue::String(format!("{:?}", array)))
            }
        }
    }
}

impl Default for DataBridge {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_bridge_creation() {
        let bridge = DataBridge::new();
        assert!(bridge.infer_types);
        assert_eq!(bridge.type_inference_sample_size, 100);
    }

    #[test]
    fn test_cell_type_inference() {
        let bridge = DataBridge::new();

        assert!(matches!(
            bridge.infer_cell_type(&CellValue::Number(42.0)),
            SpreadsheetDataType::Number
        ));
        assert!(matches!(
            bridge.infer_cell_type(&CellValue::Boolean(true)),
            SpreadsheetDataType::Boolean
        ));
        assert!(matches!(
            bridge.infer_cell_type(&CellValue::Empty),
            SpreadsheetDataType::Empty
        ));
        assert!(matches!(
            bridge.infer_cell_type(&CellValue::String("hello".to_string())),
            SpreadsheetDataType::String
        ));
        assert!(matches!(
            bridge.infer_cell_type(&CellValue::String("42".to_string())),
            SpreadsheetDataType::Number
        ));
    }

    #[test]
    fn test_value_conversions() {
        let bridge = DataBridge::new();

        assert_eq!(
            bridge.cell_value_to_string(&CellValue::Number(42.5)),
            Some("42.5".to_string())
        );
        assert_eq!(
            bridge.cell_value_to_number(&CellValue::String("3.14".to_string())),
            Some(3.14)
        );
        assert_eq!(
            bridge.cell_value_to_boolean(&CellValue::String("true".to_string())),
            Some(true)
        );
        assert_eq!(
            bridge.cell_value_to_boolean(&CellValue::Number(1.0)),
            Some(true)
        );
        assert_eq!(
            bridge.cell_value_to_boolean(&CellValue::Number(0.0)),
            Some(false)
        );
    }
}
