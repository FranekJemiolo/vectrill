//! Pipeline implementation for chaining operators

use crate::error::Result;
use crate::RecordBatch;

/// Trait for all operators in the pipeline
pub trait Operator: Send + Sync {
    /// Process a batch of data
    fn process(&mut self, batch: RecordBatch) -> Result<RecordBatch>;

    /// Flush any pending data and return final batches
    fn flush(&mut self) -> Result<Vec<RecordBatch>> {
        Ok(vec![])
    }
}

/// Pipeline for chaining operators together
pub struct Pipeline {
    operators: Vec<Box<dyn Operator>>,
}

impl Pipeline {
    /// Create a new pipeline
    pub fn new() -> Self {
        Self {
            operators: Vec::new(),
        }
    }

    /// Add an operator to the pipeline
    pub fn add_operator(mut self, operator: Box<dyn Operator>) -> Self {
        self.operators.push(operator);
        self
    }

    /// Process a batch through all operators in the pipeline
    pub fn process(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let mut current_batch = batch;

        for operator in &mut self.operators {
            current_batch = operator.process(current_batch)?;
        }

        Ok(current_batch)
    }

    /// Flush all operators in the pipeline
    pub fn flush(&mut self) -> Result<Vec<RecordBatch>> {
        let mut all_batches = Vec::new();

        for operator in &mut self.operators {
            let batches = operator.flush()?;
            all_batches.extend(batches);
        }

        Ok(all_batches)
    }

    /// Get the number of operators in the pipeline
    pub fn len(&self) -> usize {
        self.operators.len()
    }

    /// Check if the pipeline is empty
    pub fn is_empty(&self) -> bool {
        self.operators.is_empty()
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    /// Test operator that passes through batches unchanged
    struct PassThroughOperator;

    impl Operator for PassThroughOperator {
        fn process(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
            Ok(batch)
        }
    }

    /// Test operator that adds a constant value to an integer column
    struct AddConstantOperator {
        column_name: String,
        value: i64,
    }

    impl AddConstantOperator {
        fn new(column_name: &str, value: i64) -> Self {
            Self {
                column_name: column_name.to_string(),
                value,
            }
        }
    }

    impl Operator for AddConstantOperator {
        fn process(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
            use crate::error::VectrillError;
            // Find the column
            let col_idx = batch
                .schema()
                .column_with_name(&self.column_name)
                .ok_or_else(|| {
                    VectrillError::InvalidSchema(format!("Column '{}' not found", self.column_name))
                })?
                .0;

            let column = batch.column(col_idx);
            let int_array = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    VectrillError::InvalidExpression(format!(
                        "Column '{}' is not an integer array",
                        self.column_name
                    ))
                })?;

            // Create new array with added values
            let new_values: Vec<i64> = int_array.values().iter().map(|&x| x + self.value).collect();
            let new_array = Arc::new(Int64Array::from(new_values));

            // Create new batch with modified column
            let mut new_columns = batch.columns().to_vec();
            new_columns[col_idx] = new_array;

            let new_batch = RecordBatch::try_new(batch.schema(), new_columns)
                .map_err(|e| VectrillError::ArrowError(e.to_string()))?;

            Ok(new_batch)
        }
    }

    #[test]
    fn test_pipeline_empty() {
        let pipeline = Pipeline::new();
        assert_eq!(pipeline.len(), 0);
        assert!(pipeline.is_empty());
    }

    #[test]
    fn test_pipeline_single_operator() {
        let pipeline = Pipeline::new().add_operator(Box::new(PassThroughOperator));

        assert_eq!(pipeline.len(), 1);
        assert!(!pipeline.is_empty());
    }

    #[test]
    fn test_pipeline_multiple_operators() {
        let pipeline = Pipeline::new()
            .add_operator(Box::new(PassThroughOperator))
            .add_operator(Box::new(PassThroughOperator))
            .add_operator(Box::new(PassThroughOperator));

        assert_eq!(pipeline.len(), 3);
    }

    #[test]
    fn test_pipeline_process() {
        // Create test batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            ],
        )
        .unwrap();

        // Create pipeline with add constant operator
        let mut pipeline =
            Pipeline::new().add_operator(Box::new(AddConstantOperator::new("id", 10)));

        // Process batch
        let result = pipeline.process(batch).unwrap();

        // Verify results
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 2);

        let id_array = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 11); // 1 + 10
        assert_eq!(id_array.value(1), 12); // 2 + 10
        assert_eq!(id_array.value(2), 13); // 3 + 10
    }

    #[test]
    fn test_pipeline_flush() {
        let mut pipeline = Pipeline::new()
            .add_operator(Box::new(PassThroughOperator))
            .add_operator(Box::new(PassThroughOperator));

        let batches = pipeline.flush().unwrap();
        assert_eq!(batches.len(), 0); // PassThroughOperator doesn't produce any batches on flush
    }
}
