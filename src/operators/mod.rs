//! Operator trait and pipeline implementation

use crate::{error::Result, RecordBatch};

/// Trait for all operators in the pipeline
pub trait Operator: Send + Sync {
    /// Process a batch of data
    fn process(&mut self, batch: RecordBatch) -> Result<RecordBatch>;

    /// Flush any pending data and return final batches
    fn flush(&mut self) -> Result<Vec<RecordBatch>> {
        Ok(vec![])
    }
}

/// No-op operator that passes batches through unchanged
pub struct NoOpOperator;

impl Operator for NoOpOperator {
    fn process(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        Ok(batch)
    }
}

/// Pipeline that chains multiple operators
pub struct Pipeline {
    operators: Vec<Box<dyn Operator>>,
}

impl Pipeline {
    /// Create a new empty pipeline
    pub fn new() -> Self {
        Self {
            operators: Vec::new(),
        }
    }

    /// Add an operator to the pipeline
    pub fn add_operator(&mut self, op: Box<dyn Operator>) {
        self.operators.push(op);
    }

    /// Execute the pipeline on a batch
    pub fn execute(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        let mut current = batch;
        for op in &mut self.operators {
            current = op.process(current)?;
        }
        Ok(current)
    }

    /// Flush the pipeline and return all final batches
    pub fn flush(&mut self) -> Result<Vec<RecordBatch>> {
        let mut results = Vec::new();
        for op in &mut self.operators {
            results.extend(op.flush()?);
        }
        Ok(results)
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

    fn create_test_batch() -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]);

        let id = Int64Array::from(vec![1, 2, 3]);
        let name = StringArray::from(vec!["Alice", "Bob", "Charlie"]);

        RecordBatch::try_new(Arc::new(schema), vec![Arc::new(id), Arc::new(name)]).unwrap()
    }

    #[test]
    fn test_noop_operator() {
        let mut op = NoOpOperator;
        let batch = create_test_batch();
        let result = op.process(batch).unwrap();
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn test_pipeline_empty() {
        let mut pipeline = Pipeline::new();
        let batch = create_test_batch();
        let result = pipeline.execute(batch).unwrap();
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn test_pipeline_with_operator() {
        let mut pipeline = Pipeline::new();
        pipeline.add_operator(Box::new(NoOpOperator));
        let batch = create_test_batch();
        let result = pipeline.execute(batch).unwrap();
        assert_eq!(result.num_rows(), 3);
    }

    #[test]
    fn test_pipeline_flush() {
        let mut pipeline = Pipeline::new();
        pipeline.add_operator(Box::new(NoOpOperator));
        let results = pipeline.flush().unwrap();
        assert!(results.is_empty());
    }
}
