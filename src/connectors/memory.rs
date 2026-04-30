//! In-memory connector for testing

use crate::{error::Result, RecordBatch};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use std::sync::Arc;
use tokio::time::{sleep, Duration};

use super::Connector;
use async_trait::async_trait;

/// In-memory connector that generates test data
pub struct MemoryConnector {
    name: String,
    schema: SchemaRef,
    batch_count: usize,
    current_batch: usize,
    batch_size: usize,
}

impl MemoryConnector {
    /// Create a new memory connector
    pub fn new(name: String, batch_count: usize, batch_size: usize) -> Self {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        Self {
            name,
            schema,
            batch_count,
            current_batch: 0,
            batch_size,
        }
    }

    /// Create a connector with a custom schema
    pub fn with_schema(
        name: String,
        schema: SchemaRef,
        batch_count: usize,
        batch_size: usize,
    ) -> Self {
        Self {
            name,
            schema,
            batch_count,
            current_batch: 0,
            batch_size,
        }
    }
}

#[async_trait]
impl Connector for MemoryConnector {
    async fn next_batch(&mut self) -> Option<Result<RecordBatch>> {
        if self.current_batch >= self.batch_count {
            return None;
        }

        // Simulate some delay
        sleep(Duration::from_millis(10)).await;

        let batch_num = self.current_batch;
        self.current_batch += 1;

        // Generate test data
        let mut timestamps = Vec::with_capacity(self.batch_size);
        let mut keys = Vec::with_capacity(self.batch_size);
        let mut values = Vec::with_capacity(self.batch_size);

        for i in 0..self.batch_size {
            let row_index = batch_num * self.batch_size + i;
            timestamps.push((row_index * 1000) as i64);
            keys.push(format!("key_{}", row_index % 10));
            values.push(row_index as i64);
        }

        let timestamp_array = Int64Array::from(timestamps);
        let key_array = StringArray::from(keys);
        let value_array = Int64Array::from(values);

        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![
                Arc::new(timestamp_array),
                Arc::new(key_array),
                Arc::new(value_array),
            ],
        );

        Some(batch.map_err(|e| e.into()))
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_memory_connector() {
        let mut connector = MemoryConnector::new("test".to_string(), 3, 5);

        assert_eq!(connector.name(), "test");
        assert_eq!(connector.schema().fields().len(), 3);

        let mut total_rows = 0;
        while let Some(batch_result) = connector.next_batch().await {
            let batch = batch_result.unwrap();
            total_rows += batch.num_rows();
        }

        assert_eq!(total_rows, 15);
    }

    #[tokio::test]
    async fn test_memory_connector_exhaustion() {
        let mut connector = MemoryConnector::new("test".to_string(), 2, 5);

        connector.next_batch().await.unwrap().unwrap();
        connector.next_batch().await.unwrap().unwrap();
        let result = connector.next_batch().await;

        assert!(result.is_none());
    }
}
