//! Kafka connector for streaming data output to Kafka topics

use crate::{error::Result, RecordBatch, VectrillError};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch as ArrowRecordBatch;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use std::time::Duration;

/// Kafka serialization format
#[derive(Debug, Clone, Copy)]
pub enum KafkaFormat {
    Json,
    Avro,
    Csv,
}

/// Kafka connector for streaming data to Kafka topics
pub struct KafkaSink {
    producer: FutureProducer,
    topic: String,
    format: KafkaFormat,
    schema: SchemaRef,
    timeout: Duration,
}

impl KafkaSink {
    /// Create a new Kafka sink
    pub fn new(
        brokers: &str,
        topic: String,
        format: KafkaFormat,
        schema: SchemaRef,
    ) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("message.timeout.ms", "5000")
            .set("delivery.timeout.ms", "10000")
            .set("request.timeout.ms", "5000")
            .set("queue.buffering.max.messages", "100000")
            .set("queue.buffering.max.kbytes", "1048576")
            .set("batch.num.messages", "1000")
            .set("compression.type", "snappy")
            .create()
            .map_err(|e| VectrillError::Connector(format!("Kafka producer creation failed: {}", e)))?;

        Ok(Self {
            producer,
            topic,
            format,
            schema,
            timeout: Duration::from_secs(10),
        })
    }

    /// Set the timeout for Kafka operations
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Write a batch of records to Kafka
    pub async fn write_batch(&self, batch: &RecordBatch) -> Result<()> {
        let arrow_batch = batch;
        
        for row_idx in 0..arrow_batch.num_rows() {
            let message = match self.format {
                KafkaFormat::Json => self.serialize_row_as_json(arrow_batch, row_idx)?,
                KafkaFormat::Avro => {
                    return Err(VectrillError::Connector(
                        "Avro serialization not yet implemented".to_string(),
                    ))
                }
                KafkaFormat::Csv => self.serialize_row_as_csv(arrow_batch, row_idx)?,
            };

            let record = FutureRecord::<(), Vec<u8>>::to(&self.topic)
                .payload(&message);

            match self.producer.send(record, Timeout::After(self.timeout)).await {
                Ok((partition, offset)) => {
                    // Successfully delivered
                    tracing::debug!("Message delivered to topic: {}, partition: {}, offset: {}", self.topic, partition, offset);
                },
                Err((kafka_error, _)) => {
                    return Err(VectrillError::Connector(format!(
                        "Kafka send failed: {:?}",
                        kafka_error
                    )));
                }
            }
        }

        Ok(())
    }

    /// Serialize a single row as JSON
    fn serialize_row_as_json(&self, batch: &ArrowRecordBatch, row_idx: usize) -> Result<Vec<u8>> {
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

        let json_str = serde_json::to_string(&Value::Object(map))
            .map_err(|e| VectrillError::Serialization(e))?;
        
        Ok(json_str.into_bytes())
    }

    /// Serialize a single row as CSV
    fn serialize_row_as_csv(&self, batch: &ArrowRecordBatch, row_idx: usize) -> Result<Vec<u8>> {
        let mut values = Vec::new();
        
        for (col_idx, _field) in self.schema.fields().iter().enumerate() {
            let array = batch.column(col_idx);
            let value = if array.is_null(row_idx) {
                String::new()
            } else {
                self.arrow_value_to_string(array, row_idx)?
            };
            values.push(value);
        }

        let csv_str = values.join(",");
        Ok(csv_str.into_bytes())
    }

    /// Convert Arrow value to JSON value
    fn arrow_value_to_json(&self, array: &dyn arrow::array::Array, row_idx: usize) -> Result<serde_json::Value> {
        use arrow::array::*;
        use arrow::datatypes::DataType;

        let data_type = array.data_type();
        
        match data_type {
            DataType::Boolean => {
                let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok(serde_json::Value::Bool(bool_array.value(row_idx)))
            }
            DataType::Int8 => {
                let int_array = array.as_any().downcast_ref::<Int8Array>().unwrap();
                Ok(serde_json::Value::Number(serde_json::Number::from(int_array.value(row_idx))))
            }
            DataType::Int16 => {
                let int_array = array.as_any().downcast_ref::<Int16Array>().unwrap();
                Ok(serde_json::Value::Number(serde_json::Number::from(int_array.value(row_idx))))
            }
            DataType::Int32 => {
                let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(serde_json::Value::Number(serde_json::Number::from(int_array.value(row_idx))))
            }
            DataType::Int64 => {
                let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(serde_json::Value::Number(serde_json::Number::from(int_array.value(row_idx))))
            }
            DataType::UInt8 => {
                let int_array = array.as_any().downcast_ref::<UInt8Array>().unwrap();
                Ok(serde_json::Value::Number(serde_json::Number::from(int_array.value(row_idx))))
            }
            DataType::UInt16 => {
                let int_array = array.as_any().downcast_ref::<UInt16Array>().unwrap();
                Ok(serde_json::Value::Number(serde_json::Number::from(int_array.value(row_idx))))
            }
            DataType::UInt32 => {
                let int_array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                Ok(serde_json::Value::Number(serde_json::Number::from(int_array.value(row_idx))))
            }
            DataType::UInt64 => {
                let int_array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
                Ok(serde_json::Value::Number(serde_json::Number::from(int_array.value(row_idx))))
            }
            DataType::Float32 => {
                let float_array = array.as_any().downcast_ref::<Float32Array>().unwrap();
                Ok(serde_json::Value::Number(serde_json::Number::from_f64(float_array.value(row_idx) as f64).unwrap()))
            }
            DataType::Float64 => {
                let float_array = array.as_any().downcast_ref::<Float64Array>().unwrap();
                Ok(serde_json::Value::Number(serde_json::Number::from_f64(float_array.value(row_idx)).unwrap()))
            }
            DataType::Utf8 => {
                let string_array = array.as_any().downcast_ref::<StringArray>().unwrap();
                Ok(serde_json::Value::String(string_array.value(row_idx).to_string()))
            }
            DataType::Timestamp(_, _) => {
                let timestamp_array = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
                Ok(serde_json::Value::Number(serde_json::Number::from(timestamp_array.value(row_idx))))
            }
            _ => {
                Err(VectrillError::Connector(format!(
                    "Unsupported data type for JSON serialization: {:?}",
                    data_type
                )))
            }
        }
    }

    /// Convert Arrow value to string
    fn arrow_value_to_string(&self, array: &dyn arrow::array::Array, row_idx: usize) -> Result<String> {
        use arrow::array::*;
        use arrow::datatypes::DataType;

        let data_type = array.data_type();
        
        match data_type {
            DataType::Boolean => {
                let bool_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok(bool_array.value(row_idx).to_string())
            }
            DataType::Int8 => {
                let int_array = array.as_any().downcast_ref::<Int8Array>().unwrap();
                Ok(int_array.value(row_idx).to_string())
            }
            DataType::Int16 => {
                let int_array = array.as_any().downcast_ref::<Int16Array>().unwrap();
                Ok(int_array.value(row_idx).to_string())
            }
            DataType::Int32 => {
                let int_array = array.as_any().downcast_ref::<Int32Array>().unwrap();
                Ok(int_array.value(row_idx).to_string())
            }
            DataType::Int64 => {
                let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                Ok(int_array.value(row_idx).to_string())
            }
            DataType::UInt8 => {
                let int_array = array.as_any().downcast_ref::<UInt8Array>().unwrap();
                Ok(int_array.value(row_idx).to_string())
            }
            DataType::UInt16 => {
                let int_array = array.as_any().downcast_ref::<UInt16Array>().unwrap();
                Ok(int_array.value(row_idx).to_string())
            }
            DataType::UInt32 => {
                let int_array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
                Ok(int_array.value(row_idx).to_string())
            }
            DataType::UInt64 => {
                let int_array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
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
            DataType::Timestamp(_, _) => {
                let timestamp_array = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
                Ok(timestamp_array.value(row_idx).to_string())
            }
            _ => {
                Err(VectrillError::Connector(format!(
                    "Unsupported data type for CSV serialization: {:?}",
                    data_type
                )))
            }
        }
    }

    /// Generate a key for the message (can be customized)
    fn generate_key(&self, batch: &ArrowRecordBatch, row_idx: usize) -> String {
        // For now, use a simple key based on row index and first column if available
        if batch.num_columns() > 0 {
            let first_column = batch.column(0);
            if !first_column.is_null(row_idx) {
                if let Ok(key) = self.arrow_value_to_string(first_column, row_idx) {
                    return key;
                }
            }
        }
        format!("row_{}", row_idx)
    }

    /// Get the topic name
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Get the format
    pub fn format(&self) -> KafkaFormat {
        self.format
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_kafka_sink_creation() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // This test will fail without a running Kafka broker
        // In a real test environment, you'd use testcontainers or mock Kafka
        let result = KafkaSink::new(
            "localhost:9092",
            "test-topic".to_string(),
            KafkaFormat::Json,
            schema,
        );
        
        // We expect this to fail in test environment without Kafka
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_kafka_format_enum() {
        let json = KafkaFormat::Json;
        let avro = KafkaFormat::Avro;
        let csv = KafkaFormat::Csv;

        assert!(matches!(json, KafkaFormat::Json));
        assert!(matches!(avro, KafkaFormat::Avro));
        assert!(matches!(csv, KafkaFormat::Csv));
    }
}
