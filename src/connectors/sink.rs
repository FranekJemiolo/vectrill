//! Sink trait and implementations for data output

use crate::{error::Result, RecordBatch};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;

/// Trait for all data sink connectors
#[async_trait]
pub trait Sink: Send + Sync {
    /// Write a batch of data to the sink
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<()>;

    /// Get the schema of the data expected by this sink
    fn schema(&self) -> SchemaRef;

    /// Get the name of this sink
    fn name(&self) -> &str;

    /// Flush any pending data (optional)
    async fn flush(&mut self) -> Result<()> {
        // Default implementation does nothing
        Ok(())
    }

    /// Close the sink and clean up resources (optional)
    async fn close(&mut self) -> Result<()> {
        // Default implementation does nothing
        Ok(())
    }
}

// Re-export sink implementations from parent module
pub use crate::connectors::file_sink::FileSink;

#[cfg(feature = "kafka")]
pub use crate::connectors::kafka::KafkaSink;
