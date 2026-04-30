//! Connector interface and implementations for data ingestion

use crate::{error::Result, RecordBatch};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;

/// Trait for all data source connectors
#[async_trait]
pub trait Connector: Send + Sync {
    /// Get the next batch of data from the connector
    /// Returns None when the connector is exhausted
    async fn next_batch(&mut self) -> Option<Result<RecordBatch>>;

    /// Get the schema of the data produced by this connector
    fn schema(&self) -> SchemaRef;

    /// Get the name of this connector
    fn name(&self) -> &str;
}

pub mod file;
pub mod memory;

pub use file::FileConnector;
pub use memory::MemoryConnector;
