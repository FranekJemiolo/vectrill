//! Custom transformation framework for Vectrill

use crate::{error::Result, RecordBatch};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;

/// Trait for custom transformations
#[async_trait]
pub trait Transformation: Send + Sync {
    /// Apply the transformation to a record batch
    async fn apply(&mut self, batch: &RecordBatch) -> Result<RecordBatch>;

    /// Get the name of this transformation
    fn name(&self) -> &str;

    /// Get the output schema of this transformation
    fn output_schema(&self) -> SchemaRef;
}

/// Built-in transformation implementations
pub mod builtin;

/// Custom transformation registry
pub mod registry;

/// Re-export common transformation types
pub use builtin::*;
pub use registry::*;
