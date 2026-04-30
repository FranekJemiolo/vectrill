//! Vectrill - High-performance Arrow-native streaming engine
//!
//! This library provides a single-node streaming execution engine with:
//! - Arrow-native columnar memory (zero-copy)
//! - Rust execution core
//! - Python DSL and control plane (via PyO3)
//! - Spark-like API with Flink-like streaming semantics

pub mod connectors;
pub mod error;
pub mod ingestion;
pub mod operators;
pub mod sequencer;

pub use arrow::array::RecordBatch;
pub use arrow::datatypes::SchemaRef;
pub use error::{Result, VectrillError};
pub use operators::{Operator, Pipeline};

/// Re-export Arrow for convenience
pub use arrow;

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
