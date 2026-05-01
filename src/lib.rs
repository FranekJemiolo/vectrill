//! Vectrill: High-performance Arrow-native streaming engine
//! This library provides a single-node streaming execution engine with:
//! - Arrow-native columnar memory (zero-copy)
//! - Rust execution core
//! - Python DSL and control plane (via PyO3)

pub mod connectors;
pub mod error;
pub mod expression;
pub mod memory;
pub mod metrics;
pub mod operators;
pub mod optimization;
pub mod performance;
pub mod planner;
pub mod sequencer;
pub mod streaming;

#[cfg(feature = "cli")]
pub mod cli;

#[cfg(feature = "python")]
pub mod ffi;

#[cfg(feature = "web-ui")]
pub mod web;

pub use arrow::datatypes::SchemaRef;
pub use arrow::record_batch::RecordBatch;
pub use connectors::{Connector, FileConnector};
pub use error::{Result, VectrillError};
pub use expression::{Expr, ExprType, ScalarValue, UnaryOp};
pub use memory::{global_buffer_pool, BufferPool};
pub use metrics::{global_registry, Metric, MetricType, MetricsRegistry};
pub use operators::Operator;
pub use optimization::{ExprOptimizer, FusedOperator};
pub use performance::{global_counter_registry, Counter, CounterRegistry, CounterType, Timer};
pub use planner::{ExecutionGraph, LogicalPlan, PhysicalPlan};
pub use sequencer::Sequencer;
pub use streaming::{window, Watermark, WindowState};

/// Re-export Arrow for convenience
pub use arrow;

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
