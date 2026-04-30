//! Vectrill: High-performance Arrow-native streaming engine
//! This library provides a single-node streaming execution engine with:
//! - Arrow-native columnar memory (zero-copy)
//! - Rust execution core
//! - Python DSL and control plane (via PyO3)

pub mod connectors;
pub mod error;
pub mod expression;
pub mod operators;
pub mod optimization;
pub mod planner;
pub mod sequencer;
pub mod streaming;
pub mod memory;
pub mod metrics;
pub mod performance;

#[cfg(feature = "cli")]
pub mod cli;

#[cfg(feature = "python")]
pub mod ffi;

#[cfg(feature = "web-ui")]
pub mod web;

pub use connectors::{Connector, FileConnector};
pub use error::{Result, VectrillError};
pub use expression::{Expr, ExprType, ScalarValue, UnaryOp};
pub use operators::Operator;
pub use optimization::{ExprOptimizer, FusedOperator};
pub use planner::{LogicalPlan, PhysicalPlan, ExecutionGraph};
pub use sequencer::Sequencer;
pub use streaming::{Watermark, window, WindowState};
pub use metrics::{Metric, MetricType, MetricsRegistry, global_registry};
pub use memory::{BufferPool, global_buffer_pool};
pub use performance::{Counter, CounterType, CounterRegistry, Timer, global_counter_registry};
pub use arrow::datatypes::SchemaRef;
pub use arrow::record_batch::RecordBatch;

/// Re-export Arrow for convenience
pub use arrow;

/// Library version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
