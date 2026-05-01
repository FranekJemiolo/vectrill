//! Operators for data processing

pub mod aggregation;
pub mod filter;
pub mod map;
pub mod pipeline;

pub use aggregation::{AggregateFunction, AggregateOperator};
pub use filter::FilterOperator;
pub use map::{MapOperator, ProjectionOperator};
pub use pipeline::{Operator, Pipeline};

/// Simple pass-through operator for benchmarking
#[derive(Debug, Default)]
pub struct PassThroughOperator;

impl Operator for PassThroughOperator {
    fn process(&mut self, batch: crate::RecordBatch) -> crate::error::Result<crate::RecordBatch> {
        Ok(batch)
    }
}
