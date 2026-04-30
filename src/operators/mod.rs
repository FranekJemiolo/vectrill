//! Operators for data processing

pub mod pipeline;
pub mod filter;
pub mod map;

pub use pipeline::{Operator, Pipeline};
pub use filter::FilterOperator;
pub use map::{MapOperator, ProjectionOperator};
