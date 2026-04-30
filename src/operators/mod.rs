//! Operators for data processing

pub mod filter;
pub mod map;
pub mod pipeline;

pub use filter::FilterOperator;
pub use map::{MapOperator, ProjectionOperator};
pub use pipeline::{Operator, Pipeline};
