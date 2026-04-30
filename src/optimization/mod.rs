//! Query optimization - operator fusion, predicate pushdown, column pruning

pub mod fusion;

pub use fusion::{
    FusableOperator, FusionSegment, FusedOperator, FusedExpr,
    is_fusable, is_stateful, is_boundary,
};
