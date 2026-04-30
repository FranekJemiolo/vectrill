//! Query optimization - operator fusion, predicate pushdown, column pruning

pub mod expr_optimizer;
pub mod fusion;

pub use expr_optimizer::ExprOptimizer;
pub use fusion::{
    FusableOperator, FusionSegment, FusedOperator, FusedExpr,
    is_fusable, is_stateful, is_boundary,
};
