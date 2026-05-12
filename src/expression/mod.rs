//! Expression Engine - Vectorized expression system for Vectrill

pub mod arithmetic;
pub mod comparison;
pub mod compiler;
pub mod functions;
pub mod ir;
pub mod operators;
pub mod physical;
pub mod scalar_value;
pub mod vectorized;

pub use arithmetic::ArithmeticOps;
pub use comparison::ComparisonOps;
pub use compiler::{compile_python_expression, expr_from_string};
pub use functions::{global_registry, FunctionMetadata, FunctionRegistry, FunctionSignature};
pub use ir::{Expr, ExprType, TypedExpr};
pub use operators::{
    map_python_bool_op, map_python_operator, map_python_unary_op, Operator, UnaryOp,
};
pub use physical::{
    create_physical_expr, global_expression_cache, global_expression_counters, ExpressionCache,
    ExpressionCounters, ExpressionStats, PhysicalExpr,
};
pub use scalar_value::ScalarValue;
pub use vectorized::VectorizedOps;
