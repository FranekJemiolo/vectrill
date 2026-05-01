//! Expression Engine - Vectorized expression system for Vectrill

pub mod compiler;
pub mod functions;
pub mod ir;
pub mod operators;
pub mod physical;
pub mod scalar_value;

pub use compiler::{compile_python_expression, expr_from_string};
pub use functions::{global_registry, FunctionMetadata, FunctionRegistry, FunctionSignature};
pub use ir::{Expr, ExprType, TypedExpr};
pub use operators::{
    map_python_bool_op, map_python_operator, map_python_unary_op, Operator, UnaryOp,
};
pub use physical::{create_physical_expr, PhysicalExpr};
pub use scalar_value::ScalarValue;
