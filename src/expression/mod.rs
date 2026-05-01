//! Expression Engine - Vectorized expression system for Vectrill

pub mod compiler;
pub mod ir;
pub mod operators;
pub mod physical;
pub mod scalar_value;
pub mod functions;

pub use compiler::{expr_from_string, compile_python_expression};
pub use ir::{Expr, ExprType, TypedExpr};
pub use operators::{Operator, UnaryOp, map_python_operator, map_python_bool_op, map_python_unary_op};
pub use scalar_value::ScalarValue;
pub use physical::{PhysicalExpr, create_physical_expr};
pub use functions::{FunctionRegistry, global_registry, FunctionSignature, FunctionMetadata};
