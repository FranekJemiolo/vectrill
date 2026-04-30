//! Expression Intermediate Representation (IR)

use serde::{Deserialize, Serialize};
use std::fmt;

use super::{Operator, ScalarValue, UnaryOp};

/// Expression IR - represents compiled expressions
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    /// Column reference
    Column(String),
    /// Literal value
    Literal(ScalarValue),
    /// Binary expression
    Binary {
        left: Box<Expr>,
        op: Operator,
        right: Box<Expr>,
    },
    /// Unary expression
    Unary { op: UnaryOp, expr: Box<Expr> },
    /// Function call
    Function { name: String, args: Vec<Expr> },
    /// Cast expression
    Cast {
        expr: Box<Expr>,
        data_type: String, // Use string representation instead of DataType
    },
}

impl Expr {
    /// Create a column reference
    pub fn column(name: impl Into<String>) -> Self {
        Expr::Column(name.into())
    }

    /// Create a literal value
    pub fn literal(value: impl Into<ScalarValue>) -> Self {
        Expr::Literal(value.into())
    }

    /// Create a binary expression
    pub fn binary(left: Expr, op: Operator, right: Expr) -> Self {
        Expr::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    /// Create a unary expression
    pub fn unary(op: UnaryOp, expr: Expr) -> Self {
        Expr::Unary {
            op,
            expr: Box::new(expr),
        }
    }

    /// Create a function call
    pub fn function(name: impl Into<String>, args: Vec<Expr>) -> Self {
        Expr::Function {
            name: name.into(),
            args,
        }
    }

    /// Create a cast expression
    pub fn cast(expr: Expr, data_type: String) -> Self {
        Expr::Cast {
            expr: Box::new(expr),
            data_type,
        }
    }

    /// Get a string representation of the expression
    #[allow(
        clippy::inherent_to_string_shadow_display,
        clippy::to_string_in_format_args
    )]
    pub fn to_string(&self) -> String {
        match self {
            Expr::Column(name) => name.clone(),
            Expr::Literal(value) => value.to_string(),
            Expr::Binary { left, op, right } => {
                format!("({} {} {})", left, op, right)
            }
            Expr::Unary { op, expr } => {
                format!("{}{}", op, expr)
            }
            Expr::Function { name, args } => {
                let args_str = args
                    .iter()
                    .map(|arg| arg.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{}({})", name, args_str)
            }
            Expr::Cast { expr, data_type } => {
                format!("CAST({} AS {})", expr, data_type)
            }
        }
    }
}

impl fmt::Display for Expr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

/// Expression type information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ExprType {
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    Utf8,
    Timestamp,
    Date,
    Null,
    Unknown,
}

impl From<&arrow::datatypes::DataType> for ExprType {
    fn from(data_type: &arrow::datatypes::DataType) -> Self {
        match data_type {
            arrow::datatypes::DataType::Boolean => ExprType::Boolean,
            arrow::datatypes::DataType::Int8 => ExprType::Int8,
            arrow::datatypes::DataType::Int16 => ExprType::Int16,
            arrow::datatypes::DataType::Int32 => ExprType::Int32,
            arrow::datatypes::DataType::Int64 => ExprType::Int64,
            arrow::datatypes::DataType::UInt8 => ExprType::UInt8,
            arrow::datatypes::DataType::UInt16 => ExprType::UInt16,
            arrow::datatypes::DataType::UInt32 => ExprType::UInt32,
            arrow::datatypes::DataType::UInt64 => ExprType::UInt64,
            arrow::datatypes::DataType::Float32 => ExprType::Float32,
            arrow::datatypes::DataType::Float64 => ExprType::Float64,
            arrow::datatypes::DataType::Utf8 => ExprType::Utf8,
            arrow::datatypes::DataType::Timestamp(_, _) => ExprType::Timestamp,
            arrow::datatypes::DataType::Date32 => ExprType::Date,
            arrow::datatypes::DataType::Null => ExprType::Null,
            _ => ExprType::Unknown,
        }
    }
}

impl From<ExprType> for arrow::datatypes::DataType {
    fn from(expr_type: ExprType) -> Self {
        match expr_type {
            ExprType::Boolean => arrow::datatypes::DataType::Boolean,
            ExprType::Int8 => arrow::datatypes::DataType::Int8,
            ExprType::Int16 => arrow::datatypes::DataType::Int16,
            ExprType::Int32 => arrow::datatypes::DataType::Int32,
            ExprType::Int64 => arrow::datatypes::DataType::Int64,
            ExprType::UInt8 => arrow::datatypes::DataType::UInt8,
            ExprType::UInt16 => arrow::datatypes::DataType::UInt16,
            ExprType::UInt32 => arrow::datatypes::DataType::UInt32,
            ExprType::UInt64 => arrow::datatypes::DataType::UInt64,
            ExprType::Float32 => arrow::datatypes::DataType::Float32,
            ExprType::Float64 => arrow::datatypes::DataType::Float64,
            ExprType::Utf8 => arrow::datatypes::DataType::Utf8,
            ExprType::Timestamp => {
                arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
            }
            ExprType::Date => arrow::datatypes::DataType::Date32,
            ExprType::Null => arrow::datatypes::DataType::Null,
            ExprType::Unknown => panic!("Cannot convert Unknown type to Arrow DataType"),
        }
    }
}

/// Typed expression with resolved type information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TypedExpr {
    pub expr: Expr,
    pub data_type: ExprType,
    pub nullable: bool,
}

impl TypedExpr {
    pub fn new(expr: Expr, data_type: ExprType, nullable: bool) -> Self {
        Self {
            expr,
            data_type,
            nullable,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_expr_creation() {
        let col = Expr::column("test");
        assert_eq!(col.to_string(), "test");

        let lit = Expr::literal(42i64);
        assert_eq!(lit.to_string(), "42");

        let binary = Expr::binary(Expr::column("a"), Operator::Add, Expr::column("b"));
        assert_eq!(binary.to_string(), "(a + b)");

        let unary = Expr::unary(UnaryOp::Not, Expr::column("flag"));
        assert_eq!(unary.to_string(), "!flag");
    }

    #[test]
    fn test_type_conversion() {
        let arrow_type = arrow::datatypes::DataType::Int64;
        let expr_type = ExprType::from(&arrow_type);
        assert_eq!(expr_type, ExprType::Int64);

        let converted_back: arrow::datatypes::DataType = expr_type.into();
        assert_eq!(converted_back, arrow_type);
    }
}
