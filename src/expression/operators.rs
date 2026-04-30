//! Expression operators

use serde::{Deserialize, Serialize};
use std::fmt;

/// Binary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Operator {
    // Comparison operators
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    
    // Arithmetic operators
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    
    // Boolean operators
    And,
    Or,
    
    // String operators
    Like,
    ILike,
    NotLike,
    NotILike,
    
    // Array operators
    In,
    NotIn,
    
    // Null operators
    IsNull,
    IsNotNull,
}

impl fmt::Display for Operator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Operator::Eq => write!(f, "="),
            Operator::NotEq => write!(f, "!="),
            Operator::Lt => write!(f, "<"),
            Operator::LtEq => write!(f, "<="),
            Operator::Gt => write!(f, ">"),
            Operator::GtEq => write!(f, ">="),
            Operator::Add => write!(f, "+"),
            Operator::Sub => write!(f, "-"),
            Operator::Mul => write!(f, "*"),
            Operator::Div => write!(f, "/"),
            Operator::Mod => write!(f, "%"),
            Operator::And => write!(f, "AND"),
            Operator::Or => write!(f, "OR"),
            Operator::Like => write!(f, "LIKE"),
            Operator::ILike => write!(f, "ILIKE"),
            Operator::NotLike => write!(f, "NOT LIKE"),
            Operator::NotILike => write!(f, "NOT ILIKE"),
            Operator::In => write!(f, "IN"),
            Operator::NotIn => write!(f, "NOT IN"),
            Operator::IsNull => write!(f, "IS NULL"),
            Operator::IsNotNull => write!(f, "IS NOT NULL"),
        }
    }
}

/// Unary operators
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnaryOp {
    Not,
    Neg,
    IsNull,
    IsNotNull,
}

impl fmt::Display for UnaryOp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UnaryOp::Not => write!(f, "!"),
            UnaryOp::Neg => write!(f, "-"),
            UnaryOp::IsNull => write!(f, "IS NULL"),
            UnaryOp::IsNotNull => write!(f, "IS NOT NULL"),
        }
    }
}

/// Operator precedence levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Precedence {
    Lowest = 0,
    Or = 1,
    And = 2,
    Comparison = 3,
    AddSub = 4,
    MulDiv = 5,
    Unary = 6,
    Highest = 7,
}

impl Operator {
    /// Get the precedence level of this operator
    pub fn precedence(&self) -> Precedence {
        match self {
            Operator::Or => Precedence::Or,
            Operator::And => Precedence::And,
            Operator::Eq | Operator::NotEq | Operator::Lt | Operator::LtEq | 
            Operator::Gt | Operator::GtEq => Precedence::Comparison,
            Operator::Add | Operator::Sub => Precedence::AddSub,
            Operator::Mul | Operator::Div | Operator::Mod => Precedence::MulDiv,
            Operator::Like | Operator::ILike | Operator::NotLike | Operator::NotILike => Precedence::Comparison,
            Operator::In | Operator::NotIn => Precedence::Comparison,
            Operator::IsNull | Operator::IsNotNull => Precedence::Comparison,
        }
    }

    /// Check if this operator is left-associative
    pub fn is_left_associative(&self) -> bool {
        match self {
            Operator::Add | Operator::Sub | Operator::Mul | Operator::Div | Operator::Mod => true,
            Operator::And | Operator::Or => true,
            Operator::Eq | Operator::NotEq | Operator::Lt | Operator::LtEq | 
            Operator::Gt | Operator::GtEq => true,
            Operator::Like | Operator::ILike | Operator::NotLike | Operator::NotILike => true,
            Operator::In | Operator::NotIn => true,
            Operator::IsNull | Operator::IsNotNull => true,
        }
    }

    /// Check if this operator is a comparison operator
    pub fn is_comparison(&self) -> bool {
        matches!(
            self,
            Operator::Eq | Operator::NotEq | Operator::Lt | Operator::LtEq |
            Operator::Gt | Operator::GtEq | Operator::Like | Operator::ILike |
            Operator::NotLike | Operator::NotILike | Operator::In | Operator::NotIn |
            Operator::IsNull | Operator::IsNotNull
        )
    }

    /// Check if this operator is arithmetic
    pub fn is_arithmetic(&self) -> bool {
        matches!(
            self,
            Operator::Add | Operator::Sub | Operator::Mul | Operator::Div | Operator::Mod
        )
    }

    /// Check if this operator is logical
    pub fn is_logical(&self) -> bool {
        matches!(self, Operator::And | Operator::Or)
    }
}

impl UnaryOp {
    /// Get the precedence level of this unary operator
    pub fn precedence(&self) -> Precedence {
        Precedence::Unary
    }
}

/// Function to map Python AST operator strings to our Operator enum
pub fn map_python_operator(op_str: &str) -> Option<Operator> {
    match op_str {
        "Eq" => Some(Operator::Eq),
        "NotEq" => Some(Operator::NotEq),
        "Lt" => Some(Operator::Lt),
        "LtE" => Some(Operator::LtEq),
        "Gt" => Some(Operator::Gt),
        "GtE" => Some(Operator::GtEq),
        "Add" => Some(Operator::Add),
        "Sub" => Some(Operator::Sub),
        "Mult" => Some(Operator::Mul),
        "Div" => Some(Operator::Div),
        "Mod" => Some(Operator::Mod),
        "And" => Some(Operator::And),
        "Or" => Some(Operator::Or),
        _ => None,
    }
}

/// Function to map Python boolean operator strings to our Operator enum
pub fn map_python_bool_op(op_str: &str) -> Option<Operator> {
    match op_str {
        "And" => Some(Operator::And),
        "Or" => Some(Operator::Or),
        _ => None,
    }
}

/// Function to map Python unary operator strings to our UnaryOp enum
pub fn map_python_unary_op(op_str: &str) -> Option<UnaryOp> {
    match op_str {
        "Not" => Some(UnaryOp::Not),
        "USub" => Some(UnaryOp::Neg),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operator_display() {
        assert_eq!(Operator::Eq.to_string(), "=");
        assert_eq!(Operator::Add.to_string(), "+");
        assert_eq!(Operator::And.to_string(), "AND");
        assert_eq!(Operator::Or.to_string(), "OR");
    }

    #[test]
    fn test_unary_op_display() {
        assert_eq!(UnaryOp::Not.to_string(), "!");
        assert_eq!(UnaryOp::Neg.to_string(), "-");
    }

    #[test]
    fn test_operator_properties() {
        assert!(Operator::Add.is_arithmetic());
        assert!(Operator::And.is_logical());
        assert!(Operator::Eq.is_comparison());
        assert!(!Operator::Add.is_logical());
        assert!(!Operator::And.is_arithmetic());
    }

    #[test]
    fn test_precedence() {
        assert!(Operator::Mul.precedence() > Operator::Add.precedence());
        assert!(Operator::Add.precedence() > Operator::Eq.precedence());
        assert!(Operator::Eq.precedence() > Operator::And.precedence());
        assert!(Operator::And.precedence() > Operator::Or.precedence());
    }

    #[test]
    fn test_python_operator_mapping() {
        assert_eq!(map_python_operator("Add"), Some(Operator::Add));
        assert_eq!(map_python_operator("Eq"), Some(Operator::Eq));
        assert_eq!(map_python_operator("Unknown"), None);
    }

    #[test]
    fn test_python_bool_op_mapping() {
        assert_eq!(map_python_bool_op("And"), Some(Operator::And));
        assert_eq!(map_python_bool_op("Or"), Some(Operator::Or));
        assert_eq!(map_python_bool_op("Unknown"), None);
    }

    #[test]
    fn test_python_unary_op_mapping() {
        assert_eq!(map_python_unary_op("Not"), Some(UnaryOp::Not));
        assert_eq!(map_python_unary_op("USub"), Some(UnaryOp::Neg));
        assert_eq!(map_python_unary_op("Unknown"), None);
    }
}
