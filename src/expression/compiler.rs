//! Expression compiler - compile Python AST to expression IR

use serde::{Deserialize, Serialize};

#[allow(unused_imports)]
use std::collections::HashMap;

#[allow(unused_imports)]
use crate::error::VectrillError;
#[allow(unused_imports)]
use crate::expression::{
    map_python_bool_op, map_python_operator, map_python_unary_op, Expr, Operator, ScalarValue,
    UnaryOp,
};

/// Python AST node representation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PythonASTNode {
    pub node_type: String,
    pub value: Option<serde_json::Value>,
    pub children: Vec<PythonASTNode>,
}

/// Expression compiler result
#[derive(Debug, Clone)]
pub struct CompileResult {
    pub expr: Expr,
    pub errors: Vec<String>,
}

/// Expression compiler
pub struct ExpressionCompiler {
    /// Available column names for validation
    available_columns: Option<std::collections::HashSet<String>>,
}

impl Default for ExpressionCompiler {
    fn default() -> Self {
        Self::new()
    }
}

impl ExpressionCompiler {
    /// Create a new expression compiler
    pub fn new() -> Self {
        Self {
            available_columns: None,
        }
    }

    /// Create a compiler with available columns for validation
    pub fn with_columns(available_columns: std::collections::HashSet<String>) -> Self {
        Self {
            available_columns: Some(available_columns),
        }
    }

    /// Compile a Python AST node to an expression
    pub fn compile(&self, ast_node: &PythonASTNode) -> CompileResult {
        let mut errors = Vec::new();
        let expr = match self.compile_node(ast_node, &mut errors) {
            Ok(expr) => expr,
            Err(e) => {
                errors.push(e);
                Expr::Literal(ScalarValue::Null)
            }
        };

        CompileResult { expr, errors }
    }

    /// Compile a single AST node
    fn compile_node(&self, node: &PythonASTNode, errors: &mut Vec<String>) -> Result<Expr, String> {
        match node.node_type.as_str() {
            "Name" => self.compile_name(node, errors),
            "Constant" => self.compile_constant(node, errors),
            "BinOp" => self.compile_bin_op(node, errors),
            "BoolOp" => self.compile_bool_op(node, errors),
            "UnaryOp" => self.compile_unary_op(node, errors),
            "Compare" => self.compile_compare(node, errors),
            "Call" => self.compile_call(node, errors),
            _ => Err(format!("Unsupported AST node type: {}", node.node_type)),
        }
    }

    /// Compile a Name node (column reference)
    fn compile_name(
        &self,
        node: &PythonASTNode,
        _errors: &mut Vec<String>,
    ) -> Result<Expr, String> {
        let name = node
            .value
            .as_ref()
            .and_then(|v| v.as_str())
            .ok_or("Name node missing value")?;

        // Validate column name if available columns are set
        if let Some(ref available) = self.available_columns {
            if !available.contains(name) {
                return Err(format!("Column '{}' not found in schema", name));
            }
        }

        Ok(Expr::Column(name.to_string()))
    }

    /// Compile a Constant node (literal value)
    fn compile_constant(
        &self,
        node: &PythonASTNode,
        _errors: &mut Vec<String>,
    ) -> Result<Expr, String> {
        let value = node.value.as_ref().ok_or("Constant node missing value")?;

        let scalar_value = match value {
            serde_json::Value::Null => ScalarValue::Null,
            serde_json::Value::Bool(b) => ScalarValue::Boolean(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    ScalarValue::Int64(i)
                } else if let Some(f) = n.as_f64() {
                    ScalarValue::Float64(f)
                } else {
                    return Err("Invalid numeric constant".to_string());
                }
            }
            serde_json::Value::String(s) => ScalarValue::Utf8(s.clone()),
            _ => return Err(format!("Unsupported constant type: {:?}", value)),
        };

        Ok(Expr::Literal(scalar_value))
    }

    /// Compile a BinOp node (binary operation)
    fn compile_bin_op(
        &self,
        node: &PythonASTNode,
        errors: &mut Vec<String>,
    ) -> Result<Expr, String> {
        if node.children.len() != 3 {
            return Err("BinOp node should have 3 children (left, op, right)".to_string());
        }

        let left = self.compile_node(&node.children[0], errors)?;
        let op_node = &node.children[1];
        let right = self.compile_node(&node.children[2], errors)?;

        let op_str = op_node
            .value
            .as_ref()
            .and_then(|v| v.as_str())
            .ok_or("BinOp operator missing value")?;

        let operator = map_python_operator(op_str)
            .ok_or(format!("Unsupported binary operator: {}", op_str))?;

        Ok(Expr::binary(left, operator, right))
    }

    /// Compile a BoolOp node (boolean operation)
    fn compile_bool_op(
        &self,
        node: &PythonASTNode,
        errors: &mut Vec<String>,
    ) -> Result<Expr, String> {
        if node.children.len() < 2 {
            return Err("BoolOp node should have at least 2 children".to_string());
        }

        let op_node = &node.children[0];
        let op_str = op_node
            .value
            .as_ref()
            .and_then(|v| v.as_str())
            .ok_or("BoolOp operator missing value")?;

        let operator = map_python_bool_op(op_str)
            .ok_or(format!("Unsupported boolean operator: {}", op_str))?;

        // BoolOp can have multiple operands, chain them
        let mut result = self.compile_node(&node.children[1], errors)?;
        for i in 2..node.children.len() {
            let right = self.compile_node(&node.children[i], errors)?;
            result = Expr::binary(result, operator, right);
        }

        Ok(result)
    }

    /// Compile a UnaryOp node (unary operation)
    fn compile_unary_op(
        &self,
        node: &PythonASTNode,
        errors: &mut Vec<String>,
    ) -> Result<Expr, String> {
        if node.children.len() != 2 {
            return Err("UnaryOp node should have 2 children (op, operand)".to_string());
        }

        let op_node = &node.children[0];
        let operand = self.compile_node(&node.children[1], errors)?;

        let op_str = op_node
            .value
            .as_ref()
            .and_then(|v| v.as_str())
            .ok_or("UnaryOp operator missing value")?;

        let operator =
            map_python_unary_op(op_str).ok_or(format!("Unsupported unary operator: {}", op_str))?;

        Ok(Expr::unary(operator, operand))
    }

    /// Compile a Compare node (comparison operation)
    fn compile_compare(
        &self,
        node: &PythonASTNode,
        errors: &mut Vec<String>,
    ) -> Result<Expr, String> {
        if node.children.len() != 3 {
            return Err("Compare node should have 3 children (left, op, right)".to_string());
        }

        let left = self.compile_node(&node.children[0], errors)?;
        let op_node = &node.children[1];
        let right = self.compile_node(&node.children[2], errors)?;

        let op_str = op_node
            .value
            .as_ref()
            .and_then(|v| v.as_str())
            .ok_or("Compare operator missing value")?;

        let operator = map_python_operator(op_str)
            .ok_or(format!("Unsupported comparison operator: {}", op_str))?;

        Ok(Expr::binary(left, operator, right))
    }

    /// Compile a Call node (function call)
    fn compile_call(&self, node: &PythonASTNode, errors: &mut Vec<String>) -> Result<Expr, String> {
        if node.children.is_empty() {
            return Err("Call node should have at least one child".to_string());
        }

        // First child should be the function name
        let func_node = &node.children[0];
        let func_name = match func_node.node_type.as_str() {
            "Name" => func_node
                .value
                .as_ref()
                .and_then(|v| v.as_str())
                .ok_or("Function name missing")?,
            _ => return Err("Function name must be a Name node".to_string()),
        };

        // Remaining children are arguments
        let mut args = Vec::new();
        for i in 1..node.children.len() {
            let arg = self.compile_node(&node.children[i], errors)?;
            args.push(arg);
        }

        // Handle special functions
        match func_name {
            "cast" => {
                if args.len() != 2 {
                    return Err("cast() function requires exactly 2 arguments".to_string());
                }

                // For now, just return the first argument (proper casting would be handled later)
                Ok(args[0].clone())
            }
            "abs" | "length" => Ok(Expr::function(func_name.to_string(), args)),
            _ => Ok(Expr::function(func_name.to_string(), args)),
        }
    }
}

/// Compile a Python expression string to an expression
pub fn compile_python_expression(
    expr_str: &str,
    available_columns: Option<std::collections::HashSet<String>>,
) -> CompileResult {
    // For now, this is a simplified implementation
    // In a real implementation, we would use Python's ast module to parse the expression

    // Simple parsing for basic expressions
    if let Ok(expr) = parse_simple_expression(expr_str) {
        let compiler = ExpressionCompiler::with_columns(available_columns.unwrap_or_default());
        compiler.compile(&expr)
    } else {
        CompileResult {
            expr: Expr::Literal(ScalarValue::Null),
            errors: vec!["Failed to parse expression".to_string()],
        }
    }
}

/// Simple expression parser (placeholder implementation)
fn parse_simple_expression(expr_str: &str) -> Result<PythonASTNode, String> {
    let expr_str = expr_str.trim();

    // Handle literals
    if let Ok(int_val) = expr_str.parse::<i64>() {
        return Ok(PythonASTNode {
            node_type: "Constant".to_string(),
            value: Some(serde_json::Value::Number(int_val.into())),
            children: Vec::new(),
        });
    }

    if let Ok(float_val) = expr_str.parse::<f64>() {
        return Ok(PythonASTNode {
            node_type: "Constant".to_string(),
            value: Some(serde_json::Value::Number(
                serde_json::Number::from_f64(float_val).unwrap(),
            )),
            children: Vec::new(),
        });
    }

    if let Ok(bool_val) = expr_str.parse::<bool>() {
        return Ok(PythonASTNode {
            node_type: "Constant".to_string(),
            value: Some(serde_json::Value::Bool(bool_val)),
            children: Vec::new(),
        });
    }

    // Handle string literals
    if (expr_str.starts_with('"') && expr_str.ends_with('"'))
        || (expr_str.starts_with('\'') && expr_str.ends_with('\''))
    {
        let content = &expr_str[1..expr_str.len() - 1];
        return Ok(PythonASTNode {
            node_type: "Constant".to_string(),
            value: Some(serde_json::Value::String(content.to_string())),
            children: Vec::new(),
        });
    }

    // Handle column names (simple identifiers)
    if is_valid_identifier(expr_str) {
        return Ok(PythonASTNode {
            node_type: "Name".to_string(),
            value: Some(serde_json::Value::String(expr_str.to_string())),
            children: Vec::new(),
        });
    }

    // Handle simple binary operations
    if let Some((left, op, right)) = parse_binary_operation(expr_str) {
        return Ok(PythonASTNode {
            node_type: "BinOp".to_string(),
            value: None,
            children: vec![
                left,
                PythonASTNode {
                    node_type: "Operator".to_string(),
                    value: Some(serde_json::Value::String(op)),
                    children: Vec::new(),
                },
                right,
            ],
        });
    }

    Err(format!("Unable to parse expression: {}", expr_str))
}

/// Check if a string is a valid identifier
fn is_valid_identifier(s: &str) -> bool {
    if s.is_empty() {
        return false;
    }

    s.chars().all(|c| c.is_alphanumeric() || c == '_')
}

/// Parse simple binary operations
fn parse_binary_operation(expr_str: &str) -> Option<(PythonASTNode, String, PythonASTNode)> {
    // Simple parsing for common operators
    let operators = vec!["==", "!=", "<=", ">=", "<", ">", "+", "-", "*", "/"];

    for op in &operators {
        if let Some(pos) = expr_str.find(op) {
            if pos > 0 && pos + op.len() < expr_str.len() {
                let left_str = expr_str[..pos].trim();
                let right_str = expr_str[pos + op.len()..].trim();

                if let Ok(left) = parse_simple_expression(left_str) {
                    if let Ok(right) = parse_simple_expression(right_str) {
                        return Some((left, op.to_string(), right));
                    }
                }
            }
        }
    }

    None
}

/// Create a simple expression from a string (convenience function)
pub fn expr_from_string(expr_str: &str) -> Expr {
    let result = compile_python_expression(expr_str, None);
    if !result.errors.is_empty() {
        // Return a null expression if compilation failed
        Expr::Literal(ScalarValue::Null)
    } else {
        result.expr
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_literal_parsing() {
        let expr = expr_from_string("42");
        assert_eq!(expr, Expr::Literal(ScalarValue::Int64(42)));

        let expr = expr_from_string("3.14");
        assert_eq!(expr, Expr::Literal(ScalarValue::Float64(3.14)));

        let expr = expr_from_string("true");
        assert_eq!(expr, Expr::Literal(ScalarValue::Boolean(true)));

        let expr = expr_from_string("'hello'");
        assert_eq!(expr, Expr::Literal(ScalarValue::Utf8("hello".to_string())));
    }

    #[test]
    fn test_column_parsing() {
        let expr = expr_from_string("column_name");
        assert_eq!(expr, Expr::Column("column_name".to_string()));
    }

    #[test]
    fn test_binary_operation_parsing() {
        let expr = expr_from_string("a + b");
        if let Expr::Binary { left, op, right } = expr {
            assert_eq!(*left, Expr::Column("a".to_string()));
            assert_eq!(op, Operator::Add);
            assert_eq!(*right, Expr::Column("b".to_string()));
        } else {
            panic!("Expected binary expression");
        }
    }

    #[test]
    fn test_comparison_parsing() {
        let expr = expr_from_string("x > 10");
        if let Expr::Binary { left, op, right } = expr {
            assert_eq!(*left, Expr::Column("x".to_string()));
            assert_eq!(op, Operator::Gt);
            assert_eq!(*right, Expr::Literal(ScalarValue::Int64(10)));
        } else {
            panic!("Expected binary expression");
        }
    }

    #[test]
    fn test_compiler_with_validation() {
        let available_columns = ["col1", "col2"].iter().map(|s| s.to_string()).collect();
        let compiler = ExpressionCompiler::with_columns(available_columns);

        let valid_node = PythonASTNode {
            node_type: "Name".to_string(),
            value: Some(serde_json::Value::String("col1".to_string())),
            children: Vec::new(),
        };

        let result = compiler.compile(&valid_node);
        assert!(result.errors.is_empty());
        assert_eq!(result.expr, Expr::Column("col1".to_string()));

        let invalid_node = PythonASTNode {
            node_type: "Name".to_string(),
            value: Some(serde_json::Value::String("col3".to_string())),
            children: Vec::new(),
        };

        let result = compiler.compile(&invalid_node);
        assert!(!result.errors.is_empty());
    }
}
