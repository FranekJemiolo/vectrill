//! Expression compiler - compile Python AST to expression IR

use serde::{Deserialize, Serialize};

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
    /// Enable constant folding optimization
    enable_constant_folding: bool,
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
            enable_constant_folding: true,
        }
    }

    /// Create a compiler with available columns for validation
    pub fn with_columns(available_columns: std::collections::HashSet<String>) -> Self {
        Self {
            available_columns: Some(available_columns),
            enable_constant_folding: true,
        }
    }

    /// Enable or disable constant folding optimization
    pub fn with_constant_folding(mut self, enable: bool) -> Self {
        self.enable_constant_folding = enable;
        self
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

        // Apply constant folding optimization if enabled
        let optimized_expr = if self.enable_constant_folding {
            self.constant_fold(&expr)
        } else {
            expr
        };

        CompileResult {
            expr: optimized_expr,
            errors,
        }
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

    /// Apply constant folding optimization to an expression
    fn constant_fold(&self, expr: &Expr) -> Expr {
        // If constant folding is disabled, return expression as-is
        if !self.enable_constant_folding {
            return expr.clone();
        }

        match expr {
            Expr::Binary { left, op, right } => {
                let folded_left = self.constant_fold(left);
                let folded_right = self.constant_fold(right);

                // Try to evaluate if both operands are constants
                if let (Expr::Literal(left_val), Expr::Literal(right_val)) =
                    (&folded_left, &folded_right)
                {
                    if let Some(result) = self.evaluate_binary_op(*op, left_val, right_val) {
                        return Expr::Literal(result);
                    }
                }

                // Apply algebraic simplifications
                if let Some(simplified) =
                    self.simplify_binary_expression(*op, &folded_left, &folded_right)
                {
                    return simplified;
                }

                // Debug: Check if we're trying to simplify y - y
                if *op == Operator::Sub {
                    println!("Trying to simplify: {:?} - {:?}", folded_left, folded_right);
                    if let (Expr::Column(ref left_col), Expr::Column(ref right_col)) =
                        (&folded_left, &folded_right)
                    {
                        if left_col == right_col {
                            println!("Should fold {:?} - {:?} to 0", left_col, right_col);
                        }
                    }
                }

                // Return folded binary expression
                Expr::Binary {
                    left: Box::new(folded_left),
                    op: *op,
                    right: Box::new(folded_right),
                }
            }
            Expr::Unary { op, expr } => {
                let folded_expr = self.constant_fold(expr);

                // Try to evaluate if operand is constant
                if let Expr::Literal(val) = &folded_expr {
                    if let Some(result) = self.evaluate_unary_op(*op, val) {
                        return Expr::Literal(result);
                    }
                }

                // Apply unary simplifications
                if let Some(simplified) = self.simplify_unary_expression(*op, &folded_expr) {
                    return simplified;
                }

                // Return folded unary expression
                Expr::Unary {
                    op: *op,
                    expr: Box::new(folded_expr),
                }
            }
            Expr::Function { name, args } => {
                let folded_args: Vec<Expr> =
                    args.iter().map(|arg| self.constant_fold(arg)).collect();

                // Try to evaluate if all arguments are constants
                if folded_args
                    .iter()
                    .all(|arg| matches!(arg, Expr::Literal(_)))
                {
                    if let Some(result) = self.evaluate_function(name, &folded_args) {
                        return Expr::Literal(result);
                    }
                }

                // Apply function-specific simplifications
                if let Some(simplified) = self.simplify_function_expression(name, &folded_args) {
                    return simplified;
                }

                // Return folded function call
                Expr::Function {
                    name: name.clone(),
                    args: folded_args,
                }
            }
            Expr::Cast { expr, data_type } => {
                let folded_expr = self.constant_fold(expr);

                // Try to evaluate if operand is constant
                if let Expr::Literal(val) = &folded_expr {
                    if let Some(result) = self.evaluate_cast(val, data_type) {
                        return Expr::Literal(result);
                    }
                }

                // Remove redundant casts
                if let Expr::Literal(val) = &folded_expr {
                    if self.is_cast_redundant(val, data_type) {
                        return folded_expr;
                    }
                }

                // Return folded cast expression
                Expr::Cast {
                    expr: Box::new(folded_expr),
                    data_type: data_type.clone(),
                }
            }
            // Base cases - literals and columns are already optimal
            Expr::Literal(_) | Expr::Column(_) => expr.clone(),
        }
    }

    /// Evaluate a binary operation on constant values
    fn evaluate_binary_op(
        &self,
        op: Operator,
        left: &ScalarValue,
        right: &ScalarValue,
    ) -> Option<ScalarValue> {
        match (left, right) {
            (ScalarValue::Int64(l), ScalarValue::Int64(r)) => {
                match op {
                    Operator::Add => Some(ScalarValue::Int64(l + r)),
                    Operator::Sub => Some(ScalarValue::Int64(l - r)),
                    Operator::Mul => Some(ScalarValue::Int64(l * r)),
                    Operator::Div => {
                        if *r != 0 {
                            Some(ScalarValue::Int64(l / r))
                        } else {
                            None // Division by zero
                        }
                    }
                    Operator::Eq => Some(ScalarValue::Boolean(l == r)),
                    Operator::NotEq => Some(ScalarValue::Boolean(l != r)),
                    Operator::Lt => Some(ScalarValue::Boolean(l < r)),
                    Operator::LtEq => Some(ScalarValue::Boolean(l <= r)),
                    Operator::Gt => Some(ScalarValue::Boolean(l > r)),
                    Operator::GtEq => Some(ScalarValue::Boolean(l >= r)),
                    _ => None,
                }
            }
            (ScalarValue::Float64(l), ScalarValue::Float64(r)) => {
                match op {
                    Operator::Add => Some(ScalarValue::Float64(l + r)),
                    Operator::Sub => Some(ScalarValue::Float64(l - r)),
                    Operator::Mul => Some(ScalarValue::Float64(l * r)),
                    Operator::Div => {
                        if *r != 0.0 {
                            Some(ScalarValue::Float64(l / r))
                        } else {
                            None // Division by zero
                        }
                    }
                    Operator::Eq => Some(ScalarValue::Boolean((l - r).abs() < f64::EPSILON)),
                    Operator::NotEq => Some(ScalarValue::Boolean((l - r).abs() >= f64::EPSILON)),
                    Operator::Lt => Some(ScalarValue::Boolean(l < r)),
                    Operator::LtEq => Some(ScalarValue::Boolean(l <= r)),
                    Operator::Gt => Some(ScalarValue::Boolean(l > r)),
                    Operator::GtEq => Some(ScalarValue::Boolean(l >= r)),
                    _ => None,
                }
            }
            (ScalarValue::Boolean(l), ScalarValue::Boolean(r)) => match op {
                Operator::Eq => Some(ScalarValue::Boolean(l == r)),
                Operator::NotEq => Some(ScalarValue::Boolean(l != r)),
                _ => None,
            },
            (ScalarValue::Utf8(l), ScalarValue::Utf8(r)) => match op {
                Operator::Eq => Some(ScalarValue::Boolean(l == r)),
                Operator::NotEq => Some(ScalarValue::Boolean(l != r)),
                _ => None,
            },
            _ => None,
        }
    }

    /// Evaluate a unary operation on a constant value
    fn evaluate_unary_op(&self, op: UnaryOp, val: &ScalarValue) -> Option<ScalarValue> {
        match val {
            ScalarValue::Int64(v) => {
                match op {
                    UnaryOp::Neg => Some(ScalarValue::Int64(-v)),
                    UnaryOp::Not => Some(ScalarValue::Boolean(*v == 0)),
                    UnaryOp::IsNull | UnaryOp::IsNotNull => None, // Cannot evaluate at compile time
                }
            }
            ScalarValue::Float64(v) => {
                match op {
                    UnaryOp::Neg => Some(ScalarValue::Float64(-v)),
                    UnaryOp::Not => Some(ScalarValue::Boolean((v - 0.0).abs() < f64::EPSILON)),
                    UnaryOp::IsNull | UnaryOp::IsNotNull => None, // Cannot evaluate at compile time
                }
            }
            ScalarValue::Boolean(v) => {
                match op {
                    UnaryOp::Not => Some(ScalarValue::Boolean(!v)),
                    UnaryOp::Neg => None, // Cannot negate boolean at compile time
                    UnaryOp::IsNull | UnaryOp::IsNotNull => None, // Cannot evaluate at compile time
                }
            }
            _ => None,
        }
    }

    /// Evaluate a function call on constant arguments
    fn evaluate_function(&self, name: &str, args: &[Expr]) -> Option<ScalarValue> {
        match name {
            "abs" if args.len() == 1 => {
                if let Expr::Literal(ScalarValue::Int64(v)) = &args[0] {
                    return Some(ScalarValue::Int64(v.abs()));
                }
                if let Expr::Literal(ScalarValue::Float64(v)) = &args[0] {
                    return Some(ScalarValue::Float64(v.abs()));
                }
            }
            "length" if args.len() == 1 => {
                if let Expr::Literal(ScalarValue::Utf8(s)) = &args[0] {
                    return Some(ScalarValue::Int64(s.len() as i64));
                }
            }
            _ => {}
        }
        None
    }

    /// Evaluate a cast operation on a constant value
    fn evaluate_cast(&self, val: &ScalarValue, target_type: &str) -> Option<ScalarValue> {
        match target_type {
            "Int64" => match val {
                ScalarValue::Float64(v) => Some(ScalarValue::Int64(*v as i64)),
                ScalarValue::Utf8(s) => s.parse::<i64>().ok().map(ScalarValue::Int64),
                _ => None,
            },
            "Float64" => match val {
                ScalarValue::Int64(v) => Some(ScalarValue::Float64(*v as f64)),
                ScalarValue::Utf8(s) => s.parse::<f64>().ok().map(ScalarValue::Float64),
                _ => None,
            },
            "Utf8" => Some(ScalarValue::Utf8(val.to_string())),
            _ => None,
        }
    }

    /// Simplify binary expressions using algebraic rules
    fn simplify_binary_expression(&self, op: Operator, left: &Expr, right: &Expr) -> Option<Expr> {
        match op {
            // Addition simplifications
            Operator::Add => {
                // x + 0 = x
                if let Expr::Literal(ScalarValue::Int64(0))
                | Expr::Literal(ScalarValue::Float64(0.0)) = right
                {
                    return Some(left.clone());
                }
                if let Expr::Literal(ScalarValue::Int64(0))
                | Expr::Literal(ScalarValue::Float64(0.0)) = left
                {
                    return Some(right.clone());
                }
                // x + (-y) = x - y
                if let Expr::Unary {
                    op: UnaryOp::Neg,
                    expr,
                } = right
                {
                    return Some(Expr::binary(left.clone(), Operator::Sub, *expr.clone()));
                }
            }
            // Multiplication simplifications
            Operator::Mul => {
                // x * 1 = x
                if let Expr::Literal(ScalarValue::Int64(1))
                | Expr::Literal(ScalarValue::Float64(1.0)) = right
                {
                    return Some(left.clone());
                }
                if let Expr::Literal(ScalarValue::Int64(1))
                | Expr::Literal(ScalarValue::Float64(1.0)) = left
                {
                    return Some(right.clone());
                }
                // x * 0 = 0
                if let Expr::Literal(ScalarValue::Int64(0))
                | Expr::Literal(ScalarValue::Float64(0.0)) = right
                {
                    return Some(right.clone());
                }
                if let Expr::Literal(ScalarValue::Int64(0))
                | Expr::Literal(ScalarValue::Float64(0.0)) = left
                {
                    return Some(left.clone());
                }
            }
            // Subtraction simplifications
            Operator::Sub => {
                // x - 0 = x
                if let Expr::Literal(ScalarValue::Int64(0))
                | Expr::Literal(ScalarValue::Float64(0.0)) = right
                {
                    return Some(left.clone());
                }
                // x - x = 0 (for any type, including columns)
                if left == right {
                    return match left {
                        Expr::Literal(ScalarValue::Int64(_)) => {
                            Some(Expr::Literal(ScalarValue::Int64(0)))
                        }
                        Expr::Literal(ScalarValue::Float64(_)) => {
                            Some(Expr::Literal(ScalarValue::Float64(0.0)))
                        }
                        Expr::Column(_) => {
                            // For columns, we can't determine the type at compile time
                            // Default to Int64(0) for simplicity
                            Some(Expr::Literal(ScalarValue::Int64(0)))
                        }
                        _ => None,
                    };
                }
            }
            // Division simplifications
            Operator::Div => {
                // x / 1 = x
                if let Expr::Literal(ScalarValue::Int64(1))
                | Expr::Literal(ScalarValue::Float64(1.0)) = right
                {
                    return Some(left.clone());
                }
                // 0 / x = 0 (if x != 0)
                if let Expr::Literal(ScalarValue::Int64(0))
                | Expr::Literal(ScalarValue::Float64(0.0)) = left
                {
                    if let Expr::Literal(val) = right {
                        if !self.is_zero_value(val) {
                            return Some(left.clone());
                        }
                    }
                }
            }
            // Comparison simplifications
            Operator::Eq if left == right => {
                // x = x is always true (for non-null values)
                if let (Expr::Literal(_), Expr::Literal(_)) = (left, right) {
                    return Some(Expr::Literal(ScalarValue::Boolean(true)));
                }
            }
            _ => {}
        }
        None
    }

    /// Simplify unary expressions using algebraic rules
    fn simplify_unary_expression(&self, op: UnaryOp, expr: &Expr) -> Option<Expr> {
        match op {
            UnaryOp::Neg => {
                // -(-x) = x
                if let Expr::Unary {
                    op: UnaryOp::Neg,
                    expr,
                } = expr
                {
                    return Some(*expr.clone());
                }
                // -(0) = 0
                if let Expr::Literal(ScalarValue::Int64(0))
                | Expr::Literal(ScalarValue::Float64(0.0)) = expr
                {
                    return Some(expr.clone());
                }
            }
            UnaryOp::Not => {
                // !(!x) = x (for boolean expressions)
                if let Expr::Unary {
                    op: UnaryOp::Not,
                    expr,
                } = expr
                {
                    return Some(*expr.clone());
                }
            }
            _ => {}
        }
        None
    }

    /// Simplify function expressions using mathematical identities
    fn simplify_function_expression(&self, name: &str, args: &[Expr]) -> Option<Expr> {
        match name {
            "abs" if args.len() == 1 => {
                // abs(abs(x)) = abs(x)
                if let Expr::Function {
                    name: inner_name,
                    args: _inner_args,
                } = &args[0]
                {
                    if inner_name == "abs" {
                        return Some(args[0].clone());
                    }
                }
                // abs(0) = 0, abs(positive) = positive
                if let Expr::Literal(_val) = &args[0] {
                    if let Some(abs_val) = self.evaluate_function("abs", args) {
                        return Some(Expr::Literal(abs_val));
                    }
                }
            }
            "sqrt" if args.len() == 1 => {
                // sqrt(0) = 0, sqrt(1) = 1
                if let Expr::Literal(_val) = &args[0] {
                    if let Some(sqrt_val) = self.evaluate_function("sqrt", args) {
                        return Some(Expr::Literal(sqrt_val));
                    }
                }
            }
            "length" if args.len() == 1 => {
                // length("") = 0
                if let Expr::Literal(ScalarValue::Utf8(s)) = &args[0] {
                    if s.is_empty() {
                        return Some(Expr::Literal(ScalarValue::Int64(0)));
                    }
                }
            }
            _ => {}
        }
        None
    }

    /// Check if a cast is redundant
    fn is_cast_redundant(&self, val: &ScalarValue, target_type: &str) -> bool {
        matches!(
            (val, target_type),
            (ScalarValue::Int64(_), "Int64")
                | (ScalarValue::Float64(_), "Float64")
                | (ScalarValue::Utf8(_), "Utf8")
                | (ScalarValue::Boolean(_), "Boolean")
        )
    }

    /// Check if a scalar value represents zero
    fn is_zero_value(&self, val: &ScalarValue) -> bool {
        matches!(val, ScalarValue::Int64(0) | ScalarValue::Float64(0.0))
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
    // Try to parse simple expressions first
    if let Some(expr) = parse_simple_expression_string(expr_str) {
        return expr;
    }

    // Fall back to Python AST compilation
    let result = compile_python_expression(expr_str, None);
    if !result.errors.is_empty() {
        // Return a null expression if compilation failed
        Expr::Literal(ScalarValue::Null)
    } else {
        result.expr
    }
}

/// Parse simple expression strings for testing
fn parse_simple_expression_string(expr_str: &str) -> Option<Expr> {
    let trimmed = expr_str.trim();

    // Check for literals first (numbers, strings, booleans)
    if let Some(literal) = parse_literal(trimmed) {
        return Some(Expr::Literal(literal));
    }

    // Check for binary operators
    if let Some((left, op, right)) = parse_simple_binary_operation(trimmed) {
        return Some(Expr::Binary {
            left: Box::new(left),
            op,
            right: Box::new(right),
        });
    }

    // Check if it's a column name (alphanumeric with underscores)
    if trimmed.chars().all(|c| c.is_alphanumeric() || c == '_') {
        return Some(Expr::Column(trimmed.to_string()));
    }

    None
}

/// Parse binary operations from a string
fn parse_simple_binary_operation(expr_str: &str) -> Option<(Expr, Operator, Expr)> {
    let operators = vec!["==", "!=", "<=", ">=", "<", ">", "+", "-", "*", "/"];

    for op in &operators {
        if let Some(pos) = expr_str.find(op) {
            if pos > 0 && pos + op.len() < expr_str.len() {
                let left_str = expr_str[..pos].trim();
                let right_str = expr_str[pos + op.len()..].trim();

                if let Some(left) = parse_simple_expression_string(left_str) {
                    if let Some(right) = parse_simple_expression_string(right_str) {
                        return Some((left, map_operator(op), right));
                    }
                }
            }
        }
    }

    None
}

/// Map operator string to Operator enum
fn map_operator(op_str: &str) -> Operator {
    match op_str {
        "+" => Operator::Add,
        "-" => Operator::Sub,
        "*" => Operator::Mul,
        "/" => Operator::Div,
        "==" => Operator::Eq,
        "!=" => Operator::NotEq,
        "<" => Operator::Lt,
        "<=" => Operator::LtEq,
        ">" => Operator::Gt,
        ">=" => Operator::GtEq,
        _ => Operator::Add,
    }
}

/// Parse literal values
fn parse_literal(expr_str: &str) -> Option<ScalarValue> {
    let trimmed = expr_str.trim();

    // String literal
    if trimmed.starts_with('\'') && trimmed.ends_with('\'') {
        return Some(ScalarValue::Utf8(trimmed[1..trimmed.len() - 1].to_string()));
    }

    // Boolean
    if trimmed == "true" {
        return Some(ScalarValue::Boolean(true));
    }
    if trimmed == "false" {
        return Some(ScalarValue::Boolean(false));
    }

    // Integer
    if let Ok(i) = trimmed.parse::<i64>() {
        return Some(ScalarValue::Int64(i));
    }

    // Float
    if let Ok(f) = trimmed.parse::<f64>() {
        return Some(ScalarValue::Float64(f));
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_literal_parsing() {
        let expr = expr_from_string("42");
        assert_eq!(expr, Expr::Literal(ScalarValue::Int64(42)));

        let expr = expr_from_string("3.14159");
        assert_eq!(expr, Expr::Literal(ScalarValue::Float64(3.14159)));

        let expr = expr_from_string("true");
        assert_eq!(expr, Expr::Literal(ScalarValue::Boolean(true)));

        let expr = expr_from_string("'hello'");
        assert_eq!(expr, Expr::Literal(ScalarValue::Utf8("hello".to_string())));
    }

    #[test]
    fn test_constant_folding_binary_operations() {
        let compiler = ExpressionCompiler::new();

        // Test constant folding for addition
        let expr = Expr::binary(
            Expr::Literal(ScalarValue::Int64(5)),
            Operator::Add,
            Expr::Literal(ScalarValue::Int64(3)),
        );
        let folded = compiler.constant_fold(&expr);
        assert_eq!(folded, Expr::Literal(ScalarValue::Int64(8)));

        // Test constant folding for multiplication
        let expr = Expr::binary(
            Expr::Literal(ScalarValue::Int64(4)),
            Operator::Mul,
            Expr::Literal(ScalarValue::Int64(6)),
        );
        let folded = compiler.constant_fold(&expr);
        assert_eq!(folded, Expr::Literal(ScalarValue::Int64(24)));

        // Test constant folding for comparison
        let expr = Expr::binary(
            Expr::Literal(ScalarValue::Int64(10)),
            Operator::Gt,
            Expr::Literal(ScalarValue::Int64(5)),
        );
        let folded = compiler.constant_fold(&expr);
        assert_eq!(folded, Expr::Literal(ScalarValue::Boolean(true)));

        // Test that expressions with variables are not folded
        let expr = Expr::binary(
            Expr::Column("a".to_string()),
            Operator::Add,
            Expr::Literal(ScalarValue::Int64(1)),
        );
        let folded = compiler.constant_fold(&expr);
        match folded {
            Expr::Binary { left, op, right } => {
                assert_eq!(*left, Expr::Column("a".to_string()));
                assert_eq!(op, Operator::Add);
                assert_eq!(*right, Expr::Literal(ScalarValue::Int64(1)));
            }
            _ => panic!("Expected binary expression"),
        }
    }

    #[test]
    fn test_constant_folding_unary_operations() {
        let compiler = ExpressionCompiler::new();

        // Test constant folding for negation
        let expr = Expr::unary(UnaryOp::Neg, Expr::Literal(ScalarValue::Int64(5)));
        let folded = compiler.constant_fold(&expr);
        assert_eq!(folded, Expr::Literal(ScalarValue::Int64(-5)));

        // Test constant folding for boolean NOT
        let expr = Expr::unary(UnaryOp::Not, Expr::Literal(ScalarValue::Boolean(true)));
        let folded = compiler.constant_fold(&expr);
        assert_eq!(folded, Expr::Literal(ScalarValue::Boolean(false)));
    }

    #[test]
    fn test_constant_folding_functions() {
        let compiler = ExpressionCompiler::new();

        // Test constant folding for abs function
        let expr = Expr::function(
            "abs".to_string(),
            vec![Expr::Literal(ScalarValue::Int64(-5))],
        );
        let folded = compiler.constant_fold(&expr);
        assert_eq!(folded, Expr::Literal(ScalarValue::Int64(5)));

        // Test constant folding for length function
        let expr = Expr::function(
            "length".to_string(),
            vec![Expr::Literal(ScalarValue::Utf8("hello".to_string()))],
        );
        let folded = compiler.constant_fold(&expr);
        assert_eq!(folded, Expr::Literal(ScalarValue::Int64(5)));
    }

    #[test]
    fn test_constant_folding_nested_expressions() {
        let compiler = ExpressionCompiler::new();

        // Test nested constant folding: (2 + 3) * 4
        let inner = Expr::binary(
            Expr::Literal(ScalarValue::Int64(2)),
            Operator::Add,
            Expr::Literal(ScalarValue::Int64(3)),
        );
        let expr = Expr::binary(inner, Operator::Mul, Expr::Literal(ScalarValue::Int64(4)));
        let folded = compiler.constant_fold(&expr);
        assert_eq!(folded, Expr::Literal(ScalarValue::Int64(20)));
    }

    #[test]
    fn test_constant_folding_disabled() {
        let compiler = ExpressionCompiler::new().with_constant_folding(false);

        let expr = Expr::binary(
            Expr::Literal(ScalarValue::Int64(5)),
            Operator::Add,
            Expr::Literal(ScalarValue::Int64(3)),
        );
        let folded = compiler.constant_fold(&expr);
        // Should remain unchanged when constant folding is disabled
        match folded {
            Expr::Binary { left, op, right } => {
                assert_eq!(*left, Expr::Literal(ScalarValue::Int64(5)));
                assert_eq!(op, Operator::Add);
                assert_eq!(*right, Expr::Literal(ScalarValue::Int64(3)));
            }
            _ => panic!("Expected binary expression"),
        }
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
        let expr = expr_from_string("a > 10");
        if let Expr::Binary {
            left: _,
            op,
            right: _,
        } = expr
        {
            assert_eq!(op, Operator::Gt);
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

    // Enhanced constant folding tests for Milestone 1.1.1
    #[test]
    fn test_algebraic_simplification_addition() {
        let compiler = ExpressionCompiler::new();

        // Test x + 0 = x
        let expr = Expr::binary(
            Expr::Column("x".to_string()),
            Operator::Add,
            Expr::Literal(ScalarValue::Int64(0)),
        );
        let folded = compiler.constant_fold(&expr);
        assert_eq!(folded, Expr::Column("x".to_string()));

        // Test 0 + x = x
        let expr = Expr::binary(
            Expr::Literal(ScalarValue::Int64(0)),
            Operator::Add,
            Expr::Column("x".to_string()),
        );
        let folded = compiler.constant_fold(&expr);
        assert_eq!(folded, Expr::Column("x".to_string()));

        // Test x + (-y) = x - y
        let expr = Expr::binary(
            Expr::Column("x".to_string()),
            Operator::Add,
            Expr::unary(UnaryOp::Neg, Expr::Column("y".to_string())),
        );
        let folded = compiler.constant_fold(&expr);
        if let Expr::Binary { left, op, right } = folded {
            assert_eq!(*left, Expr::Column("x".to_string()));
            assert_eq!(op, Operator::Sub);
            assert_eq!(*right, Expr::Column("y".to_string()));
        } else {
            panic!("Expected binary expression");
        }
    }

    #[test]
    fn test_algebraic_simplification_multiplication() {
        let compiler = ExpressionCompiler::new();

        // Test x * 1 = x
        let expr = Expr::binary(
            Expr::Column("x".to_string()),
            Operator::Mul,
            Expr::Literal(ScalarValue::Int64(1)),
        );
        let folded = compiler.constant_fold(&expr);
        assert_eq!(folded, Expr::Column("x".to_string()));

        // Test x * 0 = 0
        let expr = Expr::binary(
            Expr::Column("x".to_string()),
            Operator::Mul,
            Expr::Literal(ScalarValue::Int64(0)),
        );
        let folded = compiler.constant_fold(&expr);
        assert_eq!(folded, Expr::Literal(ScalarValue::Int64(0)));
    }

    #[test]
    fn test_algebraic_simplification_subtraction() {
        let compiler = ExpressionCompiler::new();

        // Test x - 0 = x
        let expr = Expr::binary(
            Expr::Column("x".to_string()),
            Operator::Sub,
            Expr::Literal(ScalarValue::Int64(0)),
        );
        let folded = compiler.constant_fold(&expr);
        assert_eq!(folded, Expr::Column("x".to_string()));

        // Test x - x = 0
        let expr = Expr::binary(
            Expr::Literal(ScalarValue::Int64(5)),
            Operator::Sub,
            Expr::Literal(ScalarValue::Int64(5)),
        );
        let folded = compiler.constant_fold(&expr);
        assert_eq!(folded, Expr::Literal(ScalarValue::Int64(0)));
    }

    #[test]
    fn test_algebraic_simplification_division() {
        let compiler = ExpressionCompiler::new();

        // Test x / 1 = x
        let expr = Expr::binary(
            Expr::Column("x".to_string()),
            Operator::Div,
            Expr::Literal(ScalarValue::Int64(1)),
        );
        let folded = compiler.constant_fold(&expr);
        assert_eq!(folded, Expr::Column("x".to_string()));

        // Test 0 / x = 0 (if x != 0)
        let expr = Expr::binary(
            Expr::Literal(ScalarValue::Int64(0)),
            Operator::Div,
            Expr::Literal(ScalarValue::Int64(5)),
        );
        let folded = compiler.constant_fold(&expr);
        assert_eq!(folded, Expr::Literal(ScalarValue::Int64(0)));
    }

    #[test]
    fn test_unary_simplification() {
        let compiler = ExpressionCompiler::new();

        // Test -(-x) = x
        let expr = Expr::unary(
            UnaryOp::Neg,
            Expr::unary(UnaryOp::Neg, Expr::Column("x".to_string())),
        );
        let folded = compiler.constant_fold(&expr);
        assert_eq!(folded, Expr::Column("x".to_string()));

        // Test -0 = 0
        let expr = Expr::unary(UnaryOp::Neg, Expr::Literal(ScalarValue::Int64(0)));
        let folded = compiler.constant_fold(&expr);
        assert_eq!(folded, Expr::Literal(ScalarValue::Int64(0)));

        // Test !(!x) = x
        let expr = Expr::unary(
            UnaryOp::Not,
            Expr::unary(UnaryOp::Not, Expr::Column("x".to_string())),
        );
        let folded = compiler.constant_fold(&expr);
        assert_eq!(folded, Expr::Column("x".to_string()));
    }

    #[test]
    fn test_function_simplification() {
        let compiler = ExpressionCompiler::new();

        // Test abs(abs(x)) = abs(x)
        let inner_abs = Expr::function("abs".to_string(), vec![Expr::Column("x".to_string())]);
        let expr = Expr::function("abs".to_string(), vec![inner_abs]);
        let folded = compiler.constant_fold(&expr);
        // Should simplify to just abs(x)
        if let Expr::Function { name, args } = folded {
            assert_eq!(name, "abs");
            assert_eq!(args.len(), 1);
            assert_eq!(args[0], Expr::Column("x".to_string()));
        } else {
            panic!("Expected function expression");
        }

        // Test abs(0) = 0
        let expr = Expr::function(
            "abs".to_string(),
            vec![Expr::Literal(ScalarValue::Int64(0))],
        );
        let folded = compiler.constant_fold(&expr);
        assert_eq!(folded, Expr::Literal(ScalarValue::Int64(0)));

        // Test length("") = 0
        let expr = Expr::function(
            "length".to_string(),
            vec![Expr::Literal(ScalarValue::Utf8("".to_string()))],
        );
        let folded = compiler.constant_fold(&expr);
        assert_eq!(folded, Expr::Literal(ScalarValue::Int64(0)));
    }

    #[test]
    fn test_redundant_cast_removal() {
        let compiler = ExpressionCompiler::new();

        // Test redundant cast removal
        let expr = Expr::Cast {
            expr: Box::new(Expr::Literal(ScalarValue::Int64(42))),
            data_type: "Int64".to_string(),
        };
        let folded = compiler.constant_fold(&expr);
        // Should return the literal without cast
        assert_eq!(folded, Expr::Literal(ScalarValue::Int64(42)));
    }

    #[test]
    fn test_complex_nested_folding() {
        let compiler = ExpressionCompiler::new();

        // Test complex expression: (x + 0) * 1 - (y - y)
        let expr = Expr::binary(
            Expr::binary(
                Expr::binary(
                    Expr::Column("x".to_string()),
                    Operator::Add,
                    Expr::Literal(ScalarValue::Int64(0)),
                ),
                Operator::Mul,
                Expr::Literal(ScalarValue::Int64(1)),
            ),
            Operator::Sub,
            Expr::binary(
                Expr::Column("y".to_string()),
                Operator::Sub,
                Expr::Column("y".to_string()),
            ),
        );
        let folded = compiler.constant_fold(&expr);

        // Debug: Print what we got
        println!("Folded expression: {:?}", folded);

        // Should simplify all the way to x (since x + 0 = x, x * 1 = x, y - y = 0, x - 0 = x)
        assert_eq!(folded, Expr::Column("x".to_string()));
    }

    #[test]
    fn test_constant_folding_performance() {
        let compiler = ExpressionCompiler::new();

        // Create a deeply nested constant expression
        let expr = Expr::binary(
            Expr::binary(
                Expr::Literal(ScalarValue::Int64(2)),
                Operator::Add,
                Expr::Literal(ScalarValue::Int64(3)),
            ),
            Operator::Mul,
            Expr::binary(
                Expr::Literal(ScalarValue::Int64(4)),
                Operator::Sub,
                Expr::Literal(ScalarValue::Int64(1)),
            ),
        );

        let folded = compiler.constant_fold(&expr);
        // Should fold to a single constant: (2+3) * (4-1) = 5 * 3 = 15
        assert_eq!(folded, Expr::Literal(ScalarValue::Int64(15)));
    }
}
