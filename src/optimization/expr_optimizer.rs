//! Expression optimization including constant folding and CSE

use crate::expression::{Expr, Operator, ScalarValue, UnaryOp};
use std::collections::HashMap;

/// Expression optimizer with constant folding and CSE
pub struct ExprOptimizer {
    /// Cache for common subexpression elimination
    cse_cache: HashMap<String, Expr>,
}

impl ExprOptimizer {
    /// Create a new expression optimizer
    pub fn new() -> Self {
        Self {
            cse_cache: HashMap::new(),
        }
    }

    /// Optimize an expression
    pub fn optimize(&mut self, expr: Expr) -> Expr {
        self.optimize_impl(expr)
    }

    /// Internal optimization implementation
    fn optimize_impl(&mut self, expr: Expr) -> Expr {
        // First apply constant folding
        let folded = self.constant_fold(expr);

        // Then apply CSE
        self.cse_optimize(folded)
    }

    /// Constant folding optimization
    fn constant_fold(&self, expr: Expr) -> Expr {
        match expr {
            Expr::Binary { left, op, right } => {
                let left_opt = self.constant_fold(*left);
                let right_opt = self.constant_fold(*right);

                // Try to fold if both operands are literals
                if let Expr::Literal(left_val) = &left_opt {
                    if let Expr::Literal(right_val) = &right_opt {
                        if let Some(folded) = self.fold_literals(left_val, &op, right_val) {
                            return folded;
                        }
                    }
                }

                Expr::Binary {
                    left: Box::new(left_opt),
                    op,
                    right: Box::new(right_opt),
                }
            }
            Expr::Unary { op, expr } => {
                let expr_opt = self.constant_fold(*expr);

                if let Expr::Literal(val) = &expr_opt {
                    if let Some(folded) = self.fold_unary_literal(val, &op) {
                        return folded;
                    }
                }

                Expr::Unary {
                    op,
                    expr: Box::new(expr_opt),
                }
            }
            _ => expr,
        }
    }

    /// Fold binary operation on literals
    fn fold_literals(
        &self,
        left: &ScalarValue,
        op: &Operator,
        right: &ScalarValue,
    ) -> Option<Expr> {
        match (left, op, right) {
            // Integer arithmetic
            (ScalarValue::Int64(l), Operator::Add, ScalarValue::Int64(r)) => {
                Some(Expr::Literal(ScalarValue::Int64(l.saturating_add(*r))))
            }
            (ScalarValue::Int64(l), Operator::Sub, ScalarValue::Int64(r)) => {
                Some(Expr::Literal(ScalarValue::Int64(l.saturating_sub(*r))))
            }
            (ScalarValue::Int64(l), Operator::Mul, ScalarValue::Int64(r)) => {
                Some(Expr::Literal(ScalarValue::Int64(l.saturating_mul(*r))))
            }
            (ScalarValue::Int64(l), Operator::Div, ScalarValue::Int64(r)) => {
                if *r != 0 {
                    Some(Expr::Literal(ScalarValue::Int64(l / r)))
                } else {
                    None // Division by zero - don't fold
                }
            }
            // Float arithmetic
            (ScalarValue::Float64(l), Operator::Add, ScalarValue::Float64(r)) => {
                Some(Expr::Literal(ScalarValue::Float64(l + r)))
            }
            (ScalarValue::Float64(l), Operator::Sub, ScalarValue::Float64(r)) => {
                Some(Expr::Literal(ScalarValue::Float64(l - r)))
            }
            (ScalarValue::Float64(l), Operator::Mul, ScalarValue::Float64(r)) => {
                Some(Expr::Literal(ScalarValue::Float64(l * r)))
            }
            (ScalarValue::Float64(l), Operator::Div, ScalarValue::Float64(r)) => {
                if *r != 0.0 {
                    Some(Expr::Literal(ScalarValue::Float64(l / r)))
                } else {
                    None // Division by zero - don't fold
                }
            }
            // Boolean operations
            (ScalarValue::Boolean(l), Operator::And, ScalarValue::Boolean(r)) => {
                Some(Expr::Literal(ScalarValue::Boolean(*l && *r)))
            }
            (ScalarValue::Boolean(l), Operator::Or, ScalarValue::Boolean(r)) => {
                Some(Expr::Literal(ScalarValue::Boolean(*l || *r)))
            }
            _ => None,
        }
    }

    /// Fold unary operation on literal
    fn fold_unary_literal(&self, val: &ScalarValue, op: &UnaryOp) -> Option<Expr> {
        match (val, op) {
            (ScalarValue::Int64(v), UnaryOp::Neg) => {
                Some(Expr::Literal(ScalarValue::Int64(v.saturating_neg())))
            }
            (ScalarValue::Float64(v), UnaryOp::Neg) => {
                Some(Expr::Literal(ScalarValue::Float64(-v)))
            }
            (ScalarValue::Float32(v), UnaryOp::Neg) => {
                Some(Expr::Literal(ScalarValue::Float32(-v)))
            }
            (ScalarValue::Boolean(v), UnaryOp::Not) => {
                Some(Expr::Literal(ScalarValue::Boolean(!v)))
            }
            _ => None,
        }
    }

    /// Common subexpression elimination
    fn cse_optimize(&mut self, expr: Expr) -> Expr {
        let key = expr.as_string();

        if let Some(cached) = self.cse_cache.get(&key) {
            return cached.clone();
        }

        let optimized = match expr {
            Expr::Binary { left, op, right } => Expr::Binary {
                left: Box::new(self.cse_optimize(*left)),
                op,
                right: Box::new(self.cse_optimize(*right)),
            },
            Expr::Unary { op, expr } => Expr::Unary {
                op,
                expr: Box::new(self.cse_optimize(*expr)),
            },
            Expr::Function { name, args } => Expr::Function {
                name,
                args: args.into_iter().map(|arg| self.cse_optimize(arg)).collect(),
            },
            _ => expr,
        };

        self.cse_cache
            .insert(optimized.as_string(), optimized.clone());
        optimized
    }
}

impl Default for ExprOptimizer {
    fn default() -> Self {
        Self::new()
    }
}
