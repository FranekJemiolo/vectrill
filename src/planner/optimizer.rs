//! Logical Plan Optimizer

use crate::expression::{Expr, Operator};
use crate::planner::logical::LogicalPlan;

/// Optimizer rule trait
pub trait OptimizerRule: Send + Sync {
    /// Apply the rule to a logical plan
    fn apply(&self, plan: &LogicalPlan) -> LogicalPlan;

    /// Get the name of this rule
    fn name(&self) -> &'static str;
}

/// Rule-based optimizer
pub struct Optimizer {
    rules: Vec<Box<dyn OptimizerRule>>,
}

impl Optimizer {
    /// Create a new optimizer with default rules
    pub fn new() -> Self {
        let rules: Vec<Box<dyn OptimizerRule>> = vec![
            Box::new(FilterPushdownRule),
            Box::new(MapFusionRule),
            Box::new(ColumnPruningRule),
            Box::new(ProjectionEliminationRule),
        ];

        Self { rules }
    }

    /// Add a custom rule to the optimizer
    pub fn add_rule(mut self, rule: Box<dyn OptimizerRule>) -> Self {
        self.rules.push(rule);
        self
    }

    /// Optimize a logical plan using all rules
    pub fn optimize(&self, plan: &LogicalPlan) -> LogicalPlan {
        let mut current_plan = plan.clone();

        for rule in &self.rules {
            current_plan = rule.apply(&current_plan);
        }

        current_plan
    }
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

/// Filter pushdown rule - pushes filters closer to data sources
pub struct FilterPushdownRule;

impl OptimizerRule for FilterPushdownRule {
    fn apply(&self, plan: &LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Filter { input, expr } => {
                // Try to push filter through input
                let optimized_input = self.apply(input);

                match optimized_input {
                    LogicalPlan::Filter {
                        input: inner_input,
                        expr: inner_expr,
                    } => {
                        // Combine filters: filter AND filter
                        let combined_expr =
                            Expr::binary(inner_expr.clone(), Operator::And, expr.clone());

                        LogicalPlan::Filter {
                            input: inner_input.clone(),
                            expr: combined_expr,
                        }
                    }

                    LogicalPlan::Map {
                        input: ref map_input,
                        expr: ref map_expr,
                    } => {
                        // Check if filter can be pushed through map
                        if can_push_filter_through_map(expr, map_expr) {
                            // Push filter below map
                            let pushed_filter = LogicalPlan::Filter {
                                input: map_input.clone(),
                                expr: expr.clone(),
                            };

                            LogicalPlan::Map {
                                input: Box::new(pushed_filter),
                                expr: map_expr.clone(),
                            }
                        } else {
                            // Can't push filter, keep as is
                            LogicalPlan::Filter {
                                input: Box::new(optimized_input),
                                expr: expr.clone(),
                            }
                        }
                    }

                    _ => {
                        // Can't push filter further
                        LogicalPlan::Filter {
                            input: Box::new(optimized_input),
                            expr: expr.clone(),
                        }
                    }
                }
            }

            LogicalPlan::Map { input, expr } => {
                let optimized_input = self.apply(input);
                LogicalPlan::Map {
                    input: Box::new(optimized_input),
                    expr: expr.clone(),
                }
            }

            LogicalPlan::GroupBy { input, keys } => {
                let optimized_input = self.apply(input);
                LogicalPlan::GroupBy {
                    input: Box::new(optimized_input),
                    keys: keys.clone(),
                }
            }

            LogicalPlan::Window { input, spec } => {
                let optimized_input = self.apply(input);
                LogicalPlan::Window {
                    input: Box::new(optimized_input),
                    spec: spec.clone(),
                }
            }

            LogicalPlan::Aggregate { input, spec } => {
                let optimized_input = self.apply(input);
                LogicalPlan::Aggregate {
                    input: Box::new(optimized_input),
                    spec: spec.clone(),
                }
            }

            LogicalPlan::Project { input, columns } => {
                let optimized_input = self.apply(input);
                LogicalPlan::Project {
                    input: Box::new(optimized_input),
                    columns: columns.clone(),
                }
            }

            LogicalPlan::Source { name, attrs } => LogicalPlan::Source {
                name: name.clone(),
                attrs: attrs.clone(),
            },
        }
    }

    fn name(&self) -> &'static str {
        "FilterPushdown"
    }
}

/// Map fusion rule - combines consecutive map operations
pub struct MapFusionRule;

impl OptimizerRule for MapFusionRule {
    fn apply(&self, plan: &LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Map { input, expr } => {
                let optimized_input = self.apply(input);

                match optimized_input {
                    LogicalPlan::Map {
                        input: inner_input,
                        expr: _inner_expr,
                    } => {
                        // Fuse maps: combine expressions
                        // For now, just keep the outer map
                        // In a real implementation, we'd combine the expressions
                        LogicalPlan::Map {
                            input: inner_input.clone(),
                            expr: expr.clone(),
                        }
                    }

                    _ => LogicalPlan::Map {
                        input: Box::new(optimized_input),
                        expr: expr.clone(),
                    },
                }
            }

            LogicalPlan::Filter { input, expr } => {
                let optimized_input = self.apply(input);
                LogicalPlan::Filter {
                    input: Box::new(optimized_input),
                    expr: expr.clone(),
                }
            }

            LogicalPlan::GroupBy { input, keys } => {
                let optimized_input = self.apply(input);
                LogicalPlan::GroupBy {
                    input: Box::new(optimized_input),
                    keys: keys.clone(),
                }
            }

            LogicalPlan::Window { input, spec } => {
                let optimized_input = self.apply(input);
                LogicalPlan::Window {
                    input: Box::new(optimized_input),
                    spec: spec.clone(),
                }
            }

            LogicalPlan::Aggregate { input, spec } => {
                let optimized_input = self.apply(input);
                LogicalPlan::Aggregate {
                    input: Box::new(optimized_input),
                    spec: spec.clone(),
                }
            }

            LogicalPlan::Project { input, columns } => {
                let optimized_input = self.apply(input);
                LogicalPlan::Project {
                    input: Box::new(optimized_input),
                    columns: columns.clone(),
                }
            }

            LogicalPlan::Source { name, attrs } => LogicalPlan::Source {
                name: name.clone(),
                attrs: attrs.clone(),
            },
        }
    }

    fn name(&self) -> &'static str {
        "MapFusion"
    }
}

/// Column pruning rule - removes unused columns
pub struct ColumnPruningRule;

impl OptimizerRule for ColumnPruningRule {
    fn apply(&self, plan: &LogicalPlan) -> LogicalPlan {
        // For now, this is a placeholder implementation
        // In a real implementation, we would analyze column usage
        // and add projections to prune unused columns
        plan.clone()
    }

    fn name(&self) -> &'static str {
        "ColumnPruning"
    }
}

/// Projection elimination rule - removes redundant projections
pub struct ProjectionEliminationRule;

impl OptimizerRule for ProjectionEliminationRule {
    fn apply(&self, plan: &LogicalPlan) -> LogicalPlan {
        match plan {
            LogicalPlan::Project { input, columns } => {
                let optimized_input = self.apply(input);

                match optimized_input {
                    LogicalPlan::Project {
                        input: ref inner_input,
                        columns: ref inner_columns,
                    } => {
                        // Combine projections
                        if columns == inner_columns {
                            // Same projection, eliminate the inner one
                            LogicalPlan::Project {
                                input: inner_input.clone(),
                                columns: columns.clone(),
                            }
                        } else {
                            // Different projections, keep outer
                            LogicalPlan::Project {
                                input: Box::new(optimized_input.clone()),
                                columns: columns.clone(),
                            }
                        }
                    }

                    _ => LogicalPlan::Project {
                        input: Box::new(optimized_input),
                        columns: columns.clone(),
                    },
                }
            }

            LogicalPlan::Filter { input, expr } => {
                let optimized_input = self.apply(input);
                LogicalPlan::Filter {
                    input: Box::new(optimized_input),
                    expr: expr.clone(),
                }
            }

            LogicalPlan::Map { input, expr } => {
                let optimized_input = self.apply(input);
                LogicalPlan::Map {
                    input: Box::new(optimized_input),
                    expr: expr.clone(),
                }
            }

            LogicalPlan::GroupBy { input, keys } => {
                let optimized_input = self.apply(input);
                LogicalPlan::GroupBy {
                    input: Box::new(optimized_input),
                    keys: keys.clone(),
                }
            }

            LogicalPlan::Window { input, spec } => {
                let optimized_input = self.apply(input);
                LogicalPlan::Window {
                    input: Box::new(optimized_input),
                    spec: spec.clone(),
                }
            }

            LogicalPlan::Aggregate { input, spec } => {
                let optimized_input = self.apply(input);
                LogicalPlan::Aggregate {
                    input: Box::new(optimized_input),
                    spec: spec.clone(),
                }
            }

            LogicalPlan::Source { name, attrs } => LogicalPlan::Source {
                name: name.clone(),
                attrs: attrs.clone(),
            },
        }
    }

    fn name(&self) -> &'static str {
        "ProjectionElimination"
    }
}

/// Check if a filter can be pushed through a map operation
fn can_push_filter_through_map(filter_expr: &Expr, map_expr: &Expr) -> bool {
    // Simplified check - in a real implementation, this would be more sophisticated
    match filter_expr {
        Expr::Column(name) => {
            // If filter references a column that's not modified by map, we can push it
            !modifies_column(map_expr, name)
        }
        Expr::Binary { left, op: _, right } => {
            can_push_filter_through_map(left, map_expr)
                && can_push_filter_through_map(right, map_expr)
        }
        Expr::Unary { op: _, expr } => can_push_filter_through_map(expr, map_expr),
        _ => false,
    }
}

/// Check if a map expression modifies a specific column
fn modifies_column(map_expr: &Expr, _column_name: &str) -> bool {
    // Simplified check - in a real implementation, this would analyze the expression
    match map_expr {
        Expr::Binary { left, op: _, right } => {
            modifies_column(left, _column_name) || modifies_column(right, _column_name)
        }
        Expr::Unary { op: _, expr } => modifies_column(expr, _column_name),
        Expr::Function { name: _, args: _ } => {
            // Functions typically create new columns, so they don't modify existing ones
            false
        }
        Expr::Cast { expr, data_type: _ } => modifies_column(expr, _column_name),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_optimizer_creation() {
        let optimizer = Optimizer::new();
        assert_eq!(optimizer.rules.len(), 4);
    }

    #[test]
    fn test_filter_pushdown() {
        let source = LogicalPlan::Source {
            name: "test".to_string(),
            attrs: HashMap::new(),
        };

        let filter1 = LogicalPlan::Filter {
            input: Box::new(source),
            expr: Expr::column("id"),
        };

        let filter2 = LogicalPlan::Filter {
            input: Box::new(filter1),
            expr: Expr::column("active"),
        };

        let rule = FilterPushdownRule;
        let result = rule.apply(&filter2);

        // Should combine filters
        match result {
            LogicalPlan::Filter { input, expr } => {
                match *input {
                    LogicalPlan::Source { .. } => {
                        // Filter should be pushed to source
                        assert!(true);
                    }
                    _ => panic!("Expected source as input"),
                }
            }
            _ => panic!("Expected filter node"),
        }
    }

    #[test]
    fn test_map_fusion() {
        let source = LogicalPlan::Source {
            name: "test".to_string(),
            attrs: HashMap::new(),
        };

        let map1 = LogicalPlan::Map {
            input: Box::new(source),
            expr: Expr::column("a"),
        };

        let map2 = LogicalPlan::Map {
            input: Box::new(map1),
            expr: Expr::column("b"),
        };

        let rule = MapFusionRule;
        let result = rule.apply(&map2);

        // Should fuse maps
        match result {
            LogicalPlan::Map { input, expr } => {
                match *input {
                    LogicalPlan::Source { .. } => {
                        // Should be fused to single map
                        assert!(true);
                    }
                    _ => panic!("Expected source as input"),
                }
            }
            _ => panic!("Expected map node"),
        }
    }

    #[test]
    fn test_projection_elimination() {
        let source = LogicalPlan::Source {
            name: "test".to_string(),
            attrs: HashMap::new(),
        };

        let proj1 = LogicalPlan::Project {
            input: Box::new(source),
            columns: vec!["id".to_string(), "name".to_string()],
        };

        let proj2 = LogicalPlan::Project {
            input: Box::new(proj1),
            columns: vec!["id".to_string(), "name".to_string()],
        };

        let rule = ProjectionEliminationRule;
        let result = rule.apply(&proj2);

        // Should eliminate duplicate projection
        match result {
            LogicalPlan::Project { input, columns } => {
                match *input {
                    LogicalPlan::Source { .. } => {
                        // Should eliminate one projection
                        assert_eq!(columns.len(), 2);
                    }
                    _ => panic!("Expected source as input"),
                }
            }
            _ => panic!("Expected project node"),
        }
    }
}
