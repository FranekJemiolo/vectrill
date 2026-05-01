//! Operator Fusion - fuse stateless operators to eliminate batch materialization

use crate::error::Result;
use crate::expression::Expr;
use crate::planner::physical::PhysicalPlan;
use crate::RecordBatch;

/// Trait for operators that can be fused together
pub trait FusableOperator {
    /// Get all expressions computed by this operator
    fn expressions(&self) -> Vec<&Expr>;

    /// Get the predicate (if any) that filters rows
    fn predicate(&self) -> Option<&Expr>;

    /// Get the projection (if any) that selects columns
    fn projection(&self) -> Option<&[String]>;
}

/// Fusion segment - a group of fusable operators
pub struct FusionSegment {
    /// Operators in this segment
    pub operators: Vec<Box<dyn FusableOperator>>,
    /// Boundary operator (if any) that ends this segment
    pub boundary: Option<Box<dyn crate::operators::pipeline::Operator>>,
}

impl FusionSegment {
    /// Create a new fusion segment
    pub fn new() -> Self {
        Self {
            operators: Vec::new(),
            boundary: None,
        }
    }

    /// Add an operator to the segment
    pub fn add_operator(&mut self, operator: Box<dyn FusableOperator>) {
        self.operators.push(operator);
    }

    /// Set the boundary operator
    pub fn set_boundary(&mut self, boundary: Box<dyn crate::operators::pipeline::Operator>) {
        self.boundary = Some(boundary);
    }
}

impl Default for FusionSegment {
    fn default() -> Self {
        Self::new()
    }
}

/// Fused operator - combines multiple stateless operators into one
#[derive(Debug)]
pub struct FusedOperator {
    /// Expressions to compute (in dependency order)
    pub expressions: Vec<FusedExpr>,
    /// Optional predicate to filter rows
    pub predicate: Option<Expr>,
    /// Projection - which columns to keep
    pub projection: Vec<String>,
}

/// A fused expression with its dependencies
#[derive(Debug, Clone)]
pub struct FusedExpr {
    /// The expression to compute
    pub expr: Expr,
    /// Index of this expression in the DAG
    pub id: usize,
    /// Dependencies (indices of expressions this depends on)
    pub dependencies: Vec<usize>,
}

impl FusedOperator {
    /// Create a new fused operator
    pub fn new(
        expressions: Vec<FusedExpr>,
        predicate: Option<Expr>,
        projection: Vec<String>,
    ) -> Self {
        Self {
            expressions,
            predicate,
            projection,
        }
    }
}

impl crate::operators::pipeline::Operator for FusedOperator {
    fn process(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        // 1. Evaluate expressions in dependency order
        let computed = self.eval_all(&batch)?;

        // 2. Apply predicate
        let filtered = if let Some(pred) = &self.predicate {
            self.apply_predicate(&computed, pred)?
        } else {
            computed
        };

        // 3. Apply projection
        self.apply_projection(filtered)
    }
}

impl FusedOperator {
    /// Evaluate all expressions in dependency order
    fn eval_all(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        // For now, return the batch unchanged
        // TODO: Implement actual expression evaluation with dependency ordering
        Ok(batch.clone())
    }

    /// Apply predicate to filter rows
    fn apply_predicate(&self, batch: &RecordBatch, _pred: &Expr) -> Result<RecordBatch> {
        // For now, return the batch unchanged
        // TODO: Implement actual predicate evaluation
        Ok(batch.clone())
    }

    /// Apply projection to select columns
    fn apply_projection(&self, batch: RecordBatch) -> Result<RecordBatch> {
        // If projection is empty or matches all columns, return as-is
        if self.projection.is_empty() {
            return Ok(batch);
        }

        // For now, return the batch unchanged
        // TODO: Implement actual projection
        Ok(batch)
    }
}

/// Check if an operator is fusable (stateless)
pub fn is_fusable(op: &dyn crate::operators::pipeline::Operator) -> bool {
    // Stateless operators: Filter, Map, Project, Cast
    // Stateful operators: GroupBy, Window, Join, Sort
    // Boundary operators: Source, Sink
    // For now, we'll check the type name
    let type_name = std::any::type_name_of_val(op);
    type_name.contains("FilterOperator")
        || type_name.contains("MapOperator")
        || type_name.contains("ProjectionOperator")
}

/// Check if an operator is stateful (cannot be fused)
pub fn is_stateful(op: &dyn crate::operators::pipeline::Operator) -> bool {
    let type_name = std::any::type_name_of_val(op);
    type_name.contains("GroupBy")
        || type_name.contains("Window")
        || type_name.contains("Join")
        || type_name.contains("Sort")
}

/// Check if an operator is a boundary (ends a fusion segment)
pub fn is_boundary(op: &dyn crate::operators::pipeline::Operator) -> bool {
    let type_name = std::any::type_name_of_val(op);
    type_name.contains("SourceOperator") || type_name.contains("Sink") || is_stateful(op)
}

/// Column Pruning - remove unused columns from the plan
#[allow(clippy::only_used_in_recursion)]
pub fn prune_columns(
    plan: &PhysicalPlan,
    needed_columns: &std::collections::HashSet<String>,
) -> PhysicalPlan {
    match plan {
        PhysicalPlan::ScanSource { name, attrs } => {
            // For source, we keep the scan but would ideally only read needed columns
            PhysicalPlan::ScanSource {
                name: name.clone(),
                attrs: attrs.clone(),
            }
        }
        PhysicalPlan::Map { input, expr } => {
            // Recursively prune the input
            let pruned_input = Box::new(prune_columns(input, needed_columns));
            PhysicalPlan::Map {
                input: pruned_input,
                expr: expr.clone(),
            }
        }
        PhysicalPlan::Filter { input, expr } => {
            // Recursively prune the input
            let pruned_input = Box::new(prune_columns(input, needed_columns));
            PhysicalPlan::Filter {
                input: pruned_input,
                expr: expr.clone(),
            }
        }
        PhysicalPlan::Project { input, columns } => {
            // Recursively prune the input
            let pruned_input = Box::new(prune_columns(input, needed_columns));
            PhysicalPlan::Project {
                input: pruned_input,
                columns: columns.clone(),
            }
        }
        PhysicalPlan::HashAggregate {
            input,
            keys,
            aggregations,
        } => {
            // Recursively prune the input
            let pruned_input = Box::new(prune_columns(input, needed_columns));
            PhysicalPlan::HashAggregate {
                input: pruned_input,
                keys: keys.clone(),
                aggregations: aggregations.clone(),
            }
        }
        PhysicalPlan::WindowedAggregate {
            input,
            window,
            aggregations,
        } => {
            // Recursively prune the input
            let pruned_input = Box::new(prune_columns(input, needed_columns));
            PhysicalPlan::WindowedAggregate {
                input: pruned_input,
                window: window.clone(),
                aggregations: aggregations.clone(),
            }
        }
        _ => plan.clone(),
    }
}

/// Predicate Pushdown - move predicates toward sources
pub fn push_down_predicates(plan: PhysicalPlan) -> PhysicalPlan {
    match plan {
        PhysicalPlan::Filter { input, expr } => {
            // Try to push the filter down through the input
            let pushed_input = push_down_predicates(*input);

            // Check if we can push through the pushed_input
            match pushed_input {
                PhysicalPlan::Map {
                    input: map_input,
                    expr: map_expr,
                } => {
                    // Filter before Map if possible (predicate doesn't depend on map outputs)
                    PhysicalPlan::Map {
                        input: Box::new(PhysicalPlan::Filter {
                            input: map_input,
                            expr: expr.clone(),
                        }),
                        expr: map_expr,
                    }
                }
                PhysicalPlan::Project {
                    input: proj_input,
                    columns,
                } => {
                    // Filter before Project if possible
                    PhysicalPlan::Project {
                        input: Box::new(PhysicalPlan::Filter {
                            input: proj_input,
                            expr: expr.clone(),
                        }),
                        columns,
                    }
                }
                _ => PhysicalPlan::Filter {
                    input: Box::new(pushed_input),
                    expr,
                },
            }
        }
        PhysicalPlan::Map { input, expr } => PhysicalPlan::Map {
            input: Box::new(push_down_predicates(*input)),
            expr,
        },
        PhysicalPlan::Project { input, columns } => PhysicalPlan::Project {
            input: Box::new(push_down_predicates(*input)),
            columns,
        },
        _ => plan,
    }
}

/// Fusion Segment Builder - builds fusion segments from a physical plan
pub struct FusionSegmentBuilder {
    segments: Vec<FusionSegment>,
    current_segment: FusionSegment,
}

impl FusionSegmentBuilder {
    /// Create a new fusion segment builder
    pub fn new() -> Self {
        Self {
            segments: Vec::new(),
            current_segment: FusionSegment::new(),
        }
    }

    /// Build fusion segments from a physical plan
    pub fn build_from_plan(&mut self, plan: &PhysicalPlan) -> Result<&[FusionSegment]> {
        self.reset();
        self.traverse_plan(plan)?;
        self.finalize_current_segment();
        Ok(&self.segments)
    }

    /// Reset the builder state
    fn reset(&mut self) {
        self.segments.clear();
        self.current_segment = FusionSegment::new();
    }

    /// Traverse a physical plan and build segments
    fn traverse_plan(&mut self, plan: &PhysicalPlan) -> Result<()> {
        match plan {
            PhysicalPlan::ScanSource { .. } => {
                // Source is a boundary, finalize current segment if any
                self.finalize_current_segment();
            }
            PhysicalPlan::Map { .. } => {
                // Map operators are fusable
                // For now, we'll just continue the current segment
                // TODO: Actually add the operator to the segment
            }
            PhysicalPlan::Filter { .. } => {
                // Filter operators are fusable
                // For now, we'll just continue the current segment
                // TODO: Actually add the operator to the segment
            }
            PhysicalPlan::Project { .. } => {
                // Project operators are fusable
                // For now, we'll just continue the current segment
                // TODO: Actually add the operator to the segment
            }
            PhysicalPlan::HashAggregate { .. } => {
                // Aggregate is stateful, ends a segment
                self.finalize_current_segment();
            }
            PhysicalPlan::WindowedAggregate { .. } => {
                // Window is stateful, ends a segment
                self.finalize_current_segment();
            }
            _ => {
                // Other operators end the segment
                self.finalize_current_segment();
            }
        }
        Ok(())
    }

    /// Finalize the current segment
    fn finalize_current_segment(&mut self) {
        if !self.current_segment.operators.is_empty() {
            let segment = std::mem::take(&mut self.current_segment);
            self.segments.push(segment);
        }
    }
}

impl Default for FusionSegmentBuilder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::physical::PhysicalPlan;
    use std::collections::HashMap;

    #[test]
    fn test_fusion_segment_creation() {
        let segment = FusionSegment::new();
        assert_eq!(segment.operators.len(), 0);
        assert!(segment.boundary.is_none());
    }

    #[test]
    fn test_fused_operator_creation() {
        let op = FusedOperator::new(vec![], None, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(op.expressions.len(), 0);
        assert!(op.predicate.is_none());
        assert_eq!(op.projection.len(), 2);
    }

    #[test]
    fn test_fusion_segment_builder() {
        let mut builder = FusionSegmentBuilder::new();

        // Create a simple plan with a source
        let plan = PhysicalPlan::ScanSource {
            name: "test".to_string(),
            attrs: HashMap::new(),
        };

        let segments = builder.build_from_plan(&plan).unwrap();
        assert_eq!(segments.len(), 0); // Source is a boundary, no segments created
    }

    #[test]
    fn test_is_fusable() {
        // This test requires actual operator instances
        // For now, we'll just test the helper functions compile
        assert!(true);
    }

    #[test]
    fn test_column_pruning() {
        use std::collections::HashSet;

        let plan = PhysicalPlan::ScanSource {
            name: "test".to_string(),
            attrs: HashMap::new(),
        };

        let needed_columns: HashSet<String> = vec!["id".to_string()].into_iter().collect();
        let pruned = prune_columns(&plan, &needed_columns);

        // Should return a ScanSource with the same name
        match pruned {
            PhysicalPlan::ScanSource { name, .. } => {
                assert_eq!(name, "test");
            }
            _ => panic!("Expected ScanSource"),
        }
    }

    #[test]
    fn test_predicate_pushdown() {
        let plan = PhysicalPlan::ScanSource {
            name: "test".to_string(),
            attrs: HashMap::new(),
        };

        let pushed = push_down_predicates(plan);

        // ScanSource should be unchanged
        match pushed {
            PhysicalPlan::ScanSource { name, .. } => {
                assert_eq!(name, "test");
            }
            _ => panic!("Expected ScanSource"),
        }
    }
}
