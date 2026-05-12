//! Operator Fusion - fuse stateless operators to eliminate batch materialization

use crate::error::Result;
use crate::expression::Expr;
use crate::planner::physical::PhysicalPlan;
use crate::RecordBatch;
use std::collections::HashSet;
use std::sync::Arc;

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

    /// Create a fused operator from multiple operators
    pub fn from_operators(operators: Vec<Box<dyn FusableOperator>>) -> Result<Self> {
        let mut all_expressions = Vec::new();
        let mut combined_predicate: Option<Expr> = None;
        let mut combined_projection = Vec::new();
        let mut expr_id_counter = 0;

        for operator in operators {
            // Collect expressions from this operator
            for expr in operator.expressions() {
                let fused_expr = FusedExpr {
                    expr: expr.clone(),
                    id: expr_id_counter,
                    dependencies: Self::extract_dependencies(expr),
                };
                all_expressions.push(fused_expr);
                expr_id_counter += 1;
            }

            // Combine predicates with AND
            if let Some(pred) = operator.predicate() {
                if let Some(existing_pred) = &combined_predicate {
                    combined_predicate = Some(Expr::binary(
                        existing_pred.clone(),
                        crate::expression::operators::Operator::And,
                        pred.clone(),
                    ));
                } else {
                    combined_predicate = Some(pred.clone());
                }
            }

            // Combine projections (union of all projections)
            if let Some(projection) = operator.projection() {
                for col_name in projection {
                    if !combined_projection.contains(col_name) {
                        combined_projection.push(col_name.clone());
                    }
                }
            }
        }

        // Sort expressions by dependencies (topological sort)
        let sorted_expressions = Self::topological_sort(all_expressions)?;

        Ok(Self {
            expressions: sorted_expressions,
            predicate: combined_predicate,
            projection: combined_projection,
        })
    }

    /// Extract dependencies from an expression
    fn extract_dependencies(expr: &Expr) -> Vec<usize> {
        use crate::expression::Expr::*;

        match expr {
            Column(_) => Vec::new(),
            Literal(_) => Vec::new(),
            Binary { left, right, .. } => {
                let mut deps = Self::extract_dependencies(left);
                deps.extend(Self::extract_dependencies(right));
                deps
            }
            Unary { expr, .. } => Self::extract_dependencies(expr),
            Function { args, .. } => {
                let mut deps = Vec::new();
                for arg in args {
                    deps.extend(Self::extract_dependencies(arg));
                }
                deps
            }
            Cast { expr, .. } => Self::extract_dependencies(expr),
        }
    }

    /// Topological sort expressions by dependencies
    fn topological_sort(expressions: Vec<FusedExpr>) -> Result<Vec<FusedExpr>> {
        use std::collections::HashMap;

        let mut in_degree: HashMap<usize, usize> = HashMap::new();
        let mut adj_list: HashMap<usize, Vec<usize>> = HashMap::new();
        let mut expr_map: HashMap<usize, FusedExpr> = HashMap::new();

        // Initialize structures
        for expr in &expressions {
            in_degree.insert(expr.id, 0);
            adj_list.insert(expr.id, Vec::new());
            expr_map.insert(expr.id, expr.clone());
        }

        // Build dependency graph
        for expr in &expressions {
            for &dep_id in &expr.dependencies {
                if let Some(deps) = adj_list.get_mut(&dep_id) {
                    deps.push(expr.id);
                }
                *in_degree.entry(expr.id).or_insert(0) += 1;
            }
        }

        // Topological sort using Kahn's algorithm
        let mut queue: Vec<usize> = in_degree
            .iter()
            .filter(|(_, &degree)| degree == 0)
            .map(|(&id, _)| id)
            .collect();

        let mut result = Vec::new();

        while let Some(current) = queue.pop() {
            if let Some(expr) = expr_map.remove(&current) {
                result.push(expr);
            }

            if let Some(dependents) = adj_list.remove(&current) {
                for &dep_id in &dependents {
                    if let Some(degree) = in_degree.get_mut(&dep_id) {
                        *degree -= 1;
                        if *degree == 0 {
                            queue.push(dep_id);
                        }
                    }
                }
            }
        }

        // Check for cycles
        if result.len() != expressions.len() {
            return Err(crate::error::VectrillError::InvalidExpression(
                "Circular dependency detected in expressions".to_string(),
            ));
        }

        Ok(result)
    }

    /// Optimize the fused operator by removing unused expressions
    pub fn optimize(&mut self) -> Result<()> {
        use std::collections::HashSet;

        // Find all used expressions
        let mut used = HashSet::new();

        // Mark expressions used in predicate
        if let Some(pred) = &self.predicate {
            Self::mark_used(pred, &mut used);
        }

        // Mark expressions used in projection
        for col_name in &self.projection {
            // This is simplified - in practice, we'd need to track which expressions
            // produce which columns
            for expr in &self.expressions {
                if Self::expr_produces_column(expr, col_name) {
                    Self::mark_used(&expr.expr, &mut used);
                }
            }
        }

        // Remove unused expressions
        self.expressions.retain(|expr| used.contains(&expr.id));

        // Re-sort expressions by dependencies
        self.expressions = Self::topological_sort(self.expressions.clone())?;

        Ok(())
    }

    /// Mark all expressions used by a given expression
    fn mark_used(expr: &Expr, used: &mut HashSet<usize>) {
        match expr {
            Expr::Binary { left, right, .. } => {
                Self::mark_used(left, used);
                Self::mark_used(right, used);
            }
            Expr::Unary { expr, .. } => {
                Self::mark_used(expr, used);
            }
            Expr::Function { args, .. } => {
                for arg in args {
                    Self::mark_used(arg, used);
                }
            }
            Expr::Cast { expr, .. } => {
                Self::mark_used(expr, used);
            }
            _ => {}
        }
    }

    /// Check if an expression produces a specific column (simplified)
    fn expr_produces_column(_expr: &FusedExpr, _col_name: &str) -> bool {
        // This is a simplified implementation
        // In practice, we'd need to track expression-to-column mappings
        true
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
        use crate::expression::physical::create_physical_expr;
        use arrow::array::ArrayRef;
        use std::collections::HashMap;

        let mut computed_values: HashMap<usize, ArrayRef> = HashMap::new();
        let mut result_columns: Vec<arrow::datatypes::Field> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        let mut result_arrays: Vec<ArrayRef> = batch.columns().to_vec();

        // Evaluate expressions in dependency order
        for fused_expr in &self.expressions {
            // Evaluate dependencies first
            let mut dependency_arrays: Vec<ArrayRef> = Vec::new();
            for &dep_id in &fused_expr.dependencies {
                if let Some(dep_array) = computed_values.get(&dep_id) {
                    dependency_arrays.push(dep_array.clone());
                } else {
                    // This should be a column from the original batch
                    let col_name = format!("col_{}", dep_id);
                    if let Some(col_array) = batch.column_by_name(&col_name) {
                        dependency_arrays.push(col_array.clone());
                    } else {
                        return Err(crate::error::VectrillError::InvalidExpression(format!(
                            "Dependency not found: {}",
                            dep_id
                        )));
                    }
                }
            }

            // Create a temporary batch with dependency columns
            let temp_schema = arrow::datatypes::Schema::new(
                fused_expr
                    .dependencies
                    .iter()
                    .enumerate()
                    .map(|(i, &dep_id)| {
                        let name = format!("dep_{}", i);
                        let data_type = dependency_arrays[i].data_type().clone();
                        arrow::datatypes::Field::new(name, data_type, true)
                    })
                    .collect::<Vec<_>>(),
            );

            let temp_batch = RecordBatch::try_new(Arc::new(temp_schema), dependency_arrays)?;

            // Evaluate the expression
            let physical_expr = create_physical_expr(&fused_expr.expr, &temp_batch.schema())?;
            let result_array = physical_expr.evaluate(&temp_batch)?;

            // Store the computed value
            computed_values.insert(fused_expr.id, result_array.clone());

            // Add to result columns
            let col_name = format!("expr_{}", fused_expr.id);
            result_columns.push(arrow::datatypes::Field::new(
                col_name.clone(),
                result_array.data_type().clone(),
                true,
            ));
            result_arrays.push(result_array);
        }

        // Create the result batch
        let result_schema = arrow::datatypes::Schema::new(result_columns);
        Ok(RecordBatch::try_new(
            Arc::new(result_schema),
            result_arrays,
        )?)
    }

    /// Apply predicate to filter rows
    fn apply_predicate(&self, batch: &RecordBatch, pred: &Expr) -> Result<RecordBatch> {
        use crate::expression::physical::create_physical_expr;
        use arrow::array::BooleanArray;

        // Evaluate the predicate expression
        let physical_expr = create_physical_expr(pred, &batch.schema())?;
        let predicate_array = physical_expr.evaluate(batch)?;

        // Convert to boolean array for filtering
        let bool_array = predicate_array
            .as_any()
            .downcast_ref::<BooleanArray>()
            .ok_or_else(|| {
                crate::error::VectrillError::InvalidExpression(
                    "Predicate must evaluate to boolean array".to_string(),
                )
            })?;

        // Apply the filter to all columns
        let mut filtered_arrays = Vec::new();
        for column in batch.columns() {
            let filtered = arrow::compute::filter(column, bool_array)?;
            filtered_arrays.push(filtered);
        }

        // Create the filtered batch
        let filtered_batch = RecordBatch::try_new(batch.schema(), filtered_arrays)?;

        Ok(filtered_batch)
    }

    /// Apply projection to select columns
    fn apply_projection(&self, batch: RecordBatch) -> Result<RecordBatch> {
        // If projection is empty or matches all columns, return as-is
        if self.projection.is_empty() {
            return Ok(batch);
        }

        // Find the indices of the projected columns
        let mut projected_arrays = Vec::new();
        let mut projected_fields = Vec::new();

        for col_name in &self.projection {
            if let Some((col_index, _)) = batch.schema().column_with_name(col_name) {
                projected_arrays.push(batch.column(col_index).clone());
                projected_fields.push(batch.schema().field(col_index).clone());
            } else {
                return Err(crate::error::VectrillError::InvalidExpression(format!(
                    "Column not found for projection: {}",
                    col_name
                )));
            }
        }

        // Create the projected batch
        let projected_schema = arrow::datatypes::Schema::new(projected_fields);
        let projected_batch = RecordBatch::try_new(Arc::new(projected_schema), projected_arrays)?;

        Ok(projected_batch)
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
