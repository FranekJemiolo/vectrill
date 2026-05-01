//! Execution Graph Builder and Executor

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::error::VectrillError;
use crate::operators::pipeline::{Operator as PipelineOperator, Pipeline};
use crate::operators::{FilterOperator, MapOperator};
use crate::planner::physical::PhysicalPlan;
use crate::RecordBatch;

/// Unique node identifier
pub type NodeId = u64;

/// Execution node in the DAG
#[derive(Debug)]
pub struct ExecNode {
    /// Unique node identifier
    pub id: NodeId,
    /// Physical plan for this node
    pub plan: PhysicalPlan,
    /// Input node IDs
    pub inputs: Vec<NodeId>,
    /// Output node IDs (filled during graph construction)
    pub outputs: Vec<NodeId>,
}

/// Execution graph representing the query plan
#[derive(Debug)]
pub struct ExecutionGraph {
    /// All nodes in the graph
    pub nodes: HashMap<NodeId, ExecNode>,
    /// Root nodes (sources)
    pub roots: Vec<NodeId>,
    /// Leaf nodes (sinks)
    pub leaves: Vec<NodeId>,
    /// Next node ID to assign
    next_id: AtomicU64,
}

impl ExecutionGraph {
    /// Create a new execution graph
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            roots: Vec::new(),
            leaves: Vec::new(),
            next_id: AtomicU64::new(1),
        }
    }

    /// Build an execution graph from a physical plan
    pub fn build_from_plan(&mut self, plan: PhysicalPlan) -> Result<NodeId, VectrillError> {
        let root_id = self.build_node_recursive(plan)?;
        self.update_connections();
        Ok(root_id)
    }

    /// Recursively build nodes from physical plan
    fn build_node_recursive(&mut self, plan: PhysicalPlan) -> Result<NodeId, VectrillError> {
        let node_id = self.next_id.fetch_add(1, Ordering::SeqCst);

        let mut input_ids = Vec::new();

        // Build input nodes first
        let plan_with_inputs = match plan {
            PhysicalPlan::ScanSource { .. } => {
                // Source node, no inputs
                plan
            }

            PhysicalPlan::Filter { input, expr } => {
                let input_id = self.build_node_recursive(*input)?;
                input_ids.push(input_id);

                PhysicalPlan::Filter {
                    input: Box::new(PhysicalPlan::ScanSource {
                        name: "placeholder".to_string(),
                        attrs: HashMap::new(),
                    }),
                    expr,
                }
            }

            PhysicalPlan::Map { input, expr } => {
                let input_id = self.build_node_recursive(*input)?;
                input_ids.push(input_id);

                PhysicalPlan::Map {
                    input: Box::new(PhysicalPlan::ScanSource {
                        name: "placeholder".to_string(),
                        attrs: HashMap::new(),
                    }),
                    expr,
                }
            }

            PhysicalPlan::HashAggregate {
                input,
                keys,
                aggregations,
            } => {
                let input_id = self.build_node_recursive(*input)?;
                input_ids.push(input_id);

                PhysicalPlan::HashAggregate {
                    input: Box::new(PhysicalPlan::ScanSource {
                        name: "placeholder".to_string(),
                        attrs: HashMap::new(),
                    }),
                    keys,
                    aggregations,
                }
            }

            PhysicalPlan::WindowedAggregate {
                input,
                window,
                aggregations,
            } => {
                let input_id = self.build_node_recursive(*input)?;
                input_ids.push(input_id);

                PhysicalPlan::WindowedAggregate {
                    input: Box::new(PhysicalPlan::ScanSource {
                        name: "placeholder".to_string(),
                        attrs: HashMap::new(),
                    }),
                    window,
                    aggregations,
                }
            }

            PhysicalPlan::Project { input, columns } => {
                let input_id = self.build_node_recursive(*input)?;
                input_ids.push(input_id);

                PhysicalPlan::Project {
                    input: Box::new(PhysicalPlan::ScanSource {
                        name: "placeholder".to_string(),
                        attrs: HashMap::new(),
                    }),
                    columns,
                }
            }

            PhysicalPlan::Sort { input, sort_keys } => {
                let input_id = self.build_node_recursive(*input)?;
                input_ids.push(input_id);

                PhysicalPlan::Sort {
                    input: Box::new(PhysicalPlan::ScanSource {
                        name: "placeholder".to_string(),
                        attrs: HashMap::new(),
                    }),
                    sort_keys,
                }
            }

            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => {
                let input_id = self.build_node_recursive(*input)?;
                input_ids.push(input_id);

                PhysicalPlan::Limit {
                    input: Box::new(PhysicalPlan::ScanSource {
                        name: "placeholder".to_string(),
                        attrs: HashMap::new(),
                    }),
                    limit,
                    offset,
                }
            }
        };

        // Create execution node
        let exec_node = ExecNode {
            id: node_id,
            plan: plan_with_inputs,
            inputs: input_ids,
            outputs: Vec::new(),
        };

        self.nodes.insert(node_id, exec_node);
        Ok(node_id)
    }

    /// Update input/output connections between nodes
    fn update_connections(&mut self) {
        // Clear existing connections
        for node in self.nodes.values_mut() {
            node.outputs.clear();
        }

        // Build connections
        let input_connections: Vec<(NodeId, NodeId)> = self
            .nodes
            .iter()
            .flat_map(|(node_id, node)| node.inputs.iter().map(|&input_id| (input_id, *node_id)))
            .collect();

        for (input_id, node_id) in input_connections {
            if let Some(input_node) = self.nodes.get_mut(&input_id) {
                input_node.outputs.push(node_id);
            }
        }

        // Identify roots and leaves
        self.roots.clear();
        self.leaves.clear();

        for (node_id, node) in &self.nodes {
            if node.inputs.is_empty() {
                self.roots.push(*node_id);
            }
            if node.outputs.is_empty() {
                self.leaves.push(*node_id);
            }
        }
    }

    /// Validate the execution graph
    pub fn validate(&self) -> Result<(), VectrillError> {
        // Check for cycles
        self.check_cycles()?;

        // Check connectivity
        self.check_connectivity()?;

        // Check that all nodes have valid plans
        self.check_node_plans()?;

        Ok(())
    }

    /// Check for cycles in the graph
    fn check_cycles(&self) -> Result<(), VectrillError> {
        let mut visited = HashSet::new();
        let mut recursion_stack = HashSet::new();

        for &node_id in &self.roots {
            if !visited.contains(&node_id) {
                self.dfs_cycle_check(node_id, &mut visited, &mut recursion_stack)?;
            }
        }

        Ok(())
    }

    /// Depth-first search for cycle detection
    fn dfs_cycle_check(
        &self,
        node_id: NodeId,
        visited: &mut HashSet<NodeId>,
        recursion_stack: &mut HashSet<NodeId>,
    ) -> Result<(), VectrillError> {
        visited.insert(node_id);
        recursion_stack.insert(node_id);

        if let Some(node) = self.nodes.get(&node_id) {
            for &output_id in &node.outputs {
                if recursion_stack.contains(&output_id) {
                    return Err(VectrillError::InvalidExpression(format!(
                        "Cycle detected in execution graph involving nodes {} and {}",
                        node_id, output_id
                    )));
                }

                if !visited.contains(&output_id) {
                    self.dfs_cycle_check(output_id, visited, recursion_stack)?;
                }
            }
        }

        recursion_stack.remove(&node_id);
        Ok(())
    }

    /// Check graph connectivity
    fn check_connectivity(&self) -> Result<(), VectrillError> {
        if self.roots.is_empty() {
            return Err(VectrillError::InvalidExpression(
                "Execution graph has no root nodes".to_string(),
            ));
        }

        if self.leaves.is_empty() {
            return Err(VectrillError::InvalidExpression(
                "Execution graph has no leaf nodes".to_string(),
            ));
        }

        // Check that all nodes are reachable from roots
        let mut reachable = HashSet::new();
        for &root_id in &self.roots {
            self.dfs_reachable(root_id, &mut reachable);
        }

        if reachable.len() != self.nodes.len() {
            return Err(VectrillError::InvalidExpression(format!(
                "Execution graph has {} unreachable nodes",
                self.nodes.len() - reachable.len()
            )));
        }

        Ok(())
    }

    /// DFS to find reachable nodes
    fn dfs_reachable(&self, node_id: NodeId, reachable: &mut HashSet<NodeId>) {
        if reachable.contains(&node_id) {
            return;
        }

        reachable.insert(node_id);

        if let Some(node) = self.nodes.get(&node_id) {
            for &output_id in &node.outputs {
                self.dfs_reachable(output_id, reachable);
            }
        }
    }

    /// Check that all nodes have valid plans
    fn check_node_plans(&self) -> Result<(), VectrillError> {
        for (node_id, node) in &self.nodes {
            if let PhysicalPlan::ScanSource { name, attrs: _ } = &node.plan {
                if name.is_empty() {
                    return Err(VectrillError::InvalidExpression(format!(
                        "Node {} has empty source name",
                        node_id
                    )));
                }
            }
        }
        Ok(())
    }

    /// Get topological order of nodes
    pub fn topological_order(&self) -> Result<Vec<NodeId>, VectrillError> {
        let mut visited = HashSet::new();
        let mut order = Vec::new();

        for &root_id in &self.roots {
            self.dfs_topological(root_id, &mut visited, &mut order);
        }

        if order.len() != self.nodes.len() {
            return Err(VectrillError::InvalidExpression(
                "Topological sort failed - graph may have cycles".to_string(),
            ));
        }

        Ok(order)
    }

    /// DFS for topological sort
    fn dfs_topological(
        &self,
        node_id: NodeId,
        visited: &mut HashSet<NodeId>,
        order: &mut Vec<NodeId>,
    ) {
        if visited.contains(&node_id) {
            return;
        }

        visited.insert(node_id);

        if let Some(node) = self.nodes.get(&node_id) {
            for &output_id in &node.outputs {
                self.dfs_topological(output_id, visited, order);
            }
        }

        order.push(node_id);
    }

    /// Create an execution pipeline from the graph
    pub fn create_pipeline(&self) -> Result<Pipeline, VectrillError> {
        let mut pipeline = Pipeline::new();
        let order = self.topological_order()?;

        for node_id in order {
            let node = self.nodes.get(&node_id).unwrap();
            let operator = self.create_operator_from_node(node)?;
            pipeline = pipeline.add_operator(operator);
        }

        Ok(pipeline)
    }

    /// Create a pipeline operator from an execution node
    fn create_operator_from_node(
        &self,
        node: &ExecNode,
    ) -> Result<Box<dyn PipelineOperator>, VectrillError> {
        match &node.plan {
            PhysicalPlan::ScanSource { name, attrs: _ } => {
                // Create a source operator
                Ok(Box::new(SourceOperator::new(name.clone(), HashMap::new())))
            }

            PhysicalPlan::Filter { expr, .. } => {
                // Create a filter operator
                Ok(Box::new(FilterOperator::new(expr.clone())))
            }

            PhysicalPlan::Map { expr, .. } => {
                // Create a map operator
                Ok(Box::new(MapOperator::new(vec![(
                    "computed".to_string(),
                    expr.clone(),
                )])))
            }

            PhysicalPlan::Project { columns: _, .. } => {
                // Create a projection operator - simplified implementation
                Ok(Box::new(PassThroughOperator::new("projection".to_string())))
            }

            _ => {
                // For other operators, create a pass-through placeholder
                Ok(Box::new(PassThroughOperator::new(format!(
                    "{:?}",
                    node.plan
                ))))
            }
        }
    }

    /// Get a string representation of the graph
    #[allow(clippy::inherent_to_string)]
    pub fn to_string(&self) -> String {
        let mut result = String::new();
        result.push_str("Execution Graph:\n");

        for (node_id, node) in &self.nodes {
            result.push_str(&format!("  Node {}:\n", node_id));
            result.push_str(&format!("    Plan: {}\n", node.plan.to_string(2)));
            result.push_str(&format!("    Inputs: {:?}\n", node.inputs));
            result.push_str(&format!("    Outputs: {:?}\n", node.outputs));
        }

        result.push_str(&format!("Roots: {:?}\n", self.roots));
        result.push_str(&format!("Leaves: {:?}\n", self.leaves));
        result.push_str(&format!("Nodes: {:?}\n", self.nodes));
        result
    }
}

impl Default for ExecutionGraph {
    fn default() -> Self {
        Self::new()
    }
}

/// Source operator that reads from a data source
#[allow(dead_code)]
#[derive(Debug)]
pub struct SourceOperator {
    name: String,
    attrs: HashMap<String, String>,
}

impl SourceOperator {
    pub fn new(name: String, attrs: HashMap<String, String>) -> Self {
        Self { name, attrs }
    }
}

impl PipelineOperator for SourceOperator {
    fn process(&mut self, _batch: RecordBatch) -> crate::error::Result<RecordBatch> {
        // For now, return an empty batch
        // In a real implementation, this would read from the source
        let schema = arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
            arrow::datatypes::Field::new("data", arrow::datatypes::DataType::Utf8, true),
        ]);

        let batch = RecordBatch::new_empty(Arc::new(schema));
        Ok(batch)
    }
}

/// Pass-through operator for unimplemented operators
#[allow(dead_code)]
#[derive(Debug)]
pub struct PassThroughOperator {
    name: String,
}

impl PassThroughOperator {
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl PipelineOperator for PassThroughOperator {
    fn process(&mut self, batch: RecordBatch) -> crate::error::Result<RecordBatch> {
        // Just pass through the batch
        Ok(batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::physical::PhysicalPlan;
    use std::collections::HashMap;

    #[test]
    fn test_execution_graph_creation() {
        let graph = ExecutionGraph::new();
        assert_eq!(graph.nodes.len(), 0);
        assert!(graph.roots.is_empty());
        assert!(graph.leaves.is_empty());
    }

    #[test]
    fn test_simple_graph_building() {
        let mut graph = ExecutionGraph::new();

        let source_plan = PhysicalPlan::ScanSource {
            name: "test".to_string(),
            attrs: HashMap::new(),
        };

        let root_id = graph.build_from_plan(source_plan).unwrap();

        assert_eq!(graph.nodes.len(), 1);
        assert_eq!(graph.roots.len(), 1);
        assert_eq!(graph.leaves.len(), 1);
        assert_eq!(graph.roots[0], root_id);
        assert_eq!(graph.leaves[0], root_id);
    }

    #[test]
    fn test_graph_validation() {
        let mut graph = ExecutionGraph::new();

        let source_plan = PhysicalPlan::ScanSource {
            name: "test".to_string(),
            attrs: HashMap::new(),
        };

        graph.build_from_plan(source_plan).unwrap();

        // Should validate successfully
        assert!(graph.validate().is_ok());
    }

    #[test]
    fn test_topological_order() {
        let mut graph = ExecutionGraph::new();

        let source_plan = PhysicalPlan::ScanSource {
            name: "test".to_string(),
            attrs: HashMap::new(),
        };

        graph.build_from_plan(source_plan).unwrap();

        let order = graph.topological_order().unwrap();
        assert_eq!(order.len(), 1);
    }

    #[test]
    fn test_source_operator() {
        let mut operator = SourceOperator::new("test".to_string(), HashMap::new());

        let schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
            "id",
            arrow::datatypes::DataType::Int64,
            false,
        )]);
        let batch = RecordBatch::new_empty(Arc::new(schema));

        let result = operator.process(batch).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn test_pass_through_operator() {
        let mut operator = PassThroughOperator::new("test".to_string());

        let schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
            "id",
            arrow::datatypes::DataType::Int64,
            false,
        )]);
        let batch = RecordBatch::new_empty(Arc::new(schema));

        let result = operator.process(batch).unwrap();
        assert_eq!(result.num_rows(), 0);
    }
}
