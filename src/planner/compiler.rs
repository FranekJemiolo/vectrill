//! Logical to Physical Plan Compiler

use std::collections::HashMap;

use crate::error::VectrillError;
use crate::expression::create_physical_expr;
use crate::planner::logical::{AggFunction, AggSpec, LogicalPlan, WindowSpec, WindowType};
#[allow(unused_imports)]
use crate::planner::physical::{PhysicalAggregation, PhysicalPlan, SortKey};

/// Compiler that converts logical plans to physical plans
pub struct PlanCompiler {
    // Could add compilation options here
}

impl PlanCompiler {
    /// Create a new plan compiler
    pub fn new() -> Self {
        Self {}
    }

    /// Compile a logical plan to a physical plan
    pub fn compile(&self, logical_plan: &LogicalPlan) -> Result<PhysicalPlan, VectrillError> {
        match logical_plan {
            LogicalPlan::Source { name, attrs: _ } => Ok(PhysicalPlan::ScanSource {
                name: name.clone(),
                attrs: HashMap::new(),
            }),

            LogicalPlan::Filter { input, expr } => {
                let input_physical = self.compile(input)?;
                let physical_expr = create_physical_expr(expr, &input_physical.schema())
                    .map_err(|e| VectrillError::ExpressionError(e.to_string()))?;

                Ok(PhysicalPlan::Filter {
                    input: Box::new(input_physical),
                    expr: physical_expr,
                })
            }

            LogicalPlan::Map { input, expr } => {
                let input_physical = self.compile(input)?;
                let physical_expr = create_physical_expr(expr, &input_physical.schema())
                    .map_err(|e| VectrillError::ExpressionError(e.to_string()))?;

                Ok(PhysicalPlan::Map {
                    input: Box::new(input_physical),
                    expr: physical_expr,
                })
            }

            LogicalPlan::GroupBy { input, keys } => {
                let input_physical = self.compile(input)?;

                // GroupBy without aggregations becomes a projection + sort
                let mut column_indices = Vec::new();
                let input_schema = input_physical.schema();

                for key in keys {
                    if let Some((col_idx, _)) = input_schema.column_with_name(key) {
                        column_indices.push(col_idx);
                    }
                }

                Ok(PhysicalPlan::Project {
                    input: Box::new(input_physical),
                    columns: column_indices,
                })
            }

            LogicalPlan::Window { input, spec } => {
                let input_physical = self.compile(input)?;

                // For now, implement WindowedAggregate with no aggregations
                // In a real implementation, this would be more sophisticated
                Ok(PhysicalPlan::WindowedAggregate {
                    input: Box::new(input_physical),
                    window: spec.clone(),
                    aggregations: Vec::new(),
                })
            }

            LogicalPlan::Aggregate { input, spec } => {
                let input_physical = self.compile(input)?;

                let mut physical_aggs = Vec::new();
                for (column, function) in &spec.aggregations {
                    let physical_agg = PhysicalAggregation {
                        column: column.clone(),
                        function: function.clone(),
                        alias: None,
                    };
                    physical_aggs.push(physical_agg);
                }

                Ok(PhysicalPlan::HashAggregate {
                    input: Box::new(input_physical),
                    keys: Vec::new(), // For now, no grouping keys
                    aggregations: physical_aggs,
                })
            }

            LogicalPlan::Project { input, columns } => {
                let input_physical = self.compile(input)?;
                let input_schema = input_physical.schema();

                let mut column_indices = Vec::new();
                for column in columns {
                    if let Some((col_idx, _)) = input_schema.column_with_name(column) {
                        column_indices.push(col_idx);
                    }
                }

                Ok(PhysicalPlan::Project {
                    input: Box::new(input_physical),
                    columns: column_indices,
                })
            }
        }
    }

    /// Validate that a physical plan is executable
    pub fn validate(&self, physical_plan: &PhysicalPlan) -> Result<(), VectrillError> {
        // Check schema consistency
        self.validate_schema_consistency(physical_plan)?;

        // Check for unsupported operations
        self.validate_operations(physical_plan)?;

        Ok(())
    }

    /// Validate schema consistency through the plan
    fn validate_schema_consistency(&self, plan: &PhysicalPlan) -> Result<(), VectrillError> {
        match plan {
            PhysicalPlan::ScanSource { .. } => Ok(()),

            PhysicalPlan::Filter { input, expr } => {
                let _input_schema = input.schema();
                let expr_schema = expr.data_type();

                // Filter expression should return boolean
                if !matches!(expr_schema, arrow::datatypes::DataType::Boolean) {
                    return Err(VectrillError::InvalidExpression(
                        "Filter expression must return boolean".to_string(),
                    ));
                }

                self.validate_schema_consistency(input)
            }

            PhysicalPlan::Map { input, expr: _ } => self.validate_schema_consistency(input),

            PhysicalPlan::HashAggregate {
                input,
                keys,
                aggregations,
            } => {
                let input_schema = input.schema();

                // Check that all keys exist in input schema
                for key in keys {
                    if input_schema.column_with_name(key).is_none() {
                        return Err(VectrillError::InvalidSchema(format!(
                            "Group key '{}' not found in input schema",
                            key
                        )));
                    }
                }

                // Check that all aggregation columns exist in input schema
                for agg in aggregations {
                    if input_schema.column_with_name(&agg.column).is_none() {
                        return Err(VectrillError::InvalidSchema(format!(
                            "Aggregation column '{}' not found in input schema",
                            agg.column
                        )));
                    }
                }

                self.validate_schema_consistency(input)
            }

            PhysicalPlan::WindowedAggregate {
                input,
                window: _,
                aggregations,
            } => {
                let input_schema = input.schema();

                // Check that all aggregation columns exist in input schema
                for agg in aggregations {
                    if input_schema.column_with_name(&agg.column).is_none() {
                        return Err(VectrillError::InvalidSchema(format!(
                            "Aggregation column '{}' not found in input schema",
                            agg.column
                        )));
                    }
                }

                self.validate_schema_consistency(input)
            }

            PhysicalPlan::Project { input, columns } => {
                let input_schema = input.schema();

                // Check that all projected columns exist in input schema
                for &col_idx in columns {
                    if col_idx >= input_schema.fields().len() {
                        return Err(VectrillError::InvalidSchema(format!(
                            "Column index {} out of bounds for input schema with {} columns",
                            col_idx,
                            input_schema.fields().len()
                        )));
                    }
                }

                self.validate_schema_consistency(input)
            }

            PhysicalPlan::Sort { input, sort_keys } => {
                let input_schema = input.schema();

                // Check that all sort keys exist in input schema
                for sort_key in sort_keys {
                    if input_schema.column_with_name(&sort_key.column).is_none() {
                        return Err(VectrillError::InvalidSchema(format!(
                            "Sort key column '{}' not found in input schema",
                            sort_key.column
                        )));
                    }
                }

                self.validate_schema_consistency(input)
            }

            PhysicalPlan::Limit {
                input,
                limit: _,
                offset: _,
            } => self.validate_schema_consistency(input),
        }
    }

    /// Validate that all operations are supported
    fn validate_operations(&self, plan: &PhysicalPlan) -> Result<(), VectrillError> {
        match plan {
            PhysicalPlan::ScanSource { name, attrs: _ } => {
                // Check if source type is supported
                match name.as_str() {
                    "memory" | "file" | "kafka" => Ok(()),
                    _ => Err(VectrillError::InvalidExpression(format!(
                        "Unsupported source type: {}",
                        name
                    ))),
                }
            }

            PhysicalPlan::Filter { input, .. } => self.validate_operations(input),

            PhysicalPlan::Map { input, .. } => self.validate_operations(input),

            PhysicalPlan::HashAggregate { input, .. } => self.validate_operations(input),

            PhysicalPlan::WindowedAggregate { input, .. } => self.validate_operations(input),

            PhysicalPlan::Project { input, .. } => self.validate_operations(input),

            PhysicalPlan::Sort { input, .. } => self.validate_operations(input),

            PhysicalPlan::Limit { input, .. } => self.validate_operations(input),
        }
    }
}

impl Default for PlanCompiler {
    fn default() -> Self {
        Self::new()
    }
}

/// Python DSL to Logical Plan compiler
pub struct PythonDSLCompiler;

impl PythonDSLCompiler {
    /// Compile a Python DSL node to a logical plan
    pub fn compile(node: &serde_json::Value) -> Result<LogicalPlan, VectrillError> {
        let op = node
            .get("op")
            .and_then(|v| v.as_str())
            .ok_or_else(|| VectrillError::InvalidExpression("Missing 'op' field".to_string()))?;

        let attrs = node
            .get("attrs")
            .and_then(|v| v.as_object())
            .map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
                    .collect::<HashMap<String, String>>()
            })
            .unwrap_or_default();

        let inputs = node
            .get("inputs")
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().map(Self::compile).collect::<Result<Vec<_>, _>>())
            .transpose()?
            .unwrap_or_default();

        match op {
            "source" => {
                let name = attrs
                    .get("name")
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());

                Ok(LogicalPlan::Source { name, attrs })
            }

            "filter" => {
                if inputs.len() != 1 {
                    return Err(VectrillError::InvalidExpression(
                        "Filter requires exactly one input".to_string(),
                    ));
                }

                let expr_str = attrs.get("expr").ok_or_else(|| {
                    VectrillError::InvalidExpression("Filter missing 'expr'".to_string())
                })?;

                let expr_result = crate::expression::compile_python_expression(expr_str, None);
                let expr = if expr_result.errors.is_empty() {
                    expr_result.expr
                } else {
                    return Err(VectrillError::ExpressionError(
                        expr_result.errors.join("; "),
                    ));
                };

                Ok(LogicalPlan::Filter {
                    input: Box::new(inputs[0].clone()),
                    expr,
                })
            }

            "map" => {
                if inputs.len() != 1 {
                    return Err(VectrillError::InvalidExpression(
                        "Map requires exactly one input".to_string(),
                    ));
                }

                let expr_str = attrs.get("expr").ok_or_else(|| {
                    VectrillError::InvalidExpression("Map missing 'expr'".to_string())
                })?;

                let expr_result = crate::expression::compile_python_expression(expr_str, None);
                let expr = if expr_result.errors.is_empty() {
                    expr_result.expr
                } else {
                    return Err(VectrillError::ExpressionError(
                        expr_result.errors.join("; "),
                    ));
                };

                Ok(LogicalPlan::Map {
                    input: Box::new(inputs[0].clone()),
                    expr,
                })
            }

            "group_by" => {
                if inputs.len() != 1 {
                    return Err(VectrillError::InvalidExpression(
                        "GroupBy requires exactly one input".to_string(),
                    ));
                }

                let key_str = attrs.get("key").ok_or_else(|| {
                    VectrillError::InvalidExpression("GroupBy missing 'key'".to_string())
                })?;

                let keys = vec![key_str.clone()];

                Ok(LogicalPlan::GroupBy {
                    input: Box::new(inputs[0].clone()),
                    keys,
                })
            }

            "window" => {
                if inputs.len() != 1 {
                    return Err(VectrillError::InvalidExpression(
                        "Window requires exactly one input".to_string(),
                    ));
                }

                let spec_str = attrs.get("spec").ok_or_else(|| {
                    VectrillError::InvalidExpression("Window missing 'spec'".to_string())
                })?;

                let spec = Self::parse_window_spec(spec_str)?;

                Ok(LogicalPlan::Window {
                    input: Box::new(inputs[0].clone()),
                    spec,
                })
            }

            "agg" => {
                if inputs.len() != 1 {
                    return Err(VectrillError::InvalidExpression(
                        "Aggregate requires exactly one input".to_string(),
                    ));
                }

                let spec_obj = node
                    .get("attrs")
                    .and_then(|v| v.get("spec"))
                    .and_then(|v| v.as_object())
                    .ok_or_else(|| {
                        VectrillError::InvalidExpression("Aggregate missing 'spec'".to_string())
                    })?;

                let mut aggregations = HashMap::new();
                for (col, fn_str) in spec_obj {
                    if let Some(fn_str) = fn_str.as_str() {
                        let agg_fn = Self::parse_agg_function(fn_str)?;
                        aggregations.insert(col.clone(), agg_fn);
                    }
                }

                let spec = AggSpec { aggregations };

                Ok(LogicalPlan::Aggregate {
                    input: Box::new(inputs[0].clone()),
                    spec,
                })
            }

            "project" => {
                if inputs.len() != 1 {
                    return Err(VectrillError::InvalidExpression(
                        "Project requires exactly one input".to_string(),
                    ));
                }

                let columns = vec!["id".to_string()]; // Simplified for now

                Ok(LogicalPlan::Project {
                    input: Box::new(inputs[0].clone()),
                    columns,
                })
            }

            _ => Err(VectrillError::InvalidExpression(format!(
                "Unsupported operation: {}",
                op
            ))),
        }
    }

    /// Parse window specification string
    fn parse_window_spec(spec_str: &str) -> Result<WindowSpec, VectrillError> {
        // Simple parsing for window specifications
        // Format: "tumbling(duration)", "sliding(duration, slide)", "session(timeout)"
        if spec_str.starts_with("tumbling(") {
            let duration = spec_str
                .strip_prefix("tumbling(")
                .and_then(|s| s.strip_suffix(')'))
                .ok_or_else(|| {
                    VectrillError::InvalidExpression("Invalid tumbling window spec".to_string())
                })?;

            Ok(WindowSpec {
                window_type: WindowType::Tumbling,
                duration: duration.to_string(),
                slide: None,
                timeout: None,
            })
        } else if spec_str.starts_with("sliding(") {
            let inner = spec_str
                .strip_prefix("sliding(")
                .and_then(|s| s.strip_suffix(')'))
                .ok_or_else(|| {
                    VectrillError::InvalidExpression("Invalid sliding window spec".to_string())
                })?;

            let parts: Vec<&str> = inner.split(',').collect();
            if parts.len() != 2 {
                return Err(VectrillError::InvalidExpression(
                    "Sliding window requires duration and slide".to_string(),
                ));
            }

            Ok(WindowSpec {
                window_type: WindowType::Sliding,
                duration: parts[0].trim().to_string(),
                slide: Some(parts[1].trim().to_string()),
                timeout: None,
            })
        } else if spec_str.starts_with("session(") {
            let timeout = spec_str
                .strip_prefix("session(")
                .and_then(|s| s.strip_suffix(')'))
                .ok_or_else(|| {
                    VectrillError::InvalidExpression("Invalid session window spec".to_string())
                })?;

            Ok(WindowSpec {
                window_type: WindowType::Session,
                duration: String::new(), // Not used for session windows
                slide: None,
                timeout: Some(timeout.to_string()),
            })
        } else {
            Err(VectrillError::InvalidExpression(format!(
                "Unknown window specification: {}",
                spec_str
            )))
        }
    }

    /// Parse aggregation function string
    fn parse_agg_function(fn_str: &str) -> Result<AggFunction, VectrillError> {
        match fn_str {
            "sum" => Ok(AggFunction::Sum),
            "avg" => Ok(AggFunction::Avg),
            "count" => Ok(AggFunction::Count),
            "min" => Ok(AggFunction::Min),
            "max" => Ok(AggFunction::Max),
            "first" => Ok(AggFunction::First),
            "last" => Ok(AggFunction::Last),
            _ => Err(VectrillError::InvalidExpression(format!(
                "Unknown aggregation function: {}",
                fn_str
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::{Expr, Operator, ScalarValue};
    use std::collections::HashMap;

    #[test]
    fn test_compiler_creation() {
        let compiler = PlanCompiler::new();
        assert!(true); // Just test creation
    }

    #[test]
    fn test_source_compilation() {
        let compiler = PlanCompiler::new();
        let logical_plan = LogicalPlan::Source {
            name: "test".to_string(),
            attrs: HashMap::new(),
        };

        let physical_plan = compiler.compile(&logical_plan).unwrap();
        match physical_plan {
            PhysicalPlan::ScanSource { name, .. } => {
                assert_eq!(name, "test");
            }
            _ => panic!("Expected ScanSource"),
        }
    }

    #[test]
    fn test_filter_compilation() {
        let compiler = PlanCompiler::new();
        let source = LogicalPlan::Source {
            name: "test".to_string(),
            attrs: HashMap::new(),
        };

        let filter_expr = Expr::column("id");
        let filter_plan = LogicalPlan::Filter {
            input: Box::new(source),
            expr: filter_expr,
        };

        let physical_plan = compiler.compile(&filter_plan).unwrap();
        match physical_plan {
            PhysicalPlan::Filter { input, expr } => {
                match *input {
                    PhysicalPlan::ScanSource { .. } => {
                        // Should have scan source as input
                        assert!(true);
                    }
                    _ => panic!("Expected ScanSource as input"),
                }
                // Should have compiled expression
                assert_eq!(expr.as_string(), "id");
            }
            _ => panic!("Expected Filter"),
        }
    }

    #[test]
    fn test_python_dsl_compilation() {
        let dsl_json = serde_json::json!({
            "op": "source",
            "inputs": [],
            "attrs": {
                "name": "test_source"
            }
        });

        let logical_plan = PythonDSLCompiler::compile(&dsl_json).unwrap();
        match logical_plan {
            LogicalPlan::Source { name, .. } => {
                assert_eq!(name, "test_source");
            }
            _ => panic!("Expected Source"),
        }
    }

    #[test]
    fn test_window_spec_parsing() {
        let tumbling = PythonDSLCompiler::parse_window_spec("tumbling(10s)").unwrap();
        assert!(matches!(tumbling.window_type, WindowType::Tumbling));
        assert_eq!(tumbling.duration, "10s");

        let sliding = PythonDSLCompiler::parse_window_spec("sliding(1m, 30s)").unwrap();
        assert!(matches!(sliding.window_type, WindowType::Sliding));
        assert_eq!(sliding.duration, "1m");
        assert_eq!(sliding.slide.unwrap(), "30s");

        let session = PythonDSLCompiler::parse_window_spec("session(5m)").unwrap();
        assert!(matches!(session.window_type, WindowType::Session));
        assert_eq!(session.timeout.unwrap(), "5m");
    }

    #[test]
    fn test_agg_function_parsing() {
        let sum_fn = PythonDSLCompiler::parse_agg_function("sum").unwrap();
        assert!(matches!(sum_fn, AggFunction::Sum));

        let avg_fn = PythonDSLCompiler::parse_agg_function("avg").unwrap();
        assert!(matches!(avg_fn, AggFunction::Avg));

        let count_fn = PythonDSLCompiler::parse_agg_function("count").unwrap();
        assert!(matches!(count_fn, AggFunction::Count));
    }
}
