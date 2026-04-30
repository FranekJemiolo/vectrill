//! Logical Plan IR

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::expression::{Expr, ExprType, ScalarValue};
use crate::error::VectrillError;

/// Window specification for windowed operations
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WindowSpec {
    pub window_type: WindowType,
    pub duration: String,
    pub slide: Option<String>,
    pub timeout: Option<String>,
}

/// Window types
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum WindowType {
    Tumbling,
    Sliding,
    Session,
}

/// Aggregation specification
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggSpec {
    pub aggregations: HashMap<String, AggFunction>,
}

/// Aggregation functions
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AggFunction {
    Sum,
    Avg,
    Count,
    Min,
    Max,
    First,
    Last,
}

/// Logical plan representation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LogicalPlan {
    /// Source node - reads data from external systems
    Source {
        name: String,
        attrs: HashMap<String, String>,
    },
    
    /// Filter node - filters records based on predicate
    Filter {
        input: Box<LogicalPlan>,
        expr: Expr,
    },
    
    /// Map node - transforms records using expressions
    Map {
        input: Box<LogicalPlan>,
        expr: Expr,
    },
    
    /// GroupBy node - groups records by key
    GroupBy {
        input: Box<LogicalPlan>,
        keys: Vec<String>,
    },
    
    /// Window node - applies window specification
    Window {
        input: Box<LogicalPlan>,
        spec: WindowSpec,
    },
    
    /// Aggregate node - applies aggregations
    Aggregate {
        input: Box<LogicalPlan>,
        spec: AggSpec,
    },
    
    /// Project node - selects columns
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<String>,
    },
}

impl LogicalPlan {
    /// Get the schema for this plan node
    pub fn schema(&self) -> Result<arrow::datatypes::SchemaRef, VectrillError> {
        match self {
            LogicalPlan::Source { name: _, attrs } => {
                // For now, return a simple schema
                // In a real implementation, this would infer schema from source
                let schema = arrow::datatypes::Schema::new(vec![
                    arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
                    arrow::datatypes::Field::new("data", arrow::datatypes::DataType::Utf8, true),
                ]);
                Ok(Arc::new(schema))
            }
            
            LogicalPlan::Filter { input, expr: _ } => {
                // Filter doesn't change schema
                input.schema()
            }
            
            LogicalPlan::Map { input, expr } => {
                // Map can add new columns based on expressions
                let input_schema = input.schema()?;
                let mut fields = input_schema.fields().to_vec();
                
                // Add field for map expression result
                let data_type = arrow::datatypes::DataType::Utf8; // Simplified for now
                fields.push(Arc::new(arrow::datatypes::Field::new("computed", data_type, true)));
                
                let schema = arrow::datatypes::Schema::new(fields);
                Ok(Arc::new(schema))
            }
            
            LogicalPlan::GroupBy { input, keys } => {
                // GroupBy adds grouping keys
                let input_schema = input.schema()?;
                let mut fields = Vec::new();
                
                // Add key fields
                for key in keys {
                    if let Some(field) = input_schema.field_with_name(key).ok() {
                        fields.push(field.clone());
                    }
                }
                
                let schema = arrow::datatypes::Schema::new(fields);
                Ok(Arc::new(schema))
            }
            
            LogicalPlan::Window { input, spec: _ } => {
                // Window doesn't change schema immediately
                input.schema()
            }
            
            LogicalPlan::Aggregate { input, spec } => {
                // Aggregate changes schema based on aggregations
                let input_schema = input.schema()?;
                let mut fields = Vec::new();
                
                // Add aggregation result fields
                for (col_name, agg_fn) in &spec.aggregations {
                    let data_type = match agg_fn {
                        AggFunction::Sum | AggFunction::Avg => arrow::datatypes::DataType::Float64,
                        AggFunction::Count => arrow::datatypes::DataType::Int64,
                        AggFunction::Min | AggFunction::Max => arrow::datatypes::DataType::Float64,
                        AggFunction::First | AggFunction::Last => arrow::datatypes::DataType::Utf8,
                    };
                    fields.push(arrow::datatypes::Field::new(col_name, data_type, true));
                }
                
                let schema = arrow::datatypes::Schema::new(fields);
                Ok(Arc::new(schema))
            }
            
            LogicalPlan::Project { input, columns } => {
                // Project selects specific columns
                let input_schema = input.schema()?;
                let mut fields = Vec::new();
                
                for col_name in columns {
                    if let Some(field) = input_schema.field_with_name(col_name).ok() {
                        fields.push(field.clone());
                    }
                }
                
                let schema = arrow::datatypes::Schema::new(fields);
                Ok(Arc::new(schema))
            }
        }
    }
    
    /// Get all children of this plan node
    pub fn children(&self) -> Vec<&LogicalPlan> {
        match self {
            LogicalPlan::Source { .. } => vec![],
            LogicalPlan::Filter { input, .. } => vec![input.as_ref()],
            LogicalPlan::Map { input, .. } => vec![input.as_ref()],
            LogicalPlan::GroupBy { input, .. } => vec![input.as_ref()],
            LogicalPlan::Window { input, .. } => vec![input.as_ref()],
            LogicalPlan::Aggregate { input, .. } => vec![input.as_ref()],
            LogicalPlan::Project { input, .. } => vec![input.as_ref()],
        }
    }
    
    /// Transform this plan node with a function
    pub fn transform<F>(&self, f: F) -> LogicalPlan 
    where 
        F: Fn(&LogicalPlan) -> LogicalPlan + Clone,
    {
        let transformed = match self {
            LogicalPlan::Source { name, attrs } => {
                LogicalPlan::Source {
                    name: name.clone(),
                    attrs: attrs.clone(),
                }
            }
            LogicalPlan::Filter { input, expr } => {
                LogicalPlan::Filter {
                    input: Box::new(input.transform(f.clone())),
                    expr: expr.clone(),
                }
            }
            LogicalPlan::Map { input, expr } => {
                LogicalPlan::Map {
                    input: Box::new(input.transform(f.clone())),
                    expr: expr.clone(),
                }
            }
            LogicalPlan::GroupBy { input, keys } => {
                LogicalPlan::GroupBy {
                    input: Box::new(input.transform(f.clone())),
                    keys: keys.clone(),
                }
            }
            LogicalPlan::Window { input, spec } => {
                LogicalPlan::Window {
                    input: Box::new(input.transform(f.clone())),
                    spec: spec.clone(),
                }
            }
            LogicalPlan::Aggregate { input, spec } => {
                LogicalPlan::Aggregate {
                    input: Box::new(input.transform(f.clone())),
                    spec: spec.clone(),
                }
            }
            LogicalPlan::Project { input, columns } => {
                LogicalPlan::Project {
                    input: Box::new(input.transform(f.clone())),
                    columns: columns.clone(),
                }
            }
        };
        
        f(&transformed)
    }
    
    /// Get a string representation of the plan
    pub fn to_string(&self, indent: usize) -> String {
        let spaces = "  ".repeat(indent);
        match self {
            LogicalPlan::Source { name, attrs } => {
                format!("{}Source(name: {}, attrs: {:?})", spaces, name, attrs)
            }
            LogicalPlan::Filter { input, expr } => {
                format!("{}Filter:\n{}\n{}Expr: {}", 
                    spaces, 
                    input.to_string(indent + 1),
                    spaces,
                    expr.to_string()
                )
            }
            LogicalPlan::Map { input, expr } => {
                format!("{}Map:\n{}\n{}Expr: {}", 
                    spaces, 
                    input.to_string(indent + 1),
                    spaces,
                    expr.to_string()
                )
            }
            LogicalPlan::GroupBy { input, keys } => {
                format!("{}GroupBy(keys: {:?}):\n{}", 
                    spaces, 
                    keys,
                    input.to_string(indent + 1)
                )
            }
            LogicalPlan::Window { input, spec } => {
                format!("{}Window({:?}):\n{}", 
                    spaces, 
                    spec,
                    input.to_string(indent + 1)
                )
            }
            LogicalPlan::Aggregate { input, spec } => {
                format!("{}Aggregate({:?}):\n{}", 
                    spaces, 
                    spec,
                    input.to_string(indent + 1)
                )
            }
            LogicalPlan::Project { input, columns } => {
                format!("{}Project(columns: {:?}):\n{}", 
                    spaces, 
                    columns,
                    input.to_string(indent + 1)
                )
            }
        }
    }
}

/// Map expression type to Arrow data type
fn map_expr_type_to_arrow(expr_type: &ExprType) -> Result<arrow::datatypes::DataType, VectrillError> {
    match expr_type {
        ExprType::Null => Ok(arrow::datatypes::DataType::Null),
        ExprType::Boolean => Ok(arrow::datatypes::DataType::Boolean),
        ExprType::Int8 => Ok(arrow::datatypes::DataType::Int8),
        ExprType::Int16 => Ok(arrow::datatypes::DataType::Int16),
        ExprType::Int32 => Ok(arrow::datatypes::DataType::Int32),
        ExprType::Int64 => Ok(arrow::datatypes::DataType::Int64),
        ExprType::UInt8 => Ok(arrow::datatypes::DataType::UInt8),
        ExprType::UInt16 => Ok(arrow::datatypes::DataType::UInt16),
        ExprType::UInt32 => Ok(arrow::datatypes::DataType::UInt32),
        ExprType::UInt64 => Ok(arrow::datatypes::DataType::UInt64),
        ExprType::Float32 => Ok(arrow::datatypes::DataType::Float32),
        ExprType::Float64 => Ok(arrow::datatypes::DataType::Float64),
        ExprType::Utf8 => Ok(arrow::datatypes::DataType::Utf8),
        ExprType::Timestamp => Ok(arrow::datatypes::DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)),
        ExprType::Date => Ok(arrow::datatypes::DataType::Date32),
        ExprType::Unknown => Ok(arrow::datatypes::DataType::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::{Expr, ScalarValue, Operator};

    #[test]
    fn test_logical_plan_creation() {
        let source = LogicalPlan::Source {
            name: "test".to_string(),
            attrs: HashMap::new(),
        };
        
        let filter_expr = Expr::binary(
            Expr::column("id"),
            Operator::Gt,
            Expr::literal(ScalarValue::Int64(10)),
        );
        
        let filter_plan = LogicalPlan::Filter {
            input: Box::new(source),
            expr: filter_expr,
        };
        
        assert_eq!(filter_plan.to_string(0), "Filter:\n  Source(name: test, attrs: {})\n  Expr: (id > 10)");
    }

    #[test]
    fn test_schema_propagation() {
        let source = LogicalPlan::Source {
            name: "test".to_string(),
            attrs: HashMap::new(),
        };
        
        let schema = source.schema().unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "data");
    }

    #[test]
    fn test_plan_transformation() {
        let source = LogicalPlan::Source {
            name: "test".to_string(),
            attrs: HashMap::new(),
        };
        
        let transformed = source.transform(|plan| {
            match plan {
                LogicalPlan::Source { name, attrs } => {
                    LogicalPlan::Source {
                        name: format!("transformed_{}", name),
                        attrs: attrs.clone(),
                    }
                }
                other => other.clone(),
            }
        });
        
        match transformed {
            LogicalPlan::Source { name, .. } => {
                assert_eq!(name, "transformed_test");
            }
            _ => panic!("Expected Source node"),
        }
    }
}
