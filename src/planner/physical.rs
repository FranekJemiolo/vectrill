//! Physical Plan IR

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use crate::expression::PhysicalExpr;
use crate::planner::logical::{AggFunction, WindowSpec, WindowType};

/// Physical plan representation
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    /// Scan data from source
    ScanSource {
        name: String,
        attrs: HashMap<String, String>,
    },
    
    /// Filter records using compiled expression
    Filter {
        input: Box<PhysicalPlan>,
        expr: Arc<dyn PhysicalExpr>,
    },
    
    /// Map/transform records using compiled expression
    Map {
        input: Box<PhysicalPlan>,
        expr: Arc<dyn PhysicalExpr>,
    },
    
    /// Hash-based aggregation
    HashAggregate {
        input: Box<PhysicalPlan>,
        keys: Vec<String>,
        aggregations: Vec<PhysicalAggregation>,
    },
    
    /// Windowed aggregation
    WindowedAggregate {
        input: Box<PhysicalPlan>,
        window: WindowSpec,
        aggregations: Vec<PhysicalAggregation>,
    },
    
    /// Project/select columns
    Project {
        input: Box<PhysicalPlan>,
        columns: Vec<usize>, // Column indices
    },
    
    /// Sort operation
    Sort {
        input: Box<PhysicalPlan>,
        sort_keys: Vec<SortKey>,
    },
    
    /// Limit operation
    Limit {
        input: Box<PhysicalPlan>,
        limit: usize,
        offset: Option<usize>,
    },
}

/// Physical aggregation specification
#[derive(Debug, Clone)]
pub struct PhysicalAggregation {
    pub column: String,
    pub function: AggFunction,
    pub alias: Option<String>,
}

/// Sort key specification
#[derive(Debug, Clone)]
pub struct SortKey {
    pub column: String,
    pub ascending: bool,
    pub nulls_first: bool,
}

impl PhysicalPlan {
    /// Get the schema for this physical plan
    pub fn schema(&self) -> arrow::datatypes::SchemaRef {
        match self {
            PhysicalPlan::ScanSource { name: _, attrs: _ } => {
                // Default schema for sources
                let schema = arrow::datatypes::Schema::new(vec![
                    arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, false),
                    arrow::datatypes::Field::new("data", arrow::datatypes::DataType::Utf8, true),
                ]);
                Arc::new(schema)
            }
            
            PhysicalPlan::Filter { input, expr: _ } => {
                input.schema()
            }
            
            PhysicalPlan::Map { input, expr } => {
                let input_schema = input.schema();
                let mut fields = input_schema.fields().to_vec();
                
                // Add field for map expression result
                let data_type = arrow::datatypes::DataType::Utf8; // Simplified for now
                fields.push(Arc::new(arrow::datatypes::Field::new("computed", data_type, true)));
                
                let schema = arrow::datatypes::Schema::new(fields);
                Arc::new(schema)
            }
            
            PhysicalPlan::HashAggregate { input, keys, aggregations } => {
                let input_schema = input.schema();
                let mut fields: Vec<arrow::datatypes::FieldRef> = Vec::new();
                
                // Add key fields
                for key in keys {
                    if let Ok(field) = input_schema.field_with_name(key) {
                        fields.push(field.clone().into());
                    }
                }
                
                // Add aggregation fields
                for agg in aggregations {
                    let field_name = agg.alias.as_ref().unwrap_or(&agg.column);
                    let data_type = match agg.function {
                        AggFunction::Sum | AggFunction::Avg => arrow::datatypes::DataType::Float64,
                        AggFunction::Count => arrow::datatypes::DataType::Int64,
                        AggFunction::Min | AggFunction::Max => arrow::datatypes::DataType::Float64,
                        AggFunction::First | AggFunction::Last => arrow::datatypes::DataType::Utf8,
                    };
                    fields.push(Arc::new(arrow::datatypes::Field::new(field_name, data_type, true)));
                }
                
                let schema = arrow::datatypes::Schema::new(fields);
                Arc::new(schema)
            }
            
            PhysicalPlan::WindowedAggregate { input, window: _, aggregations } => {
                let input_schema = input.schema();
                let mut fields = input_schema.fields().to_vec();
                
                // Add aggregation fields
                for agg in aggregations {
                    let field_name = agg.alias.as_ref().unwrap_or(&agg.column);
                    let data_type = match agg.function {
                        AggFunction::Sum | AggFunction::Avg => arrow::datatypes::DataType::Float64,
                        AggFunction::Count => arrow::datatypes::DataType::Int64,
                        AggFunction::Min | AggFunction::Max => arrow::datatypes::DataType::Float64,
                        AggFunction::First | AggFunction::Last => arrow::datatypes::DataType::Utf8,
                    };
                    fields.push(Arc::new(arrow::datatypes::Field::new(field_name, data_type, true)));
                }
                
                let schema = arrow::datatypes::Schema::new(fields);
                Arc::new(schema)
            }
            
            PhysicalPlan::Project { input, columns } => {
                let input_schema = input.schema();
                let mut fields = Vec::new();
                
                for &col_idx in columns {
                    if col_idx < input_schema.fields().len() {
                        fields.push(input_schema.field(col_idx).clone());
                    }
                }
                
                let schema = arrow::datatypes::Schema::new(fields);
                Arc::new(schema)
            }
            
            PhysicalPlan::Sort { input, sort_keys: _ } => {
                input.schema()
            }
            
            PhysicalPlan::Limit { input, limit: _, offset: _ } => {
                input.schema()
            }
        }
    }
    
    /// Get all children of this physical plan
    pub fn children(&self) -> Vec<&PhysicalPlan> {
        match self {
            PhysicalPlan::ScanSource { .. } => vec![],
            PhysicalPlan::Filter { input, .. } => vec![input.as_ref()],
            PhysicalPlan::Map { input, .. } => vec![input.as_ref()],
            PhysicalPlan::HashAggregate { input, .. } => vec![input.as_ref()],
            PhysicalPlan::WindowedAggregate { input, .. } => vec![input.as_ref()],
            PhysicalPlan::Project { input, .. } => vec![input.as_ref()],
            PhysicalPlan::Sort { input, .. } => vec![input.as_ref()],
            PhysicalPlan::Limit { input, .. } => vec![input.as_ref()],
        }
    }
    
    /// Get a string representation of the plan
    pub fn to_string(&self, indent: usize) -> String {
        let spaces = "  ".repeat(indent);
        match self {
            PhysicalPlan::ScanSource { name, attrs } => {
                format!("{}ScanSource(name: {}, attrs: {:?})", spaces, name, attrs)
            }
            PhysicalPlan::Filter { input, expr } => {
                format!("{}Filter:\n{}\n{}Expr: {}", 
                    spaces, 
                    input.to_string(indent + 1),
                    spaces,
                    expr.as_string()
                )
            }
            PhysicalPlan::Map { input, expr } => {
                format!("{}Map:\n{}\n{}Expr: {}", 
                    spaces, 
                    input.to_string(indent + 1),
                    spaces,
                    expr.as_string()
                )
            }
            PhysicalPlan::HashAggregate { input, keys, aggregations } => {
                let agg_strs: Vec<String> = aggregations.iter()
                    .map(|agg| format!("{}({})", agg.function, agg.column))
                    .collect();
                format!("{}HashAggregate(keys: {:?}, aggs: [{}]):\n{}", 
                    spaces, 
                    keys,
                    agg_strs.join(", "),
                    input.to_string(indent + 1)
                )
            }
            PhysicalPlan::WindowedAggregate { input, window, aggregations } => {
                let agg_strs: Vec<String> = aggregations.iter()
                    .map(|agg| format!("{}({})", agg.function, agg.column))
                    .collect();
                format!("{}WindowedAggregate({:?}, aggs: [{}]):\n{}", 
                    spaces, 
                    window,
                    agg_strs.join(", "),
                    input.to_string(indent + 1)
                )
            }
            PhysicalPlan::Project { input, columns } => {
                format!("{}Project(columns: {:?}):\n{}", 
                    spaces, 
                    columns,
                    input.to_string(indent + 1)
                )
            }
            PhysicalPlan::Sort { input, sort_keys } => {
                let key_strs: Vec<String> = sort_keys.iter()
                    .map(|key| format!("{}{}{}", key.column, if key.ascending { " ASC" } else { " DESC" }, if key.nulls_first { " NULLS FIRST" } else { " NULLS LAST" }))
                    .collect();
                format!("{}Sort([{}]):\n{}", 
                    spaces, 
                    key_strs.join(", "),
                    input.to_string(indent + 1)
                )
            }
            PhysicalPlan::Limit { input, limit, offset } => {
                format!("{}Limit(limit: {}, offset: {:?}):\n{}", 
                    spaces, 
                    limit,
                    offset,
                    input.to_string(indent + 1)
                )
            }
        }
    }
}

/// Physical aggregation function implementations
impl std::fmt::Display for AggFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AggFunction::Sum => write!(f, "sum"),
            AggFunction::Avg => write!(f, "avg"),
            AggFunction::Count => write!(f, "count"),
            AggFunction::Min => write!(f, "min"),
            AggFunction::Max => write!(f, "max"),
            AggFunction::First => write!(f, "first"),
            AggFunction::Last => write!(f, "last"),
        }
    }
}

/// Physical window specification implementations
impl std::fmt::Display for WindowType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WindowType::Tumbling => write!(f, "tumbling"),
            WindowType::Sliding => write!(f, "sliding"),
            WindowType::Session => write!(f, "session"),
        }
    }
}

impl std::fmt::Display for WindowSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WindowSpec { window_type, duration, slide, timeout } => {
                write!(f, "{}({}", window_type, duration)?;
                if let Some(slide) = slide {
                    write!(f, ", {}", slide)?;
                }
                if let Some(timeout) = timeout {
                    write!(f, ", {}", timeout)?;
                }
                write!(f, ")")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::{LiteralExpr, create_physical_expr};
    use crate::planner::logical::LogicalPlan;
    use std::collections::HashMap;

    #[test]
    fn test_physical_plan_creation() {
        let source = PhysicalPlan::ScanSource {
            name: "test".to_string(),
            attrs: HashMap::new(),
        };
        
        let schema = source.schema();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(schema.field(1).name(), "data");
    }
    
    #[test]
    fn test_physical_aggregation() {
        let agg = PhysicalAggregation {
            column: "value".to_string(),
            function: AggFunction::Sum,
            alias: Some("total".to_string()),
        };
        
        assert_eq!(agg.function.to_string(), "sum");
        assert_eq!(agg.alias.unwrap(), "total");
    }
    
    #[test]
    fn test_sort_key() {
        let sort_key = SortKey {
            column: "timestamp".to_string(),
            ascending: false,
            nulls_first: true,
        };
        
        assert_eq!(sort_key.column, "timestamp");
        assert!(!sort_key.ascending);
        assert!(sort_key.nulls_first);
    }
    
    #[test]
    fn test_window_spec_display() {
        let tumbling_window = WindowSpec {
            window_type: WindowType::Tumbling,
            duration: "10s".to_string(),
            slide: None,
            timeout: None,
        };
        
        assert_eq!(tumbling_window.to_string(), "tumbling(10s)");
        
        let sliding_window = WindowSpec {
            window_type: WindowType::Sliding,
            duration: "1m".to_string(),
            slide: Some("30s".to_string()),
            timeout: None,
        };
        
        assert_eq!(sliding_window.to_string(), "sliding(1m, 30s)");
    }
    
    #[test]
    fn test_physical_plan_to_string() {
        let source = PhysicalPlan::ScanSource {
            name: "test".to_string(),
            attrs: HashMap::new(),
        };
        
        let plan_str = source.to_string(0);
        assert!(plan_str.contains("ScanSource"));
        assert!(plan_str.contains("test"));
    }
}
