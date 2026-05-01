//! Transformation Templates
//! 
//! Pre-built transformation templates for common spreadsheet operations.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::transformations::TransformationPipeline;
use crate::transformations::Transformation;
use crate::transformations::builtin::{FilterOperator, FilterValue, MapOperation};
use crate::{error::Result, VectrillError};

/// Transformation template for common operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformationTemplate {
    /// Template name
    pub name: String,
    /// Template description
    pub description: String,
    /// Template category
    pub category: String,
    /// Required columns
    pub required_columns: Vec<String>,
    /// Optional columns
    pub optional_columns: Vec<String>,
    /// Transformation steps
    pub steps: Vec<TemplateStep>,
    /// Parameters for the template
    pub parameters: HashMap<String, TemplateParameter>,
}

/// Individual step in a transformation template
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateStep {
    /// Step name
    pub name: String,
    /// Step description
    pub description: String,
    /// Transformation type
    pub transform_type: String,
    /// Step parameters
    pub parameters: HashMap<String, String>,
    /// Whether this step is required
    pub required: bool,
}

/// Template parameter definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateParameter {
    /// Parameter name
    pub name: String,
    /// Parameter description
    pub description: String,
    /// Parameter type
    pub param_type: TemplateParameterType,
    /// Default value
    pub default_value: Option<String>,
    /// Whether parameter is required
    pub required: bool,
    /// Validation rules
    pub validation: Option<String>,
}

/// Template parameter types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TemplateParameterType {
    String,
    Number,
    Boolean,
    Column,
    ColumnList,
    Operator,
    Function,
}

/// Template manager for handling transformation templates
pub struct TemplateManager {
    templates: HashMap<String, TransformationTemplate>,
}

impl TemplateManager {
    /// Create new template manager
    pub fn new() -> Self {
        let mut manager = Self {
            templates: HashMap::new(),
        };
        manager.load_default_templates();
        manager
    }
    
    /// Load default templates
    fn load_default_templates(&mut self) {
        // Data cleaning templates
        self.add_template(self.create_remove_duplicates_template());
        self.add_template(self.create_fill_missing_values_template());
        self.add_template(self.create_standardize_text_template());
        
        // Data analysis templates
        self.add_template(self.create_summary_statistics_template());
        self.add_template(self.create_pivot_table_template());
        self.add_template(self.create_time_series_analysis_template());
        
        // Data filtering templates
        self.add_template(self.create_filter_by_condition_template());
        self.add_template(self.create_top_n_records_template());
        self.add_template(self.create_outlier_removal_template());
        
        // Data transformation templates
        self.add_template(self.create_normalize_data_template());
        self.add_template(self.create_calculated_columns_template());
        self.add_template(self.create_data_aggregation_template());
    }
    
    /// Add a template to the manager
    pub fn add_template(&mut self, template: TransformationTemplate) {
        self.templates.insert(template.name.clone(), template);
    }
    
    /// Get a template by name
    pub fn get_template(&self, name: &str) -> Option<&TransformationTemplate> {
        self.templates.get(name)
    }
    
    /// List all templates
    pub fn list_templates(&self) -> Vec<&TransformationTemplate> {
        self.templates.values().collect()
    }
    
    /// List templates by category
    pub fn list_templates_by_category(&self, category: &str) -> Vec<&TransformationTemplate> {
        self.templates
            .values()
            .filter(|t| t.category == category)
            .collect()
    }
    
    /// Create transformation pipeline from template
    pub fn create_pipeline_from_template(&self, template_name: &str, parameters: &HashMap<String, String>) -> Result<TransformationPipeline> {
        let template = self.get_template(template_name)
            .ok_or_else(|| VectrillError::InvalidConfig(format!("Template '{}' not found", template_name)))?;
        
        let mut pipeline = TransformationPipeline::new(format!("template_{}", template_name));
        
        for step in &template.steps {
            let transformation = self.create_transformation_from_step(step, parameters)?;
            pipeline = pipeline.add_transform(transformation);
        }
        
        Ok(pipeline)
    }
    
    /// Create transformation from template step
    fn create_transformation_from_step(&self, step: &TemplateStep, parameters: &HashMap<String, String>) -> Result<Box<dyn Transformation>> {
        match step.transform_type.as_str() {
            "filter" => self.create_filter_transform(step, parameters),
            "map" => self.create_map_transform(step, parameters),
            _ => Err(VectrillError::InvalidConfig(format!("Unsupported transformation type: {}", step.transform_type))),
        }
    }
    
    /// Create filter transformation from step
    fn create_filter_transform(&self, step: &TemplateStep, parameters: &HashMap<String, String>) -> Result<Box<dyn Transformation>> {
        let column = parameters.get("column")
            .or_else(|| step.parameters.get("column"))
            .ok_or_else(|| VectrillError::InvalidConfig("Column parameter required for filter".to_string()))?;
        
        let default_operator = "equals".to_string();
        let operator_str = parameters.get("operator")
            .or_else(|| step.parameters.get("operator"))
            .unwrap_or(&default_operator);
        
        let operator = match operator_str.as_str() {
            "equals" => FilterOperator::Equals,
            "not_equals" => FilterOperator::NotEquals,
            "greater_than" => FilterOperator::GreaterThan,
            "less_than" => FilterOperator::LessThan,
            "greater_than_or_equal" => FilterOperator::GreaterThanOrEqual,
            "less_than_or_equal" => FilterOperator::LessThanOrEqual,
            "contains" => FilterOperator::Contains,
            _ => return Err(VectrillError::InvalidConfig(format!("Unsupported operator: {}", operator_str))),
        };
        
        let value_str = parameters.get("value")
            .or_else(|| step.parameters.get("value"))
            .ok_or_else(|| VectrillError::InvalidConfig("Value parameter required for filter".to_string()))?;
        
        let value = if let Ok(num) = value_str.parse::<f64>() {
            FilterValue::Float64(num)
        } else if let Ok(num) = value_str.parse::<i64>() {
            FilterValue::Int64(num)
        } else if let Ok(bool_val) = value_str.parse::<bool>() {
            FilterValue::Boolean(bool_val)
        } else {
            FilterValue::String(value_str.clone())
        };
        
        // TODO: Create actual filter transformation
        // This is a placeholder - would need to create a real transformation
        Err(VectrillError::NotImplemented("Filter transformation creation not yet implemented".to_string()))
    }
    
    /// Create map transformation from step
    fn create_map_transform(&self, step: &TemplateStep, parameters: &HashMap<String, String>) -> Result<Box<dyn Transformation>> {
        let column = parameters.get("column")
            .or_else(|| step.parameters.get("column"))
            .ok_or_else(|| VectrillError::InvalidConfig("Column parameter required for map".to_string()))?;
        
        let default_function = "identity".to_string();
        let function_str = parameters.get("function")
            .or_else(|| step.parameters.get("function"))
            .unwrap_or(&default_function);
        
        let operation = match function_str.as_str() {
            "add" => {
                let value = parameters.get("value")
                    .or_else(|| step.parameters.get("value"))
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or(0.0);
                MapOperation::Add(value)
            }
            "multiply" => {
                let value = parameters.get("value")
                    .or_else(|| step.parameters.get("value"))
                    .and_then(|v| v.parse::<f64>().ok())
                    .unwrap_or(1.0);
                MapOperation::Multiply(value)
            }
            "uppercase" => MapOperation::UpperCase,
            "lowercase" => MapOperation::LowerCase,
            "abs" => MapOperation::Abs,
            _ => return Err(VectrillError::InvalidConfig(format!("Unsupported function: {}", function_str))),
        };
        
        // TODO: Create actual map transformation
        Err(VectrillError::NotImplemented("Map transformation creation not yet implemented".to_string()))
    }
    
    /// Template: Remove Duplicates
    fn create_remove_duplicates_template(&self) -> TransformationTemplate {
        TransformationTemplate {
            name: "remove_duplicates".to_string(),
            description: "Remove duplicate rows from the dataset".to_string(),
            category: "Data Cleaning".to_string(),
            required_columns: vec![],
            optional_columns: vec![],
            steps: vec![
                TemplateStep {
                    name: "deduplicate".to_string(),
                    description: "Remove duplicate rows".to_string(),
                    transform_type: "deduplicate".to_string(),
                    parameters: HashMap::new(),
                    required: true,
                },
            ],
            parameters: HashMap::new(),
        }
    }
    
    /// Template: Fill Missing Values
    fn create_fill_missing_values_template(&self) -> TransformationTemplate {
        let mut parameters = HashMap::new();
        parameters.insert("strategy".to_string(), TemplateParameter {
            name: "strategy".to_string(),
            description: "Strategy for filling missing values".to_string(),
            param_type: TemplateParameterType::String,
            default_value: Some("mean".to_string()),
            required: false,
            validation: Some("mean|median|mode|forward|backward|constant".to_string()),
        });
        
        TransformationTemplate {
            name: "fill_missing_values".to_string(),
            description: "Fill missing values in the dataset".to_string(),
            category: "Data Cleaning".to_string(),
            required_columns: vec![],
            optional_columns: vec![],
            steps: vec![
                TemplateStep {
                    name: "fill_missing".to_string(),
                    description: "Fill missing values".to_string(),
                    transform_type: "fill_missing".to_string(),
                    parameters: HashMap::new(),
                    required: true,
                },
            ],
            parameters,
        }
    }
    
    /// Template: Standardize Text
    fn create_standardize_text_template(&self) -> TransformationTemplate {
        let mut parameters = HashMap::new();
        parameters.insert("columns".to_string(), TemplateParameter {
            name: "columns".to_string(),
            description: "Columns to standardize".to_string(),
            param_type: TemplateParameterType::ColumnList,
            default_value: None,
            required: true,
            validation: None,
        });
        
        parameters.insert("operations".to_string(), TemplateParameter {
            name: "operations".to_string(),
            description: "Text operations to apply".to_string(),
            param_type: TemplateParameterType::String,
            default_value: Some("trim,lowercase".to_string()),
            required: false,
            validation: Some("trim|lowercase|uppercase|remove_punctuation|normalize_spaces".to_string()),
        });
        
        TransformationTemplate {
            name: "standardize_text".to_string(),
            description: "Standardize text columns by trimming, case conversion, etc.".to_string(),
            category: "Data Cleaning".to_string(),
            required_columns: vec![],
            optional_columns: vec![],
            steps: vec![
                TemplateStep {
                    name: "standardize".to_string(),
                    description: "Standardize text format".to_string(),
                    transform_type: "map".to_string(),
                    parameters: HashMap::new(),
                    required: true,
                },
            ],
            parameters,
        }
    }
    
    /// Template: Summary Statistics
    fn create_summary_statistics_template(&self) -> TransformationTemplate {
        let mut parameters = HashMap::new();
        parameters.insert("group_by".to_string(), TemplateParameter {
            name: "group_by".to_string(),
            description: "Columns to group by".to_string(),
            param_type: TemplateParameterType::ColumnList,
            default_value: None,
            required: false,
            validation: None,
        });
        
        TransformationTemplate {
            name: "summary_statistics".to_string(),
            description: "Generate summary statistics for the dataset".to_string(),
            category: "Data Analysis".to_string(),
            required_columns: vec![],
            optional_columns: vec![],
            steps: vec![
                TemplateStep {
                    name: "aggregate".to_string(),
                    description: "Calculate summary statistics".to_string(),
                    transform_type: "aggregate".to_string(),
                    parameters: HashMap::new(),
                    required: true,
                },
            ],
            parameters,
        }
    }
    
    /// Template: Pivot Table
    fn create_pivot_table_template(&self) -> TransformationTemplate {
        let mut parameters = HashMap::new();
        parameters.insert("index".to_string(), TemplateParameter {
            name: "index".to_string(),
            description: "Columns to use as index".to_string(),
            param_type: TemplateParameterType::ColumnList,
            default_value: None,
            required: true,
            validation: None,
        });
        
        parameters.insert("columns".to_string(), TemplateParameter {
            name: "columns".to_string(),
            description: "Columns to use as columns".to_string(),
            param_type: TemplateParameterType::ColumnList,
            default_value: None,
            required: false,
            validation: None,
        });
        
        parameters.insert("values".to_string(), TemplateParameter {
            name: "values".to_string(),
            description: "Columns to aggregate".to_string(),
            param_type: TemplateParameterType::ColumnList,
            default_value: None,
            required: true,
            validation: None,
        });
        
        parameters.insert("agg_function".to_string(), TemplateParameter {
            name: "agg_function".to_string(),
            description: "Aggregation function".to_string(),
            param_type: TemplateParameterType::String,
            default_value: Some("sum".to_string()),
            required: false,
            validation: Some("sum|mean|count|min|max".to_string()),
        });
        
        TransformationTemplate {
            name: "pivot_table".to_string(),
            description: "Create a pivot table from the data".to_string(),
            category: "Data Analysis".to_string(),
            required_columns: vec![],
            optional_columns: vec![],
            steps: vec![
                TemplateStep {
                    name: "pivot".to_string(),
                    description: "Create pivot table".to_string(),
                    transform_type: "pivot".to_string(),
                    parameters: HashMap::new(),
                    required: true,
                },
            ],
            parameters,
        }
    }
    
    /// Template: Time Series Analysis
    fn create_time_series_analysis_template(&self) -> TransformationTemplate {
        let mut parameters = HashMap::new();
        parameters.insert("time_column".to_string(), TemplateParameter {
            name: "time_column".to_string(),
            description: "Column containing time values".to_string(),
            param_type: TemplateParameterType::Column,
            default_value: None,
            required: true,
            validation: None,
        });
        
        parameters.insert("value_column".to_string(), TemplateParameter {
            name: "value_column".to_string(),
            description: "Column containing values to analyze".to_string(),
            param_type: TemplateParameterType::Column,
            default_value: None,
            required: true,
            validation: None,
        });
        
        parameters.insert("window_size".to_string(), TemplateParameter {
            name: "window_size".to_string(),
            description: "Window size for moving averages".to_string(),
            param_type: TemplateParameterType::Number,
            default_value: Some("7".to_string()),
            required: false,
            validation: Some(r"^\d+$".to_string()),
        });
        
        TransformationTemplate {
            name: "time_series_analysis".to_string(),
            description: "Perform time series analysis with moving averages and trends".to_string(),
            category: "Data Analysis".to_string(),
            required_columns: vec![],
            optional_columns: vec![],
            steps: vec![
                TemplateStep {
                    name: "moving_average".to_string(),
                    description: "Calculate moving average".to_string(),
                    transform_type: "window".to_string(),
                    parameters: HashMap::new(),
                    required: true,
                },
                TemplateStep {
                    name: "trend".to_string(),
                    description: "Calculate trend".to_string(),
                    transform_type: "map".to_string(),
                    parameters: HashMap::new(),
                    required: true,
                },
            ],
            parameters,
        }
    }
    
    /// Template: Filter by Condition
    fn create_filter_by_condition_template(&self) -> TransformationTemplate {
        let mut parameters = HashMap::new();
        parameters.insert("column".to_string(), TemplateParameter {
            name: "column".to_string(),
            description: "Column to filter on".to_string(),
            param_type: TemplateParameterType::Column,
            default_value: None,
            required: true,
            validation: None,
        });
        
        parameters.insert("operator".to_string(), TemplateParameter {
            name: "operator".to_string(),
            description: "Comparison operator".to_string(),
            param_type: TemplateParameterType::Operator,
            default_value: Some("equals".to_string()),
            required: true,
            validation: None,
        });
        
        parameters.insert("value".to_string(), TemplateParameter {
            name: "value".to_string(),
            description: "Value to compare against".to_string(),
            param_type: TemplateParameterType::String,
            default_value: None,
            required: true,
            validation: None,
        });
        
        TransformationTemplate {
            name: "filter_by_condition".to_string(),
            description: "Filter rows based on a condition".to_string(),
            category: "Data Filtering".to_string(),
            required_columns: vec![],
            optional_columns: vec![],
            steps: vec![
                TemplateStep {
                    name: "filter".to_string(),
                    description: "Apply filter condition".to_string(),
                    transform_type: "filter".to_string(),
                    parameters: HashMap::new(),
                    required: true,
                },
            ],
            parameters,
        }
    }
    
    /// Template: Top N Records
    fn create_top_n_records_template(&self) -> TransformationTemplate {
        let mut parameters = HashMap::new();
        parameters.insert("column".to_string(), TemplateParameter {
            name: "column".to_string(),
            description: "Column to sort by".to_string(),
            param_type: TemplateParameterType::Column,
            default_value: None,
            required: true,
            validation: None,
        });
        
        parameters.insert("n".to_string(), TemplateParameter {
            name: "n".to_string(),
            description: "Number of records to return".to_string(),
            param_type: TemplateParameterType::Number,
            default_value: Some("10".to_string()),
            required: false,
            validation: Some(r"^\d+$".to_string()),
        });
        
        parameters.insert("ascending".to_string(), TemplateParameter {
            name: "ascending".to_string(),
            description: "Whether to sort ascending".to_string(),
            param_type: TemplateParameterType::Boolean,
            default_value: Some("false".to_string()),
            required: false,
            validation: None,
        });
        
        TransformationTemplate {
            name: "top_n_records".to_string(),
            description: "Get the top N records based on a column".to_string(),
            category: "Data Filtering".to_string(),
            required_columns: vec![],
            optional_columns: vec![],
            steps: vec![
                TemplateStep {
                    name: "sort".to_string(),
                    description: "Sort by column".to_string(),
                    transform_type: "sort".to_string(),
                    parameters: HashMap::new(),
                    required: true,
                },
                TemplateStep {
                    name: "limit".to_string(),
                    description: "Limit to N records".to_string(),
                    transform_type: "limit".to_string(),
                    parameters: HashMap::new(),
                    required: true,
                },
            ],
            parameters,
        }
    }
    
    /// Template: Outlier Removal
    fn create_outlier_removal_template(&self) -> TransformationTemplate {
        let mut parameters = HashMap::new();
        parameters.insert("column".to_string(), TemplateParameter {
            name: "column".to_string(),
            description: "Column to check for outliers".to_string(),
            param_type: TemplateParameterType::Column,
            default_value: None,
            required: true,
            validation: None,
        });
        
        parameters.insert("method".to_string(), TemplateParameter {
            name: "method".to_string(),
            description: "Outlier detection method".to_string(),
            param_type: TemplateParameterType::String,
            default_value: Some("iqr".to_string()),
            required: false,
            validation: Some("iqr|zscore|isolation_forest".to_string()),
        });
        
        parameters.insert("threshold".to_string(), TemplateParameter {
            name: "threshold".to_string(),
            description: "Outlier threshold".to_string(),
            param_type: TemplateParameterType::Number,
            default_value: Some("1.5".to_string()),
            required: false,
            validation: None,
        });
        
        TransformationTemplate {
            name: "outlier_removal".to_string(),
            description: "Remove outliers from the dataset".to_string(),
            category: "Data Filtering".to_string(),
            required_columns: vec![],
            optional_columns: vec![],
            steps: vec![
                TemplateStep {
                    name: "detect_outliers".to_string(),
                    description: "Detect outliers".to_string(),
                    transform_type: "outlier_detection".to_string(),
                    parameters: HashMap::new(),
                    required: true,
                },
                TemplateStep {
                    name: "filter_outliers".to_string(),
                    description: "Filter out outliers".to_string(),
                    transform_type: "filter".to_string(),
                    parameters: HashMap::new(),
                    required: true,
                },
            ],
            parameters,
        }
    }
    
    /// Template: Normalize Data
    fn create_normalize_data_template(&self) -> TransformationTemplate {
        let mut parameters = HashMap::new();
        parameters.insert("columns".to_string(), TemplateParameter {
            name: "columns".to_string(),
            description: "Columns to normalize".to_string(),
            param_type: TemplateParameterType::ColumnList,
            default_value: None,
            required: true,
            validation: None,
        });
        
        parameters.insert("method".to_string(), TemplateParameter {
            name: "method".to_string(),
            description: "Normalization method".to_string(),
            param_type: TemplateParameterType::String,
            default_value: Some("min_max".to_string()),
            required: false,
            validation: Some("min_max|z_score|robust".to_string()),
        });
        
        TransformationTemplate {
            name: "normalize_data".to_string(),
            description: "Normalize numeric columns".to_string(),
            category: "Data Transformation".to_string(),
            required_columns: vec![],
            optional_columns: vec![],
            steps: vec![
                TemplateStep {
                    name: "normalize".to_string(),
                    description: "Apply normalization".to_string(),
                    transform_type: "map".to_string(),
                    parameters: HashMap::new(),
                    required: true,
                },
            ],
            parameters,
        }
    }
    
    /// Template: Calculated Columns
    fn create_calculated_columns_template(&self) -> TransformationTemplate {
        let mut parameters = HashMap::new();
        parameters.insert("expressions".to_string(), TemplateParameter {
            name: "expressions".to_string(),
            description: "Expressions for calculated columns".to_string(),
            param_type: TemplateParameterType::String,
            default_value: None,
            required: true,
            validation: None,
        });
        
        TransformationTemplate {
            name: "calculated_columns".to_string(),
            description: "Add calculated columns based on expressions".to_string(),
            category: "Data Transformation".to_string(),
            required_columns: vec![],
            optional_columns: vec![],
            steps: vec![
                TemplateStep {
                    name: "calculate".to_string(),
                    description: "Calculate new columns".to_string(),
                    transform_type: "calculate".to_string(),
                    parameters: HashMap::new(),
                    required: true,
                },
            ],
            parameters,
        }
    }
    
    /// Template: Data Aggregation
    fn create_data_aggregation_template(&self) -> TransformationTemplate {
        let mut parameters = HashMap::new();
        parameters.insert("group_by".to_string(), TemplateParameter {
            name: "group_by".to_string(),
            description: "Columns to group by".to_string(),
            param_type: TemplateParameterType::ColumnList,
            default_value: None,
            required: false,
            validation: None,
        });
        
        parameters.insert("aggregations".to_string(), TemplateParameter {
            name: "aggregations".to_string(),
            description: "Aggregation expressions".to_string(),
            param_type: TemplateParameterType::String,
            default_value: None,
            required: true,
            validation: None,
        });
        
        TransformationTemplate {
            name: "data_aggregation".to_string(),
            description: "Aggregate data by groups".to_string(),
            category: "Data Transformation".to_string(),
            required_columns: vec![],
            optional_columns: vec![],
            steps: vec![
                TemplateStep {
                    name: "aggregate".to_string(),
                    description: "Aggregate data".to_string(),
                    transform_type: "aggregate".to_string(),
                    parameters: HashMap::new(),
                    required: true,
                },
            ],
            parameters,
        }
    }
}

impl Default for TemplateManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_template_manager_creation() {
        let manager = TemplateManager::new();
        assert!(!manager.templates.is_empty());
    }
    
    #[test]
    fn test_template_listing() {
        let manager = TemplateManager::new();
        let templates = manager.list_templates();
        assert!(!templates.is_empty());
        
        let cleaning_templates = manager.list_templates_by_category("Data Cleaning");
        assert!(!cleaning_templates.is_empty());
    }
    
    #[test]
    fn test_template_retrieval() {
        let manager = TemplateManager::new();
        let template = manager.get_template("remove_duplicates");
        assert!(template.is_some());
        assert_eq!(template.unwrap().category, "Data Cleaning");
    }
}
