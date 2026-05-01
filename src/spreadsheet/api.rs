//! Spreadsheet API
//! 
//! Provides a simplified API for spreadsheet applications to interact with Vectrill.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::{error::Result, VectrillError};
use crate::transformations::TransformationPipeline;
use crate::transformations::builtin::{
    FilterOperator, FilterValue, MapOperation
};

/// Request from spreadsheet application
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpreadsheetRequest {
    /// Unique request identifier
    pub request_id: String,
    /// Type of operation
    pub operation: OperationType,
    /// Data from spreadsheet
    pub data: SpreadsheetData,
    /// Transformation configuration
    pub transformation: Option<TransformationConfig>,
    /// Output configuration
    pub output: OutputConfig,
}

/// Response to spreadsheet application
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpreadsheetResponse {
    /// Corresponding request ID
    pub request_id: String,
    /// Success status
    pub success: bool,
    /// Transformed data
    pub data: Option<SpreadsheetData>,
    /// Error message if failed
    pub error: Option<String>,
    /// Metadata about the transformation
    pub metadata: Option<TransformationMetadata>,
}

/// Operation types supported by the spreadsheet API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OperationType {
    /// Transform data using Vectrill
    Transform,
    /// Get available transformations
    GetTransformations,
    /// Validate transformation configuration
    Validate,
    /// Get transformation templates
    GetTemplates,
    /// Preview transformation results
    Preview,
}

/// Data from spreadsheet in a format that's easy to work with
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpreadsheetData {
    /// Column headers
    pub headers: Vec<String>,
    /// Data rows (2D array: rows × columns)
    pub rows: Vec<Vec<CellValue>>,
    /// Data types for each column
    pub column_types: Vec<DataType>,
    /// Range reference (e.g., "A1:C100")
    pub range: Option<String>,
    /// Sheet name
    pub sheet_name: Option<String>,
}

/// Cell value that can hold different types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum CellValue {
    String(String),
    Number(f64),
    Boolean(bool),
    Empty,
    Error(String),
}

/// Data type for spreadsheet columns
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum DataType {
    String,
    Number,
    Boolean,
    Date,
    Empty,
}

/// Transformation configuration for spreadsheet users
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformationConfig {
    /// Type of transformation
    pub transform_type: TransformType,
    /// Column to apply transformation to
    pub column: String,
    /// Parameters for the transformation
    pub parameters: HashMap<String, TransformParameter>,
    /// Output column name
    pub output_column: Option<String>,
}

/// Types of transformations available in spreadsheet interface
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransformType {
    Filter,
    Map,
    Aggregate,
    Sort,
    GroupBy,
    Join,
    Custom(String),
}

/// Parameter values for transformations
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TransformParameter {
    String(String),
    Number(f64),
    Boolean(bool),
    Array(Vec<String>),
}

/// Output configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OutputConfig {
    /// Output format
    pub format: OutputFormat,
    /// Whether to include headers
    pub include_headers: bool,
    /// Maximum number of rows to return
    pub max_rows: Option<usize>,
}

/// Output format for spreadsheet data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OutputFormat {
    /// Same format as input
    Same,
    /// Transpose rows and columns
    Transpose,
    /// Summary statistics
    Summary,
    /// Pivoted data
    Pivot,
}

/// Metadata about the transformation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformationMetadata {
    /// Number of input rows
    pub input_rows: usize,
    /// Number of output rows
    pub output_rows: usize,
    /// Number of columns
    pub columns: usize,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
    /// Transformation steps applied
    pub steps: Vec<String>,
}

/// Main Spreadsheet API
pub struct SpreadsheetAPI {
    templates: HashMap<String, TransformationTemplate>,
}

impl SpreadsheetAPI {
    /// Create new Spreadsheet API instance
    pub fn new() -> Self {
        Self {
            templates: HashMap::new(),
        }
    }
    
    /// Process a spreadsheet request
    pub async fn process_request(&mut self, request: SpreadsheetRequest) -> Result<SpreadsheetResponse> {
        let request_id = request.request_id.clone();
        
        match request.operation {
            OperationType::Transform => {
                self.handle_transform(request).await
            }
            OperationType::GetTransformations => {
                self.handle_get_transformations(request).await
            }
            OperationType::Validate => {
                self.handle_validate(request).await
            }
            OperationType::GetTemplates => {
                self.handle_get_templates(request).await
            }
            OperationType::Preview => {
                self.handle_preview(request).await
            }
        }.map(|mut response| {
            response.request_id = request_id;
            response
        })
    }
    
    /// Handle data transformation requests
    async fn handle_transform(&mut self, request: SpreadsheetRequest) -> Result<SpreadsheetResponse> {
        let transformation = request.transformation.ok_or_else(|| {
            VectrillError::InvalidConfig("Transformation configuration required".to_string())
        })?;
        
        // Convert spreadsheet data to Arrow format
        let record_batch = self.spreadsheet_to_arrow(&request.data)?;
        
        // Apply transformation
        let mut pipeline = self.build_pipeline(&transformation)?;
        let transformed_batch = pipeline.apply(record_batch).await?;
        
        // Convert back to spreadsheet format
        let output_data = self.arrow_to_spreadsheet(&transformed_batch, &request.output)?;
        
        let metadata = TransformationMetadata {
            input_rows: request.data.rows.len(),
            output_rows: output_data.rows.len(),
            columns: output_data.headers.len(),
            execution_time_ms: 0, // TODO: Measure actual execution time
            steps: vec![transformation.transform_type.to_string()],
        };
        
        Ok(SpreadsheetResponse {
            request_id: String::new(), // Will be set by caller
            success: true,
            data: Some(output_data),
            error: None,
            metadata: Some(metadata),
        })
    }
    
    /// Handle get transformations request
    async fn handle_get_transformations(&self, _request: SpreadsheetRequest) -> Result<SpreadsheetResponse> {
        // Return available transformations
        let transformations = vec![
            AvailableTransformation {
                name: "Filter".to_string(),
                description: "Filter rows based on conditions".to_string(),
                parameters: vec![
                    ParameterDef {
                        name: "column".to_string(),
                        param_type: DataType::String,
                        required: true,
                        description: "Column to filter on".to_string(),
                    },
                    ParameterDef {
                        name: "operator".to_string(),
                        param_type: DataType::String,
                        required: true,
                        description: "Comparison operator".to_string(),
                    },
                    ParameterDef {
                        name: "value".to_string(),
                        param_type: DataType::String,
                        required: true,
                        description: "Value to compare against".to_string(),
                    },
                ],
            },
            AvailableTransformation {
                name: "Map".to_string(),
                description: "Apply a function to a column".to_string(),
                parameters: vec![
                    ParameterDef {
                        name: "column".to_string(),
                        param_type: DataType::String,
                        required: true,
                        description: "Column to transform".to_string(),
                    },
                    ParameterDef {
                        name: "function".to_string(),
                        param_type: DataType::String,
                        required: true,
                        description: "Function to apply".to_string(),
                    },
                ],
            },
        ];
        
        // Convert to spreadsheet data format
        let headers = vec!["name".to_string(), "description".to_string(), "parameters".to_string()];
        let mut rows = Vec::new();
        
        for transform in transformations {
            let params_str = format!("{} parameters", transform.parameters.len());
            rows.push(vec![
                CellValue::String(transform.name),
                CellValue::String(transform.description),
                CellValue::String(params_str),
            ]);
        }
        
        let data = SpreadsheetData {
            headers,
            rows,
            column_types: vec![DataType::String, DataType::String, DataType::String],
            range: None,
            sheet_name: None,
        };
        
        Ok(SpreadsheetResponse {
            request_id: String::new(), // Will be set by caller
            success: true,
            data: Some(data),
            error: None,
            metadata: None,
        })
    }
    
    /// Handle validation request
    async fn handle_validate(&self, request: SpreadsheetRequest) -> Result<SpreadsheetResponse> {
        let transformation = request.transformation.ok_or_else(|| {
            VectrillError::InvalidConfig("Transformation configuration required".to_string())
        })?;
        
        // Validate transformation configuration
        let validation_result = self.validate_transformation(&transformation, &request.data)?;
        
        Ok(SpreadsheetResponse {
            request_id: String::new(), // Will be set by caller
            success: validation_result.is_valid,
            data: None,
            error: if validation_result.is_valid { None } else { validation_result.error.clone() },
            metadata: None,
        })
    }
    
    /// Handle get templates request
    async fn handle_get_templates(&self, _request: SpreadsheetRequest) -> Result<SpreadsheetResponse> {
        let headers = vec!["name".to_string(), "description".to_string(), "category".to_string()];
        let mut rows = Vec::new();
        
        for (name, template) in &self.templates {
            rows.push(vec![
                CellValue::String(name.clone()),
                CellValue::String(template.description.clone()),
                CellValue::String(template.category.clone()),
            ]);
        }
        
        let data = SpreadsheetData {
            headers,
            rows,
            column_types: vec![DataType::String, DataType::String, DataType::String],
            range: None,
            sheet_name: None,
        };
        
        Ok(SpreadsheetResponse {
            request_id: String::new(), // Will be set by caller
            success: true,
            data: Some(data),
            error: None,
            metadata: None,
        })
    }
    
    /// Handle preview request
    async fn handle_preview(&mut self, request: SpreadsheetRequest) -> Result<SpreadsheetResponse> {
        // Similar to transform but limit output to first few rows
        let mut preview_request = request;
        preview_request.output.max_rows = Some(10); // Preview first 10 rows
        
        self.handle_transform(preview_request).await
    }
    
    /// Convert spreadsheet data to Arrow RecordBatch
    fn spreadsheet_to_arrow(&self, data: &SpreadsheetData) -> Result<crate::RecordBatch> {
        // TODO: Implement conversion from spreadsheet format to Arrow
        // This would involve:
        // 1. Creating Arrow schema from headers and column types
        // 2. Converting rows to Arrow arrays
        // 3. Creating RecordBatch
        
        Err(VectrillError::NotImplemented("spreadsheet_to_arrow conversion not yet implemented".to_string()))
    }
    
    /// Convert Arrow RecordBatch to spreadsheet data
    fn arrow_to_spreadsheet(&self, batch: &crate::RecordBatch, output: &OutputConfig) -> Result<SpreadsheetData> {
        // TODO: Implement conversion from Arrow to spreadsheet format
        // This would involve:
        // 1. Extracting headers from schema
        // 2. Converting Arrow arrays to cell values
        // 3. Applying output format (transpose, summary, etc.)
        
        Err(VectrillError::NotImplemented("arrow_to_spreadsheet conversion not yet implemented".to_string()))
    }
    
    /// Build transformation pipeline from configuration
    fn build_pipeline(&self, config: &TransformationConfig) -> Result<TransformationPipeline> {
        // TODO: Build pipeline from configuration
        Err(VectrillError::NotImplemented("Pipeline building not yet implemented".to_string()))
    }
    
    /// Validate transformation configuration
    fn validate_transformation(&self, config: &TransformationConfig, data: &SpreadsheetData) -> Result<ValidationResult> {
        // TODO: Validate configuration against data schema
        Ok(ValidationResult {
            is_valid: true,
            error: None,
            warnings: Vec::new(),
        })
    }
}

/// Available transformation definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AvailableTransformation {
    pub name: String,
    pub description: String,
    pub parameters: Vec<ParameterDef>,
}

/// Parameter definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParameterDef {
    pub name: String,
    pub param_type: DataType,
    pub required: bool,
    pub description: String,
}

/// Validation result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationResult {
    pub is_valid: bool,
    pub error: Option<String>,
    pub warnings: Vec<String>,
}

/// Transformation template
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformationTemplate {
    pub name: String,
    pub description: String,
    pub category: String,
    pub transformations: Vec<TransformationConfig>,
}

impl Default for SpreadsheetAPI {
    fn default() -> Self {
        Self::new()
    }
}

// Implement Display for various types
impl std::fmt::Display for TransformType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransformType::Filter => write!(f, "Filter"),
            TransformType::Map => write!(f, "Map"),
            TransformType::Aggregate => write!(f, "Aggregate"),
            TransformType::Sort => write!(f, "Sort"),
            TransformType::GroupBy => write!(f, "GroupBy"),
            TransformType::Join => write!(f, "Join"),
            TransformType::Custom(name) => write!(f, "{}", name),
        }
    }
}

impl std::fmt::Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataType::String => write!(f, "string"),
            DataType::Number => write!(f, "number"),
            DataType::Boolean => write!(f, "boolean"),
            DataType::Date => write!(f, "date"),
            DataType::Empty => write!(f, "empty"),
        }
    }
}

impl std::fmt::Display for CellValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CellValue::String(s) => write!(f, "{}", s),
            CellValue::Number(n) => write!(f, "{}", n),
            CellValue::Boolean(b) => write!(f, "{}", b),
            CellValue::Empty => write!(f, ""),
            CellValue::Error(e) => write!(f, "ERROR: {}", e),
        }
    }
}
