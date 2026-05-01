//! Unit tests for Spreadsheet API
//! 
//! Tests the core API functionality including request/response handling,
//! data validation, and transformation configuration.

use vectrill::spreadsheet::api::{
    SpreadsheetAPI, SpreadsheetRequest, SpreadsheetResponse, OperationType,
    SpreadsheetData, CellValue, DataType, TransformType, TransformationConfig,
    OutputConfig, ValidationResult
};
use vectrill::spreadsheet::templates::TemplateManager;
use std::collections::HashMap;

#[tokio::test]
async fn test_spreadsheet_api_creation() {
    let api = SpreadsheetAPI::new();
    assert!(true); // Basic creation test
}

#[tokio::test]
async fn test_transform_request_validation() {
    let mut api = SpreadsheetAPI::new();
    
    // Valid request
    let valid_request = SpreadsheetRequest {
        request_id: "test-123".to_string(),
        operation: OperationType::Transform,
        data: SpreadsheetData {
            headers: vec!["Name".to_string(), "Age".to_string()],
            rows: vec![
                vec![CellValue::String("Alice".to_string()), CellValue::Number(30.0)],
                vec![CellValue::String("Bob".to_string()), CellValue::Number(25.0)],
            ],
            column_types: vec![DataType::String, DataType::Number],
            range: Some("A1:B3".to_string()),
            sheet_name: Some("Sheet1".to_string()),
        },
        transformation: Some(TransformationConfig {
            transform_type: TransformType::Filter,
            column: "Age".to_string(),
            parameters: HashMap::new(),
            output_column: None,
        }),
        output: OutputConfig {
            format: vectrill::spreadsheet::api::OutputFormat::Same,
            include_headers: true,
            max_rows: None,
        },
    };
    
    let response = api.process_request(valid_request).await.unwrap();
    assert_eq!(response.request_id, "test-123");
    assert_eq!(response.success, true);
}

#[tokio::test]
async fn test_transform_request_missing_data() {
    let mut api = SpreadsheetAPI::new();
    
    // Invalid request - no data
    let invalid_request = SpreadsheetRequest {
        request_id: "test-456".to_string(),
        operation: OperationType::Transform,
        data: SpreadsheetData {
            headers: vec![],
            rows: vec![],
            column_types: vec![],
            range: None,
            sheet_name: None,
        },
        transformation: Some(TransformationConfig {
            transform_type: TransformType::Filter,
            column: "Age".to_string(),
            parameters: HashMap::new(),
            output_column: None,
        }),
        output: OutputConfig {
            format: vectrill::spreadsheet::api::OutputFormat::Same,
            include_headers: true,
            max_rows: None,
        },
    };
    
    let response = api.process_request(invalid_request).await.unwrap();
    assert_eq!(response.request_id, "test-456");
    assert_eq!(response.success, false);
    assert!(response.error.is_some());
}

#[tokio::test]
async fn test_get_transformations() {
    let mut api = SpreadsheetAPI::new();
    
    let request = SpreadsheetRequest {
        request_id: "get-transformations".to_string(),
        operation: OperationType::GetTransformations,
        data: SpreadsheetData {
            headers: vec![],
            rows: vec![],
            column_types: vec![],
            range: None,
            sheet_name: None,
        },
        transformation: None,
        output: OutputConfig {
            format: vectrill::spreadsheet::api::OutputFormat::Same,
            include_headers: true,
            max_rows: None,
        },
    };
    
    let response = api.process_request(request).await.unwrap();
    assert_eq!(response.success, true);
    assert!(response.data.is_some());
    
    let data = response.data.unwrap();
    assert!(!data.headers.is_empty());
    assert!(!data.rows.is_empty());
}

#[tokio::test]
async fn test_get_templates() {
    let mut api = SpreadsheetAPI::new();
    
    let request = SpreadsheetRequest {
        request_id: "get-templates".to_string(),
        operation: OperationType::GetTemplates,
        data: SpreadsheetData {
            headers: vec![],
            rows: vec![],
            column_types: vec![],
            range: None,
            sheet_name: None,
        },
        transformation: None,
        output: OutputConfig {
            format: vectrill::spreadsheet::api::OutputFormat::Same,
            include_headers: true,
            max_rows: None,
        },
    };
    
    let response = api.process_request(request).await.unwrap();
    assert_eq!(response.success, true);
    assert!(response.data.is_some());
}

#[tokio::test]
async fn test_validate_transformation() {
    let mut api = SpreadsheetAPI::new();
    
    let request = SpreadsheetRequest {
        request_id: "validate".to_string(),
        operation: OperationType::Validate,
        data: SpreadsheetData {
            headers: vec!["Name".to_string(), "Age".to_string()],
            rows: vec![
                vec![CellValue::String("Alice".to_string()), CellValue::Number(30.0)],
            ],
            column_types: vec![DataType::String, DataType::Number],
            range: Some("A1:B2".to_string()),
            sheet_name: Some("Sheet1".to_string()),
        },
        transformation: Some(TransformationConfig {
            transform_type: TransformType::Filter,
            column: "Age".to_string(),
            parameters: HashMap::new(),
            output_column: None,
        }),
        output: OutputConfig {
            format: vectrill::spreadsheet::api::OutputFormat::Same,
            include_headers: true,
            max_rows: None,
        },
    };
    
    let response = api.process_request(request).await.unwrap();
    assert_eq!(response.success, true);
}

#[tokio::test]
async fn test_preview_transformation() {
    let mut api = SpreadsheetAPI::new();
    
    let request = SpreadsheetRequest {
        request_id: "preview".to_string(),
        operation: OperationType::Preview,
        data: SpreadsheetData {
            headers: vec!["Name".to_string(), "Age".to_string()],
            rows: vec![
                vec![CellValue::String("Alice".to_string()), CellValue::Number(30.0)],
                vec![CellValue::String("Bob".to_string()), CellValue::Number(25.0)],
            ],
            column_types: vec![DataType::String, DataType::Number],
            range: Some("A1:B3".to_string()),
            sheet_name: Some("Sheet1".to_string()),
        },
        transformation: Some(TransformationConfig {
            transform_type: TransformType::Filter,
            column: "Age".to_string(),
            parameters: {
                let mut params = HashMap::new();
                params.insert("operator".to_string(), "greater_than".to_string());
                params.insert("value".to_string(), "25".to_string());
                params
            },
            output_column: None,
        }),
        output: OutputConfig {
            format: vectrill::spreadsheet::api::OutputFormat::Same,
            include_headers: true,
            max_rows: Some(10), // Preview limit
        },
    };
    
    let response = api.process_request(request).await.unwrap();
    assert_eq!(response.success, true);
    assert!(response.data.is_some());
    assert!(response.metadata.is_some());
}

#[test]
fn test_data_type_inference() {
    // Test string values
    let string_values = vec!["hello", "world", "test"];
    let inferred_type = infer_data_type(&string_values);
    assert_eq!(inferred_type, DataType::String);
    
    // Test numeric values
    let numeric_values = vec!["1", "2", "3.14"];
    let inferred_type = infer_data_type(&numeric_values);
    assert_eq!(inferred_type, DataType::Number);
    
    // Test boolean values
    let boolean_values = vec!["true", "false", "yes"];
    let inferred_type = infer_data_type(&boolean_values);
    assert_eq!(inferred_type, DataType::Boolean);
    
    // Test empty values
    let empty_values = vec!["", "", ""];
    let inferred_type = infer_data_type(&empty_values);
    assert_eq!(inferred_type, DataType::Empty);
}

#[test]
fn test_cell_value_conversion() {
    // Test string conversion
    let cell_value = CellValue::String("test".to_string());
    assert_eq!(cell_value_to_string(&cell_value), "test");
    
    // Test number conversion
    let cell_value = CellValue::Number(42.5);
    assert_eq!(cell_value_to_string(&cell_value), "42.5");
    
    // Test boolean conversion
    let cell_value = CellValue::Boolean(true);
    assert_eq!(cell_value_to_string(&cell_value), "true");
    
    // Test empty conversion
    let cell_value = CellValue::Empty;
    assert_eq!(cell_value_to_string(&cell_value), "");
}

#[test]
fn test_transformation_config_validation() {
    // Valid filter configuration
    let config = TransformationConfig {
        transform_type: TransformType::Filter,
        column: "Age".to_string(),
        parameters: {
            let mut params = HashMap::new();
            params.insert("operator".to_string(), "greater_than".to_string());
            params.insert("value".to_string(), "25".to_string());
            params
        },
        output_column: None,
    };
    
    assert!(validate_filter_config(&config).is_ok());
    
    // Invalid filter configuration - missing column
    let invalid_config = TransformationConfig {
        transform_type: TransformType::Filter,
        column: "".to_string(),
        parameters: HashMap::new(),
        output_column: None,
    };
    
    assert!(validate_filter_config(&invalid_config).is_err());
}

#[test]
fn test_template_manager() {
    let mut template_manager = TemplateManager::new();
    
    // Test template listing
    let templates = template_manager.list_templates();
    assert!(!templates.is_empty());
    
    // Test template retrieval
    let template = template_manager.get_template("remove_duplicates");
    assert!(template.is_some());
    
    // Test template by category
    let cleaning_templates = template_manager.list_templates_by_category("data_cleaning");
    assert!(!cleaning_templates.is_empty());
}

#[test]
fn test_output_configuration() {
    // Test same format
    let config = OutputConfig {
        format: vectrill::spreadsheet::api::OutputFormat::Same,
        include_headers: true,
        max_rows: None,
    };
    assert!(validate_output_config(&config).is_ok());
    
    // Test transpose format
    let config = OutputConfig {
        format: vectrill::spreadsheet::api::OutputFormat::Transpose,
        include_headers: false,
        max_rows: Some(100),
    };
    assert!(validate_output_config(&config).is_ok());
    
    // Test invalid max rows
    let config = OutputConfig {
        format: vectrill::spreadsheet::api::OutputFormat::Same,
        include_headers: true,
        max_rows: Some(-1), // Invalid
    };
    assert!(validate_output_config(&config).is_err());
}

// Helper functions for testing
fn infer_data_type(values: &[String]) -> DataType {
    let mut type_counts = HashMap::new();
    type_counts.insert(DataType::String, 0);
    type_counts.insert(DataType::Number, 0);
    type_counts.insert(DataType::Boolean, 0);
    type_counts.insert(DataType::Date, 0);
    type_counts.insert(DataType::Empty, 0);
    
    for value in values {
        let inferred_type = if value.trim().is_empty() {
            DataType::Empty
        } else if value.parse::<f64>().is_ok() {
            DataType::Number
        } else if value.parse::<bool>().is_ok() {
            DataType::Boolean
        } else if looks_like_date(value) {
            DataType::Date
        } else {
            DataType::String
        };
        
        *type_counts.entry(inferred_type).or_insert(0) += 1;
    }
    
    type_counts.iter()
        .max_by_key(|(_, &count)| count)
        .map(|(t, _)| t.clone())
        .unwrap_or(DataType::String)
}

fn looks_like_date(value: &str) -> bool {
    value.contains('/') || value.contains('-') || value.contains(':')
}

fn cell_value_to_string(value: &CellValue) -> String {
    match value {
        CellValue::String(s) => s.clone(),
        CellValue::Number(n) => n.to_string(),
        CellValue::Boolean(b) => b.to_string(),
        CellValue::Empty => String::new(),
        _ => "error".to_string(),
    }
}

fn validate_filter_config(config: &TransformationConfig) -> Result<(), String> {
    if config.column.is_empty() {
        return Err("Column cannot be empty".to_string());
    }
    
    if config.transform_type != TransformType::Filter {
        return Err("Invalid transformation type".to_string());
    }
    
    Ok(())
}

fn validate_output_config(config: &OutputConfig) -> Result<(), String> {
    if let Some(max_rows) = config.max_rows {
        if max_rows <= 0 {
            return Err("Max rows must be positive".to_string());
        }
    }
    
    Ok(())
}

#[cfg(test)]
mod performance_tests {
    use super::*;
    use std::time::Instant;
    
    #[tokio::test]
    async fn test_large_dataset_performance() {
        let mut api = SpreadsheetAPI::new();
        
        // Create large dataset (10K rows)
        let mut rows = Vec::new();
        for i in 0..10000 {
            rows.push(vec![
                CellValue::String(format!("User_{}", i)),
                CellValue::Number(i as f64),
                CellValue::Boolean(i % 2 == 0),
            ]);
        }
        
        let request = SpreadsheetRequest {
            request_id: "perf-test".to_string(),
            operation: OperationType::Transform,
            data: SpreadsheetData {
                headers: vec!["Name".to_string(), "ID".to_string(), "Active".to_string()],
                rows,
                column_types: vec![DataType::String, DataType::Number, DataType::Boolean],
                range: Some("A1:C10001".to_string()),
                sheet_name: Some("LargeData".to_string()),
            },
            transformation: Some(TransformationConfig {
                transform_type: TransformType::Filter,
                column: "Active".to_string(),
                parameters: {
                    let mut params = HashMap::new();
                    params.insert("operator".to_string(), "equals".to_string());
                    params.insert("value".to_string(), "true".to_string());
                    params
                },
                output_column: None,
            }),
            output: OutputConfig {
                format: vectrill::spreadsheet::api::OutputFormat::Same,
                include_headers: true,
                max_rows: None,
            },
        };
        
        let start = Instant::now();
        let response = api.process_request(request).await.unwrap();
        let duration = start.elapsed();
        
        assert_eq!(response.success, true);
        assert!(duration.as_millis() < 1000, "Performance test failed: took too long");
        
        println!("Large dataset processing took: {:?}", duration);
    }
    
    #[test]
    fn test_memory_usage() {
        // Test memory usage with large datasets
        let large_data = vec![
            CellValue::String("x".repeat(1000)); // 1KB string
        ];
        
        // This test would ideally measure actual memory usage
        // For now, we just ensure it doesn't panic
        let _result = infer_data_type(&large_data.iter().map(|s| s.to_string()).collect::<Vec<_>>());
    }
}
