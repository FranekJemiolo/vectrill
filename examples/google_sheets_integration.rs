//! Google Sheets Integration Example
//!
//! This example demonstrates how Vectrill could be integrated with Google Sheets
//! through Google Apps Script. This shows the architecture and API design
//! for cloud-based spreadsheet integration.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;
use vectrill::spreadsheet::api::{
    CellValue, DataType as SpreadsheetDataType, OutputConfig, SpreadsheetData, TransformType,
    TransformationConfig,
};
use vectrill::spreadsheet::data_bridge::DataBridge;
use vectrill::spreadsheet::{
    OperationType, SpreadsheetAPI, SpreadsheetRequest, SpreadsheetResponse,
};

// Mock Google Apps Script interfaces (in real implementation, these would be JavaScript)
#[allow(dead_code)]
pub struct GoogleAppsScriptAPI {
    spreadsheet_id: String,
    sheets: HashMap<String, GoogleSheet>,
}

#[allow(dead_code)]
pub struct GoogleSheet {
    name: String,
    ranges: HashMap<String, GoogleRange>,
}

#[allow(dead_code)]
pub struct GoogleRange {
    values: Vec<Vec<String>>,
    range: String,
}

#[allow(dead_code)]
pub struct GoogleSheetsUI {
    menus: Vec<GoogleMenu>,
    custom_functions: Vec<GoogleCustomFunction>,
}

#[allow(dead_code)]
pub struct GoogleMenu {
    name: String,
    items: Vec<GoogleMenuItem>,
}

#[allow(dead_code)]
pub struct GoogleMenuItem {
    name: String,
    function_name: String,
}

#[allow(dead_code)]
pub struct GoogleCustomFunction {
    name: String,
    description: String,
    parameters: Vec<GoogleParameter>,
    function: Box<dyn Fn(&[String]) -> String + Send + Sync>,
}

#[allow(dead_code)]
pub struct GoogleParameter {
    name: String,
    description: String,
}

/// Google Sheets Integration for Vectrill
pub struct VectrillGoogleSheetsIntegration {
    /// Google Apps Script API reference
    gas_api: Arc<Mutex<GoogleAppsScriptAPI>>,
    /// Vectrill API instance
    vectrill_api: Arc<Mutex<SpreadsheetAPI>>,
    /// Data bridge for Google Sheets conversions
    data_bridge: DataBridge,
    /// Tokio runtime for async operations
    runtime: Arc<Runtime>,
    /// Custom function registry
    custom_functions: Arc<Mutex<HashMap<String, GoogleCustomFunction>>>,
}

impl VectrillGoogleSheetsIntegration {
    /// Create new Google Sheets integration instance
    pub fn new(spreadsheet_id: String) -> Result<Self, Box<dyn std::error::Error>> {
        let runtime = Arc::new(Runtime::new()?);
        let gas_api = Arc::new(Mutex::new(GoogleAppsScriptAPI {
            spreadsheet_id,
            sheets: HashMap::new(),
        }));

        Ok(Self {
            gas_api,
            vectrill_api: Arc::new(Mutex::new(SpreadsheetAPI::new())),
            data_bridge: DataBridge::new(),
            runtime,
            custom_functions: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// Initialize the integration
    pub fn initialize(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🚀 Initializing Vectrill Google Sheets Integration...");

        // Create custom functions
        self.create_custom_functions()?;

        // Create menu items
        self.create_menu_items()?;

        // Set up triggers
        self.setup_triggers()?;

        println!("✅ Google Sheets Integration initialized successfully!");
        Ok(())
    }

    /// Create custom functions for Google Sheets
    fn create_custom_functions(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut functions = self.custom_functions.lock().unwrap();

        // VECTRILL_TRANSFORM function
        functions.insert(
            "VECTRILL_TRANSFORM".to_string(),
            GoogleCustomFunction {
                name: "VECTRILL_TRANSFORM".to_string(),
                description: "Apply Vectrill transformation to data range".to_string(),
                parameters: vec![
                    GoogleParameter {
                        name: "range".to_string(),
                        description: "Data range to transform (e.g., 'A1:C100')".to_string(),
                    },
                    GoogleParameter {
                        name: "transform_type".to_string(),
                        description: "Type of transformation to apply".to_string(),
                    },
                    GoogleParameter {
                        name: "column".to_string(),
                        description: "Column to transform (optional)".to_string(),
                    },
                    GoogleParameter {
                        name: "value".to_string(),
                        description: "Value for filter/operation (optional)".to_string(),
                    },
                ],
                function: Box::new(|args| {
                    // In real implementation, this would call Vectrill API
                    format!("Transformed: {:?}", args)
                }),
            },
        );

        // VECTRILL_FILTER function
        functions.insert(
            "VECTRILL_FILTER".to_string(),
            GoogleCustomFunction {
                name: "VECTRILL_FILTER".to_string(),
                description: "Filter data range based on conditions".to_string(),
                parameters: vec![
                    GoogleParameter {
                        name: "range".to_string(),
                        description: "Data range to filter".to_string(),
                    },
                    GoogleParameter {
                        name: "column".to_string(),
                        description: "Column to filter on".to_string(),
                    },
                    GoogleParameter {
                        name: "condition".to_string(),
                        description: "Filter condition".to_string(),
                    },
                    GoogleParameter {
                        name: "value".to_string(),
                        description: "Value to compare against".to_string(),
                    },
                ],
                function: Box::new(|args| format!("Filtered: {:?}", args)),
            },
        );

        // VECTRILL_AGGREGATE function
        functions.insert(
            "VECTRILL_AGGREGATE".to_string(),
            GoogleCustomFunction {
                name: "VECTRILL_AGGREGATE".to_string(),
                description: "Aggregate data by groups".to_string(),
                parameters: vec![
                    GoogleParameter {
                        name: "range".to_string(),
                        description: "Data range to aggregate".to_string(),
                    },
                    GoogleParameter {
                        name: "group_by".to_string(),
                        description: "Columns to group by".to_string(),
                    },
                    GoogleParameter {
                        name: "aggregation".to_string(),
                        description: "Aggregation function".to_string(),
                    },
                ],
                function: Box::new(|args| format!("Aggregated: {:?}", args)),
            },
        );

        // VECTRILL_PIVOT function
        functions.insert(
            "VECTRILL_PIVOT".to_string(),
            GoogleCustomFunction {
                name: "VECTRILL_PIVOT".to_string(),
                description: "Create pivot table from data".to_string(),
                parameters: vec![
                    GoogleParameter {
                        name: "range".to_string(),
                        description: "Data range for pivot".to_string(),
                    },
                    GoogleParameter {
                        name: "index".to_string(),
                        description: "Index columns".to_string(),
                    },
                    GoogleParameter {
                        name: "columns".to_string(),
                        description: "Column headers".to_string(),
                    },
                    GoogleParameter {
                        name: "values".to_string(),
                        description: "Value columns".to_string(),
                    },
                    GoogleParameter {
                        name: "function".to_string(),
                        description: "Aggregation function".to_string(),
                    },
                ],
                function: Box::new(|args| format!("Pivoted: {:?}", args)),
            },
        );

        // VECTRILL_SUMMARY function
        functions.insert(
            "VECTRILL_SUMMARY".to_string(),
            GoogleCustomFunction {
                name: "VECTRILL_SUMMARY".to_string(),
                description: "Generate summary statistics".to_string(),
                parameters: vec![GoogleParameter {
                    name: "range".to_string(),
                    description: "Data range to summarize".to_string(),
                }],
                function: Box::new(|args| format!("Summary: {:?}", args)),
            },
        );

        println!("📊 Created {} custom functions", functions.len());
        Ok(())
    }

    /// Create menu items
    fn create_menu_items(&self) -> Result<(), Box<dyn std::error::Error>> {
        let mut ui = GoogleSheetsUI {
            menus: Vec::new(),
            custom_functions: Vec::new(),
        };

        // Create Vectrill menu
        let vectrill_menu = GoogleMenu {
            name: "Vectrill".to_string(),
            items: vec![
                GoogleMenuItem {
                    name: "Transform Data".to_string(),
                    function_name: "showTransformDialog".to_string(),
                },
                GoogleMenuItem {
                    name: "Apply Template".to_string(),
                    function_name: "showTemplateGallery".to_string(),
                },
                GoogleMenuItem {
                    name: "Analyze Data".to_string(),
                    function_name: "analyzeData".to_string(),
                },
                GoogleMenuItem {
                    name: "Settings".to_string(),
                    function_name: "showSettings".to_string(),
                },
                GoogleMenuItem {
                    name: "Help".to_string(),
                    function_name: "showHelp".to_string(),
                },
            ],
        };

        ui.menus.push(vectrill_menu);

        println!(
            "📋 Created Vectrill menu with {} items",
            ui.menus[0].items.len()
        );
        Ok(())
    }

    /// Set up triggers for automatic processing
    fn setup_triggers(&self) -> Result<(), Box<dyn std::error::Error>> {
        // In real implementation, this would set up Google Apps Script triggers
        println!("⚡ Set up automatic triggers");
        Ok(())
    }

    /// Transform data range
    pub async fn transform_range(
        &self,
        range: &str,
        transform_config: &TransformationConfig,
    ) -> Result<SpreadsheetResponse, Box<dyn std::error::Error>> {
        println!("🔄 Transforming data in range: {}", range);

        // Get Google Sheets data
        let sheets_data = self.get_sheets_range_data(range)?;

        // Convert to Vectrill format
        let spreadsheet_data = self.sheets_to_spreadsheet_data(sheets_data)?;

        // Create transformation request
        let request = SpreadsheetRequest {
            request_id: format!("transform_{}", chrono::Utc::now().timestamp_millis()),
            operation: OperationType::Transform,
            data: spreadsheet_data,
            transformation: Some(transform_config.clone()),
            output: OutputConfig {
                format: vectrill::spreadsheet::api::OutputFormat::Same,
                include_headers: true,
                max_rows: None,
            },
        };

        // Process transformation
        let mut api = self.vectrill_api.lock().unwrap();
        let response = api.process_request(request).await?;

        println!("✅ Transformation completed successfully");
        Ok(response)
    }

    /// Get Google Sheets range data (mock implementation)
    fn get_sheets_range_data(
        &self,
        range: &str,
    ) -> Result<GoogleRange, Box<dyn std::error::Error>> {
        // In real implementation, this would call Google Sheets API
        // For now, return mock data

        let mock_data = vec![
            vec![
                "Product".to_string(),
                "Sales".to_string(),
                "Region".to_string(),
                "Date".to_string(),
            ],
            vec![
                "Widget A".to_string(),
                "1500".to_string(),
                "North".to_string(),
                "2024-01-15".to_string(),
            ],
            vec![
                "Widget B".to_string(),
                "2300".to_string(),
                "South".to_string(),
                "2024-01-16".to_string(),
            ],
            vec![
                "Widget C".to_string(),
                "1800".to_string(),
                "East".to_string(),
                "2024-01-17".to_string(),
            ],
            vec![
                "Widget D".to_string(),
                "3200".to_string(),
                "West".to_string(),
                "2024-01-18".to_string(),
            ],
            vec![
                "Widget E".to_string(),
                "2100".to_string(),
                "Central".to_string(),
                "2024-01-19".to_string(),
            ],
        ];

        Ok(GoogleRange {
            values: mock_data,
            range: range.to_string(),
        })
    }

    /// Convert Google Sheets data to SpreadsheetData
    fn sheets_to_spreadsheet_data(
        &self,
        sheets_range: GoogleRange,
    ) -> Result<SpreadsheetData, Box<dyn std::error::Error>> {
        if sheets_range.values.is_empty() {
            return Err("Empty Google Sheets range".into());
        }

        let headers = sheets_range.values[0].clone();
        let mut rows = Vec::new();
        let mut column_types = Vec::new();

        // Infer column types
        for col_idx in 0..headers.len() {
            let column_values: Vec<String> = sheets_range
                .values
                .iter()
                .skip(1) // Skip header row
                .filter_map(|row| row.get(col_idx))
                .cloned()
                .collect();

            let data_type = self.infer_column_type(&column_values);
            column_types.push(data_type);
        }

        // Convert data rows
        for row in sheets_range.values.iter().skip(1) {
            let mut spreadsheet_row = Vec::new();
            for (col_idx, cell_value) in row.iter().enumerate() {
                let cell_type = &column_types[col_idx];
                let converted_value = self.convert_sheets_cell(cell_value, cell_type)?;
                spreadsheet_row.push(converted_value);
            }
            rows.push(spreadsheet_row);
        }

        Ok(SpreadsheetData {
            headers,
            rows,
            column_types,
            range: Some(sheets_range.range),
            sheet_name: Some("Sheet1".to_string()),
        })
    }

    /// Infer column type from sample values
    fn infer_column_type(&self, values: &[String]) -> SpreadsheetDataType {
        if values.is_empty() {
            return SpreadsheetDataType::String;
        }

        let mut type_counts = HashMap::new();
        type_counts.insert(SpreadsheetDataType::String, 0);
        type_counts.insert(SpreadsheetDataType::Number, 0);
        type_counts.insert(SpreadsheetDataType::Boolean, 0);
        type_counts.insert(SpreadsheetDataType::Date, 0);
        type_counts.insert(SpreadsheetDataType::Empty, 0);

        for value in values {
            let inferred_type = if value.trim().is_empty() {
                SpreadsheetDataType::Empty
            } else if value.parse::<f64>().is_ok() {
                SpreadsheetDataType::Number
            } else if value.parse::<bool>().is_ok() {
                SpreadsheetDataType::Boolean
            } else if self.looks_like_date(value) {
                SpreadsheetDataType::Date
            } else {
                SpreadsheetDataType::String
            };

            *type_counts.entry(inferred_type).or_insert(0) += 1;
        }

        type_counts
            .iter()
            .max_by_key(|(_, &count)| count)
            .map(|(t, _)| t.clone())
            .unwrap_or(SpreadsheetDataType::String)
    }

    /// Check if string looks like a date
    fn looks_like_date(&self, value: &str) -> bool {
        value.contains('/') || value.contains('-') || value.contains(':')
    }

    /// Convert Google Sheets cell value to CellValue
    fn convert_sheets_cell(
        &self,
        value: &str,
        data_type: &SpreadsheetDataType,
    ) -> Result<CellValue, Box<dyn std::error::Error>> {
        match data_type {
            SpreadsheetDataType::String => Ok(CellValue::String(value.to_string())),
            SpreadsheetDataType::Number => {
                let num = value.parse::<f64>()?;
                Ok(CellValue::Number(num))
            }
            SpreadsheetDataType::Boolean => {
                let bool_val = value.parse::<bool>()?;
                Ok(CellValue::Boolean(bool_val))
            }
            SpreadsheetDataType::Date => Ok(CellValue::String(value.to_string())),
            SpreadsheetDataType::Empty => Ok(CellValue::Empty),
        }
    }

    /// Write transformed data back to Google Sheets
    pub fn write_to_sheets(
        &self,
        response: &SpreadsheetResponse,
        target_range: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(data) = &response.data {
            println!(
                "💾 Writing {} rows to Google Sheets range: {}",
                data.rows.len(),
                target_range
            );

            // Convert SpreadsheetData back to Google Sheets format
            let sheets_data = self.spreadsheet_to_sheets_data(data)?;

            // In real implementation, this would write to Google Sheets via API
            println!("📝 Data written to Google Sheets successfully");

            // Display transformation metadata
            if let Some(metadata) = &response.metadata {
                println!("📊 Transformation Summary:");
                println!("  - Input rows: {}", metadata.input_rows);
                println!("  - Output rows: {}", metadata.output_rows);
                println!("  - Columns: {}", metadata.columns);
                println!("  - Steps: {:?}", metadata.steps);
            }
        }

        Ok(())
    }

    /// Convert SpreadsheetData back to Google Sheets format
    fn spreadsheet_to_sheets_data(
        &self,
        data: &SpreadsheetData,
    ) -> Result<GoogleRange, Box<dyn std::error::Error>> {
        let mut sheets_values = Vec::new();

        // Add headers
        sheets_values.push(data.headers.clone());

        // Add data rows
        for row in &data.rows {
            let mut sheets_row = Vec::new();
            for cell in row {
                let cell_str = match cell {
                    CellValue::String(s) => s.clone(),
                    CellValue::Number(n) => n.to_string(),
                    CellValue::Boolean(b) => b.to_string(),
                    CellValue::Empty => String::new(),
                    CellValue::Error(e) => format!("ERROR: {}", e),
                };
                sheets_row.push(cell_str);
            }
            sheets_values.push(sheets_row);
        }

        Ok(GoogleRange {
            values: sheets_values,
            range: "A1".to_string(), // Would be calculated based on data size
        })
    }

    /// Show transformation dialog (mock implementation)
    pub fn show_transform_dialog(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("🎨 Showing transformation dialog...");

        // In real implementation, this would show a modal dialog with:
        // - Available transformations
        // - Parameter inputs
        // - Preview functionality
        // - Apply button

        println!("📋 Available Transformations:");
        println!("  1. Filter - Filter rows based on conditions");
        println!("  2. Map - Apply functions to columns");
        println!("  3. Aggregate - Group and aggregate data");
        println!("  4. Sort - Sort data by columns");
        println!("  5. Pivot - Create pivot tables");

        Ok(())
    }

    /// Show template gallery (mock implementation)
    pub fn show_template_gallery(&self) -> Result<(), Box<dyn std::error::Error>> {
        println!("📋 Showing template gallery...");

        println!("🎯 Available Templates:");
        println!("  🧹 Data Cleaning");
        println!("    - Remove Duplicates");
        println!("    - Fill Missing Values");
        println!("    - Standardize Text");
        println!("  📊 Data Analysis");
        println!("    - Summary Statistics");
        println!("    - Pivot Table");
        println!("    - Time Series Analysis");
        println!("  🔍 Data Filtering");
        println!("    - Filter by Condition");
        println!("    - Top N Records");
        println!("    - Outlier Removal");
        println!("  🔄 Data Transformation");
        println!("    - Normalize Data");
        println!("    - Calculated Columns");
        println!("    - Data Aggregation");

        Ok(())
    }

    /// Generate JavaScript code for Google Apps Script
    pub fn generate_gas_code(&self) -> String {
        r#"
// Vectrill Google Apps Script Integration
// Generated by Vectrill

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Vectrill')
    .addItem('Transform Data', 'showTransformDialog')
    .addItem('Apply Template', 'showTemplateGallery')
    .addItem('Analyze Data', 'analyzeData')
    .addItem('Settings', 'showSettings')
    .addToUi();
}

// Custom function for data transformation
function VECTRILL_TRANSFORM(range, transformType, column, value) {
  try {
    // Get data from range
    const data = range.getValues();
    
    // Convert to Vectrill format
    const spreadsheetData = {
      headers: data[0],
      rows: data.slice(1).map(row => 
        row.map(cell => ({
          value: cell,
          type: inferType(cell)
        }))
      ),
      columnTypes: inferColumnTypes(data)
    };
    
    // Call Vectrill API (would be HTTP request to Vectrill server)
    const response = callVectrillAPI({
      operation: 'transform',
      data: spreadsheetData,
      transformType: transformType,
      column: column,
      value: value
    });
    
    // Convert response back to Sheets format
    return response.data.rows.map(row => 
      row.map(cell => cell.value)
    );
    
  } catch (error) {
    return 'Error: ' + error.toString();
  }
}

// Helper function to infer data type
function inferType(value) {
  if (value === '' || value === null || value === undefined) {
    return 'empty';
  } else if (!isNaN(value) && value !== '') {
    return 'number';
  } else if (value === 'TRUE' || value === 'FALSE') {
    return 'boolean';
  } else if (isValidDate(value)) {
    return 'date';
  } else {
    return 'string';
  }
}

// Helper function to check if value is a date
function isValidDate(value) {
  return !isNaN(Date.parse(value));
}

// Helper function to infer column types
function inferColumnTypes(data) {
  if (data.length === 0) return [];
  
  const columnCount = data[0].length;
  const types = [];
  
  for (let col = 0; col < columnCount; col++) {
    const columnValues = data.slice(1).map(row => row[col]);
    types.push(inferColumnType(columnValues));
  }
  
  return types;
}

// Helper function to infer column type from sample
function inferColumnType(values) {
  const typeCounts = {
    string: 0,
    number: 0,
    boolean: 0,
    date: 0,
    empty: 0
  };
  
  values.forEach(value => {
    const type = inferType(value);
    typeCounts[type]++;
  });
  
  return Object.keys(typeCounts).reduce((a, b) => 
    typeCounts[a] > typeCounts[b] ? a : b
  );
}

// Mock API call (would be real HTTP request)
function callVectrillAPI(request) {
  // In real implementation, this would make HTTP request to Vectrill server
  return {
    success: true,
    data: {
      rows: request.data.rows.map(row => 
        row.map(cell => ({ value: cell.value }))
      )
    }
  };
}

// Show transformation dialog
function showTransformDialog() {
  const html = HtmlService.createHtmlOutput(`
    <dialog>
      <h3>Vectrill Data Transformation</h3>
      <p>Select a transformation to apply to your data:</p>
      
      <select id="transformType">
        <option value="filter">Filter</option>
        <option value="map">Map</option>
        <option value="aggregate">Aggregate</option>
        <option value="sort">Sort</option>
        <option value="pivot">Pivot</option>
      </select>
      
      <br><br>
      
      <label>Column:</label>
      <input type="text" id="column" placeholder="Column name">
      
      <br><br>
      
      <label>Value:</label>
      <input type="text" id="value" placeholder="Filter value">
      
      <br><br>
      
      <button onclick="applyTransformation()">Apply</button>
      <button onclick="closeDialog()">Cancel</button>
    </dialog>
  `);
  
  SpreadsheetApp.getUi().showModalDialog(html);
}

// Apply transformation from dialog
function applyTransformation() {
  const transformType = document.getElementById('transformType').value;
  const column = document.getElementById('column').value;
  const value = document.getElementById('value').value;
  const range = SpreadsheetApp.getActiveRange();
  
  // Apply transformation
  const result = VECTRILL_TRANSFORM(range, transformType, column, value);
  
  // Write results back to sheet
  range.offset(0, range.getNumColumns() + 1).setValues(result);
  
  SpreadsheetApp.getUi().alert('Transformation applied successfully!');
}

// Close dialog
function closeDialog() {
  SpreadsheetApp.getUi().showModalDialog(null);
}
        "#
        .to_string()
    }
}

/// Example usage of the Google Sheets integration
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Vectrill Google Sheets Integration");
    println!("=======================================");

    // Create integration instance
    let integration = VectrillGoogleSheetsIntegration::new("your-spreadsheet-id".to_string())?;

    // Initialize the integration
    integration.initialize()?;

    // Demonstrate functionality
    println!("\n📊 Demonstrating Google Sheets Integration:");

    // Show available custom functions
    let functions = integration.custom_functions.lock().unwrap();
    println!("\n🔧 Custom Functions:");
    for (name, func) in functions.iter() {
        println!("  - {}: {}", name, func.description);
        println!("    Parameters: {}", func.parameters.len());
    }

    // Show transformation dialog
    integration.show_transform_dialog()?;

    // Show template gallery
    integration.show_template_gallery()?;

    // Example transformation
    println!("\n🔄 Example Transformation:");
    let transform_config = TransformationConfig {
        transform_type: TransformType::Filter,
        column: "Sales".to_string(),
        parameters: HashMap::new(),
        output_column: None,
    };

    let response = integration
        .transform_range("A1:D6", &transform_config)
        .await?;
    integration.write_to_sheets(&response, "F1:I6")?;

    // Generate Google Apps Script code
    println!("\n📝 Generated Google Apps Script Code:");
    let gas_code = integration.generate_gas_code();
    println!("{}", gas_code);

    println!("\n✅ Google Sheets Integration prototype completed successfully!");
    println!("\n📝 Next Steps for Production:");
    println!("  1. Deploy Vectrill API server with HTTPS endpoint");
    println!("  2. Create Google Workspace Marketplace listing");
    println!("  3. Implement OAuth2 authentication");
    println!("  4. Add real-time collaboration features");
    println!("  5. Create comprehensive testing suite");
    println!("  6. Add error handling and retry logic");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integration_creation() {
        let integration = VectrillGoogleSheetsIntegration::new("test-id".to_string());
        assert!(integration.is_ok());
    }

    #[test]
    fn test_column_type_inference() {
        let integration = VectrillGoogleSheetsIntegration::new("test-id".to_string()).unwrap();

        let numbers = vec!["1", "2", "3", "4.5"];
        let data_type = integration.infer_column_type(&numbers);
        assert!(matches!(data_type, SpreadsheetDataType::Number));

        let strings = vec!["hello", "world", "test"];
        let data_type = integration.infer_column_type(&strings);
        assert!(matches!(data_type, SpreadsheetDataType::String));

        let booleans = vec!["true", "false", "yes"];
        let data_type = integration.infer_column_type(&booleans);
        assert!(matches!(data_type, SpreadsheetDataType::Boolean));
    }

    #[test]
    fn test_sheets_data_conversion() {
        let integration = VectrillGoogleSheetsIntegration::new("test-id".to_string()).unwrap();

        let sheets_range = GoogleRange {
            value: vec![
                vec!["Name".to_string(), "Score".to_string()],
                vec!["Alice".to_string(), "95".to_string()],
                vec!["Bob".to_string(), "87".to_string()],
            ],
            range: "A1:B3".to_string(),
        };

        let spreadsheet_data = integration.sheets_to_spreadsheet_data(sheets_range);
        assert!(spreadsheet_data.is_ok());

        let data = spreadsheet_data.unwrap();
        assert_eq!(data.headers, vec!["Name", "Score"]);
        assert_eq!(data.rows.len(), 2);
        assert_eq!(data.column_types.len(), 2);
    }

    #[test]
    fn test_gas_code_generation() {
        let integration = VectrillGoogleSheetsIntegration::new("test-id".to_string()).unwrap();
        let code = integration.generate_gas_code();
        assert!(code.contains("VECTRILL_TRANSFORM"));
        assert!(code.contains("function onOpen"));
        assert!(code.contains("SpreadsheetApp"));
    }
}
