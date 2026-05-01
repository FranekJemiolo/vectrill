//! Excel COM Add-in for Vectrill
//!
//! Provides native Excel integration through COM interface, allowing users to
//! access Vectrill's data processing capabilities directly from Excel.

use std::collections::HashMap;
use std::ffi::CStr;
use std::ptr;
use std::sync::{Arc, Mutex};

use widestring::U16CString;
use windows::core::{HSTRING, PCWSTR, PWSTR};
use windows::Win32::Foundation::{E_FAIL, E_NOTIMPL, E_POINTER, HRESULT, S_OK};
use windows::Win32::System::Com::{CoCreateInstance, CLSCTX_LOCAL_SERVER};
use windows::Win32::System::Ole::{IDispatch, VARIANT, VARIANT_0, VARIANT_0_0_0};
use windows::Win32::System::Variant::{VARENUM, VARIANTARG};
use windows::Win32::UI::WindowsAndMessaging::MessageBoxW;
use windows::w;
use crate::error::{Result, VectrillError};

/// Excel COM Add-in for Vectrill
pub struct ExcelAddin {
    /// Vectrill API client
    api_client: Arc<Mutex<VectrillApiClient>>,
    /// Excel application interface
    excel_app: Option<IDispatch>,
    /// Add-in configuration
    config: AddinConfig,
}

/// Configuration for Excel add-in
#[derive(Debug, Clone)]
pub struct AddinConfig {
    /// Vectrill server URL
    pub server_url: String,
    /// API authentication token
    pub auth_token: Option<String>,
    /// Default transformation template
    pub default_template: Option<String>,
    /// Enable real-time processing
    pub enable_realtime: bool,
    /// Maximum rows per request
    pub max_rows: usize,
}

impl Default for AddinConfig {
    fn default() -> Self {
        Self {
            server_url: "http://localhost:8080".to_string(),
            auth_token: None,
            default_template: None,
            enable_realtime: false,
            max_rows: 10000,
        }
    }
}

/// Vectrill API client for Excel add-in
pub struct VectrillApiClient {
    /// Server endpoint
    server_url: String,
    /// Authentication token
    auth_token: Option<String>,
    /// HTTP client
    client: Arc<Mutex<reqwest::blocking::Client>>,
}

impl VectrillApiClient {
    /// Create new API client
    pub fn new(config: &AddinConfig) -> Self {
        let mut headers = reqwest::header::HeaderMap::new();
        if let Some(token) = &config.auth_token {
            headers.insert(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", token).parse().unwrap(),
            );
        }

        let client = reqwest::blocking::Client::builder()
            .default_headers(headers)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap();

        Self {
            server_url: config.server_url.clone(),
            auth_token: config.auth_token.clone(),
            client: Arc::new(Mutex::new(client)),
        }
    }

    /// Send transformation request to Vectrill
    pub fn transform_data(&self, request: &TransformRequest) -> Result<TransformResponse> {
        let client = self.client.lock().unwrap();
        let url = format!("{}/api/spreadsheet/transform", self.server_url);
        
        let response = client
            .post(&url)
            .json(request)
            .send()
            .map_err(|e| VectrillError::NetworkError(format!("Request failed: {}", e)))?;

        if response.status().is_success() {
            let result = response
                .json::<TransformResponse>()
                .map_err(|e| VectrillError::NetworkError(format!("Failed to parse response: {}", e)))?;
            Ok(result)
        } else {
            Err(VectrillError::NetworkError(format!(
                "API request failed with status: {}",
                response.status()
            )))
        }
    }

    /// Get available transformation templates
    pub fn get_templates(&self) -> Result<Vec<TemplateInfo>> {
        let client = self.client.lock().unwrap();
        let url = format!("{}/api/spreadsheet/templates", self.server_url);
        
        let response = client
            .get(&url)
            .send()
            .map_err(|e| VectrillError::NetworkError(format!("Request failed: {}", e)))?;

        if response.status().is_success() {
            let templates = response
                .json::<Vec<TemplateInfo>>()
                .map_err(|e| VectrillError::NetworkError(format!("Failed to parse response: {}", e)))?;
            Ok(templates)
        } else {
            Err(VectrillError::NetworkError(format!(
                "API request failed with status: {}",
                response.status()
            )))
        }
    }
}

/// Transformation request from Excel
#[derive(Debug, serde::Serialize)]
pub struct TransformRequest {
    /// Request ID
    pub request_id: String,
    /// Spreadsheet data
    pub data: SpreadsheetData,
    /// Transformation configuration
    pub transformation: TransformationConfig,
    /// Output configuration
    pub output: OutputConfig,
}

/// Transformation response from Vectrill
#[derive(Debug, serde::Deserialize)]
pub struct TransformResponse {
    /// Request ID
    pub request_id: String,
    /// Transformed data
    pub data: SpreadsheetData,
    /// Processing statistics
    pub stats: ProcessingStats,
    /// Validation result
    pub validation: ValidationResult,
}

/// Processing statistics
#[derive(Debug, serde::Deserialize)]
pub struct ProcessingStats {
    /// Number of input rows
    pub input_rows: usize,
    /// Number of output rows
    pub output_rows: usize,
    /// Processing time in milliseconds
    pub processing_time_ms: u64,
    /// Memory usage in MB
    pub memory_usage_mb: f64,
}

/// Validation result
#[derive(Debug, serde::Deserialize)]
pub struct ValidationResult {
    /// Whether transformation is valid
    pub is_valid: bool,
    /// Validation errors
    pub errors: Vec<String>,
    /// Validation warnings
    pub warnings: Vec<String>,
}

/// Template information
#[derive(Debug, serde::Deserialize)]
pub struct TemplateInfo {
    /// Template ID
    pub id: String,
    /// Template name
    pub name: String,
    /// Template description
    pub description: String,
    /// Template category
    pub category: String,
}

/// Spreadsheet data from Excel
#[derive(Debug, serde::Serialize)]
pub struct SpreadsheetData {
    /// Headers
    pub headers: Vec<String>,
    /// Data rows
    pub rows: Vec<Vec<CellValue>>,
}

/// Cell value from Excel
#[derive(Debug, serde::Serialize)]
#[serde(untagged)]
pub enum CellValue {
    String(String),
    Number(f64),
    Boolean(bool),
    Empty,
    Error(String),
}

/// Transformation configuration
#[derive(Debug, serde::Serialize)]
pub struct TransformationConfig {
    /// Template ID to use
    pub template_id: Option<String>,
    /// Custom transformation steps
    pub steps: Vec<TransformStep>,
}

/// Transformation step
#[derive(Debug, serde::Serialize)]
pub struct TransformStep {
    /// Step type
    pub step_type: String,
    /// Step parameters
    pub parameters: HashMap<String, String>,
}

/// Output configuration
#[derive(Debug, serde::Serialize)]
pub struct OutputConfig {
    /// Output format
    pub format: String,
    /// Maximum rows to return
    pub max_rows: Option<usize>,
    /// Whether to transpose output
    pub transpose: bool,
    /// Whether to include summary
    pub include_summary: bool,
}

impl ExcelAddin {
    /// Create new Excel add-in instance
    pub fn new(config: AddinConfig) -> Self {
        Self {
            api_client: Arc::new(Mutex::new(VectrillApiClient::new(&config))),
            excel_app: None,
            config,
        }
    }

    /// Initialize Excel COM interface
    pub fn initialize(&mut self) -> Result<()> {
        unsafe {
            // Get Excel application object
            let excel_app: IDispatch = CoCreateInstance(&w!("Excel.Application"), CLSCTX_LOCAL_SERVER)
                .map_err(|e| VectrillError::ComError(format!("Failed to create Excel instance: {:?}", e)))?;

            self.excel_app = Some(excel_app);

            // Show welcome message
            self.show_message("Vectrill Add-in", "Vectrill integration initialized successfully!")?;
        }

        Ok(())
    }

    /// Transform selected Excel range
    pub fn transform_selection(&self) -> Result<()> {
        let excel_app = self.excel_app.as_ref()
            .ok_or_else(|| VectrillError::ComError("Excel not initialized".to_string()))?;

        unsafe {
            // Get selection from Excel
            let selection = self.get_excel_property(excel_app, "Selection")?;
            let range = self.get_excel_property(&selection, "CurrentRegion")?;
            
            // Get range data
            let value = self.get_excel_property(&range, "Value")?;
            let data = self.variant_to_spreadsheet_data(&value)?;

            // Create transformation request
            let request = TransformRequest {
                request_id: uuid::Uuid::new_v4().to_string(),
                data,
                transformation: TransformationConfig {
                    template_id: self.config.default_template.clone(),
                    steps: vec![],
                },
                output: OutputConfig {
                    format: "table".to_string(),
                    max_rows: Some(self.config.max_rows),
                    transpose: false,
                    include_summary: true,
                },
            };

            // Send transformation request
            let response = self.api_client.lock().unwrap().transform_data(&request)?;

            // Write results back to Excel
            self.write_results_to_excel(&response)?;

            // Show completion message
            self.show_message(
                "Vectrill Transformation",
                &format!(
                    "Successfully processed {} rows in {}ms",
                    response.stats.input_rows,
                    response.stats.processing_time_ms
                ),
            )?;
        }

        Ok(())
    }

    /// Get available templates and show in Excel
    pub fn show_templates(&self) -> Result<()> {
        let templates = self.api_client.lock().unwrap().get_templates()?;
        
        let mut message = "Available Vectrill Templates:\n\n".to_string();
        for template in &templates {
            message.push_str(&format!(
                "{}: {} ({})\n",
                template.id, template.name, template.description
            ));
        }

        let excel_app = self.excel_app.as_ref()
            .ok_or_else(|| VectrillError::ComError("Excel not initialized".to_string()))?;

        unsafe {
            self.show_message("Vectrill Templates", &message)?;
        }

        Ok(())
    }

    /// Show configuration dialog
    pub fn show_config(&self) -> Result<()> {
        let message = format!(
            "Vectrill Add-in Configuration:\n\n\
             Server URL: {}\n\
             Max Rows: {}\n\
             Real-time Processing: {}\n\
             Default Template: {:?}",
            self.config.server_url,
            self.config.max_rows,
            self.config.enable_realtime,
            self.config.default_template
        );

        let excel_app = self.excel_app.as_ref()
            .ok_or_else(|| VectrillError::ComError("Excel not initialized".to_string()))?;

        unsafe {
            self.show_message("Vectrill Configuration", &message)?;
        }

        Ok(())
    }

    /// Get Excel property using IDispatch
    unsafe fn get_excel_property(&self, obj: &IDispatch, name: &str) -> Result<VARIANT> {
        let name_wide = U16CString::from_str(name);
        let mut disp_id = [0u16; 1];
        let mut result = VARIANT::default();

        let hr = obj.GetIDsOfNames(
            &PCWSTR(name_wide.as_ptr()),
            ptr::addr_of_mut!(disp_id),
            ptr::null_mut(),
        );

        if hr != S_OK {
            return Err(VectrillError::ComError(format!("Failed to get dispatch ID for {}", name)));
        }

        let hr = obj.Invoke(
            disp_id[0],
            ptr::null_mut(),
            w::LOCALE_USER_DEFAULT,
            &mut VARIANTARG::default(),
            ptr::addr_of_mut!(result),
            ptr::null_mut(),
        );

        if hr != S_OK {
            return Err(VectrillError::ComError(format!("Failed to invoke property {}", name)));
        }

        Ok(result)
    }

    /// Convert VARIANT to spreadsheet data
    unsafe fn variant_to_spreadsheet_data(&self, variant: &VARIANT) -> Result<SpreadsheetData> {
        match variant.Anonymous.Anonymous.Anonymous.vt {
            VARENUM::VT_ARRAY | VARENUM::VT_VARIANT => {
                // Handle 2D array from Excel
                let array = variant.Anonymous.Anonymous.Anonymous.Anonymous.parray;
                if array.is_null() {
                    return Ok(SpreadsheetData {
                        headers: vec![],
                        rows: vec![],
                    });
                }

                // Get array bounds
                let mut lower_bounds = [0i32; 2];
                let mut upper_bounds = [0i32; 2];
                let hr = windows::Win32::System::Com::SafeArrayGetLBound(
                    *array,
                    1,
                    &mut lower_bounds[0],
                );
                if hr != S_OK {
                    return Err(VectrillError::ComError("Failed to get array bounds".to_string()));
                }

                let hr = windows::Win32::System::Com::SafeArrayGetUBound(
                    *array,
                    1,
                    &mut upper_bounds[0],
                );
                if hr != S_OK {
                    return Err(VectrillError::ComError("Failed to get array bounds".to_string()));
                }

                let rows = (upper_bounds[0] - lower_bounds[0] + 1) as usize;
                let cols = (upper_bounds[1] - lower_bounds[1] + 1) as usize;

                // Extract data from array
                let mut headers = Vec::new();
                let mut data_rows = Vec::new();

                for row in 0..rows {
                    let mut row_data = Vec::new();
                    for col in 0..cols {
                        let mut indices = [row as i32, col as i32];
                        let mut variant = VARIANT::default();
                        
                        let hr = windows::Win32::System::Com::SafeArrayGetElement(
                            *array,
                            indices.as_ptr(),
                            &mut variant,
                        );

                        if hr == S_OK {
                            let cell_value = self.variant_to_cell_value(&variant);
                            row_data.push(cell_value);
                        } else {
                            row_data.push(CellValue::Empty);
                        }
                    }
                    data_rows.push(row_data);
                }

                Ok(SpreadsheetData { headers, rows: data_rows })
            }
            _ => Err(VectrillError::ComError("Unsupported variant type".to_string())),
        }
    }

    /// Convert VARIANT to cell value
    unsafe fn variant_to_cell_value(&self, variant: &VARIANT) -> CellValue {
        match variant.Anonymous.Anonymous.Anonymous.vt {
            VARENUM::VT_BSTR => {
                let bstr = variant.Anonymous.Anonymous.Anonymous.Anonymous.Anonymous.bstrVal;
                if !bstr.is_null() {
                    let string = String::from_utf16_lossy(std::slice::from_raw_parts(
                        bstr as *const u16,
                        windows::Win32::System::Com::SysStringLen(bstr) as usize,
                    ));
                    CellValue::String(string)
                } else {
                    CellValue::Empty
                }
            }
            VARENUM::VT_R4 | VARENUM::VT_R8 => {
                CellValue::Number(variant.Anonymous.Anonymous.Anonymous.Anonymous.dblVal)
            }
            VARENUM::VT_BOOL => {
                CellValue::Boolean(variant.Anonymous.Anonymous.Anonymous.Anonymous.boolVal != 0)
            }
            VARENUM::VT_EMPTY | VARENUM::VT_NULL => CellValue::Empty,
            _ => CellValue::Error("Unsupported type".to_string()),
        }
    }

    /// Write transformation results back to Excel
    unsafe fn write_results_to_excel(&self, response: &TransformResponse) -> Result<()> {
        let excel_app = self.excel_app.as_ref()
            .ok_or_else(|| VectrillError::ComError("Excel not initialized".to_string()))?;

        // Get active worksheet
        let sheets = self.get_excel_property(excel_app, "Worksheets")?;
        let active_sheet = self.get_excel_property(&sheets, "ActiveSheet")?;

        // Find next available column
        let start_col = self.find_next_available_column(&active_sheet)?;

        // Write headers
        for (col_idx, header) in response.data.headers.iter().enumerate() {
            let cell_address = format!("{}1", self.index_to_column(start_col + col_idx));
            let cell = self.get_excel_range(&active_sheet, &cell_address)?;
            self.set_excel_value(&cell, &CellValue::String(header.clone()))?;
        }

        // Write data rows
        for (row_idx, row) in response.data.rows.iter().enumerate() {
            for (col_idx, cell_value) in row.iter().enumerate() {
                let cell_address = format!("{}{}", 
                    self.index_to_column(start_col + col_idx), 
                    row_idx + 2
                );
                let cell = self.get_excel_range(&active_sheet, &cell_address)?;
                self.set_excel_value(&cell, cell_value)?;
            }
        }

        // Auto-fit columns
        let used_range = self.get_excel_property(&active_sheet, "UsedRange")?;
        let _ = self.invoke_excel_method(&used_range, "AutoFit", &[]);

        Ok(())
    }

    /// Find next available column for output
    unsafe fn find_next_available_column(&self, sheet: &IDispatch) -> Result<u32> {
        let used_range = self.get_excel_property(sheet, "UsedRange")?;
        let columns = self.get_excel_property(&used_range, "Columns")?;
        let count = self.get_excel_property(&columns, "Count")?;

        match self.variant_to_cell_value(&count) {
            CellValue::Number(n) => Ok(n as u32),
            _ => Ok(1), // Start from column A if no columns used
        }
    }

    /// Get Excel range object
    unsafe fn get_excel_range(&self, sheet: &IDispatch, address: &str) -> Result<IDispatch> {
        let range = self.invoke_excel_method(sheet, "Range", &[address])?;
        Ok(range)
    }

    /// Set Excel cell value
    unsafe fn set_excel_value(&self, cell: &IDispatch, value: &CellValue) -> Result<()> {
        let variant = self.cell_value_to_variant(value);
        let _ = self.invoke_excel_method(cell, "SetValue", &[variant])?;
        Ok(())
    }

    /// Invoke Excel method
    unsafe fn invoke_excel_method(&self, obj: &IDispatch, method: &str, args: &[VARIANT]) -> Result<IDispatch> {
        let method_wide = U16CString::from_str(method);
        let mut disp_id = [0u16; 1];
        let mut result = VARIANT::default();

        let hr = obj.GetIDsOfNames(
            &PCWSTR(method_wide.as_ptr()),
            ptr::addr_of_mut!(disp_id),
            ptr::null_mut(),
        );

        if hr != S_OK {
            return Err(VectrillError::ComError(format!("Failed to get dispatch ID for {}", method)));
        }

        let mut dispparams = windows::Win32::System::Ole::DISPPARAMS {
            rgvarg: args.as_ptr(),
            rgdispidNamedArgs: ptr::null(),
            cArgs: args.len() as u32,
            cNamedArgs: 0,
        };

        let hr = obj.Invoke(
            disp_id[0],
            &dispparams,
            w::LOCALE_USER_DEFAULT,
            &mut result,
            ptr::null_mut(),
        );

        if hr != S_OK {
            return Err(VectrillError::ComError(format!("Failed to invoke method {}", method)));
        }

        match result.Anonymous.Anonymous.Anonymous.vt {
            VARENUM::VT_DISPATCH => {
                Ok(*result.Anonymous.Anonymous.Anonymous.Anonymous.pdispVal)
            }
            _ => Err(VectrillError::ComError("Method did not return dispatch object".to_string())),
        }
    }

    /// Convert cell value to VARIANT
    unsafe fn cell_value_to_variant(&self, value: &CellValue) -> VARIANT {
        match value {
            CellValue::String(s) => {
                let bstr = windows::Win32::System::Com::SysAllocString(
                    windows::core::PCWSTR(s.encode_utf16().as_ptr()),
                    s.len() as u32,
                );
                VARIANT {
                    Anonymous: windows::Win32::System::Ole::VARIANT_0 {
                        Anonymous: windows::Win32::System::Ole::VARIANT_0_0_0 {
                            vt: VARENUM::VT_BSTR,
                            Anonymous: windows::Win32::System::Ole::VARIANT_0_0_0_0 {
                                bstrVal: bstr,
                            },
                        },
                    },
                }
            }
            CellValue::Number(n) => VARIANT {
                Anonymous: windows::Win32::System::Ole::VARIANT_0 {
                    Anonymous: windows::Win32::System::Ole::VARIANT_0_0_0 {
                        vt: VARENUM::VT_R8,
                        Anonymous: windows::Win32::System::Ole::VARIANT_0_0_0_0 {
                            dblVal: *n,
                        },
                    },
                },
            }
            CellValue::Boolean(b) => VARIANT {
                Anonymous: windows::Win32::System::Ole::VARIANT_0 {
                    Anonymous: windows::Win32::System::Ole::VARIANT_0_0_0 {
                        vt: VARENUM::VT_BOOL,
                        Anonymous: windows::Win32::System::Ole::VARIANT_0_0_0_0 {
                            boolVal: if *b { -1 } else { 0 },
                        },
                    },
                },
            }
            CellValue::Empty => VARIANT {
                Anonymous: windows::Win32::System::Ole::VARIANT_0 {
                    Anonymous: windows::Win32::System::Ole::VARIANT_0_0_0 {
                        vt: VARENUM::VT_EMPTY,
                        Anonymous: windows::Win32::System::Ole::VARIANT_0_0_0_0 { },
                    },
                },
            }
            CellValue::Error(s) => {
                let bstr = windows::Win32::System::Com::SysAllocString(
                    windows::core::PCWSTR(s.encode_utf16().as_ptr()),
                    s.len() as u32,
                );
                VARIANT {
                    Anonymous: windows::Win32::System::Ole::VARIANT_0 {
                        Anonymous: windows::Win32::System::Ole::VARIANT_0_0_0 {
                            vt: VARENUM::VT_ERROR,
                            Anonymous: windows::Win32::System::Ole::VARIANT_0_0_0_0 {
                                scode: 0, // Generic error
                            },
                        },
                    },
                }
            }
        }
    }

    /// Convert column index to column letter
    fn index_to_column(&self, index: u32) -> String {
        let mut column = String::new();
        let mut i = index;
        
        loop {
            let remainder = i % 26;
            column.push(char::from(b'A' + remainder as u8));
            i /= 26;
            if i == 0 {
                break;
            }
        }
        
        column.chars().rev().collect()
    }

    /// Show message box in Excel
    unsafe fn show_message(&self, title: &str, message: &str) -> Result<()> {
        let title_wide = U16CString::from_str(title);
        let message_wide = U16CString::from_str(message);
        
        MessageBoxW(
            ptr::null_mut(),
            PCWSTR(message_wide.as_ptr()),
            PCWSTR(title_wide.as_ptr()),
            windows::Win32::UI::WindowsAndMessaging::MESSAGEBOX_OK,
        );

        Ok(())
    }
}

/// COM interface implementation for Excel add-in
#[no_mangle]
pub extern "stdcall" fn DllMain(
    _hinst: windows::Win32::Foundation::HINSTANCE,
    _reason: u32,
    _reserved: *mut std::ffi::c_void,
) -> bool {
    true
}

/// Excel add-in entry point
#[no_mangle]
pub extern "stdcall" fn VectrillAddinEntry() -> HRESULT {
    // Initialize COM
    unsafe {
        let hr = windows::Win32::System::Com::CoInitializeEx(
            ptr::null_mut(),
            windows::Win32::System::Com::COINIT_APARTMENTTHREADED,
        );
        
        if hr != S_OK {
            return E_FAIL;
        }
    }

    S_OK
}

/// Excel add-in cleanup
#[no_mangle]
pub extern "stdcall" fn VectrillAddinCleanup() -> HRESULT {
    unsafe {
        windows::Win32::System::Com::CoUninitialize();
    }
    S_OK
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_addin_config_default() {
        let config = AddinConfig::default();
        assert_eq!(config.server_url, "http://localhost:8080");
        assert_eq!(config.max_rows, 10000);
        assert!(!config.enable_realtime);
        assert!(config.auth_token.is_none());
        assert!(config.default_template.is_none());
    }

    #[test]
    fn test_api_client_creation() {
        let config = AddinConfig::default();
        let client = VectrillApiClient::new(&config);
        assert_eq!(client.server_url, "http://localhost:8080");
        assert!(client.auth_token.is_none());
    }

    #[test]
    fn test_column_index_conversion() {
        let addin = ExcelAddin::new(AddinConfig::default());
        
        assert_eq!(addin.index_to_column(0), "A");
        assert_eq!(addin.index_to_column(25), "Z");
        assert_eq!(addin.index_to_column(26), "AA");
        assert_eq!(addin.index_to_column(51), "AZ");
    }

    #[test]
    fn test_cell_value_conversion() {
        let addin = ExcelAddin::new(AddinConfig::default());
        
        // Test string conversion
        let string_val = CellValue::String("test".to_string());
        let variant = unsafe { addin.cell_value_to_variant(&string_val) };
        unsafe {
            assert_eq!(variant.Anonymous.Anonymous.Anonymous.vt, VARENUM::VT_BSTR);
        }
        
        // Test number conversion
        let number_val = CellValue::Number(42.5);
        let variant = unsafe { addin.cell_value_to_variant(&number_val) };
        unsafe {
            assert_eq!(variant.Anonymous.Anonymous.Anonymous.vt, VARENUM::VT_R8);
        }
    }
}
