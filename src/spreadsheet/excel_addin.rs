//! Excel COM Add-in for Vectrill (Windows-specific)
//!
//! Provides integration with Microsoft Excel through COM automation,
//! allowing users to access Vectrill's data processing capabilities directly from Excel.

#[cfg(windows)]
use crate::error::Result;

/// Excel COM Add-in for Vectrill
#[cfg(windows)]
pub struct ExcelAddin {
    /// COM application instance
    app: Option<windows::core::IUnknown>,
}

#[cfg(windows)]
impl ExcelAddin {
    /// Create new Excel add-in instance
    pub fn new() -> Result<Self> {
        Ok(Self { app: None })
    }

    /// Initialize COM connection to Excel
    pub fn initialize(&mut self) -> Result<()> {
        // Mock implementation for now
        println!("Initializing Excel COM connection");
        Ok(())
    }

    /// Get selected range from Excel
    pub fn get_selected_range(&self) -> Result<Vec<Vec<String>>> {
        // Mock implementation for now
        Ok(vec![
            vec!["Header1".to_string(), "Header2".to_string()],
            vec!["Data1".to_string(), "Data2".to_string()],
        ])
    }

    /// Update Excel range with data
    pub fn update_range(&self, range: &str, data: Vec<Vec<String>>) -> Result<()> {
        // Mock implementation for now
        println!("Updating Excel range {} with {} rows", range, data.len());
        Ok(())
    }

    /// Show message in Excel
    pub fn show_message(&self, message: &str) -> Result<()> {
        // Mock implementation for now
        println!("Excel message: {}", message);
        Ok(())
    }
}

#[cfg(not(windows))]
pub struct ExcelAddin;

#[cfg(not(windows))]
impl ExcelAddin {
    pub fn new() -> Result<Self> {
        Err(crate::error::VectrillError::Connector(
            "Excel add-in is only supported on Windows".to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(windows)]
    #[test]
    fn test_excel_addin_creation() {
        let addin = ExcelAddin::new();
        assert!(addin.is_ok());
    }

    #[cfg(not(windows))]
    #[test]
    fn test_excel_addin_not_supported() {
        let addin = ExcelAddin::new();
        assert!(addin.is_err());
    }
}
