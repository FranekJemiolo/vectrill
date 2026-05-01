//! Spreadsheet Utilities
//! 
//! Utility functions for working with spreadsheet data and references.

use crate::error::VectrillError;
use std::collections::HashMap;

/// Cell reference utilities
#[derive(Debug, Clone)]
pub struct CellReference {
    /// Column letter(s)
    pub column: String,
    /// Row number
    pub row: u32,
    /// Sheet name (optional)
    pub sheet: Option<String>,
}

impl CellReference {
    /// Parse cell reference from string (e.g., "A1", "Sheet1!A1")
    pub fn parse(reference: &str) -> Result<Self, VectrillError> {
        let (sheet_part, cell_part) = if reference.contains('!') {
            let parts: Vec<&str> = reference.split('!').collect();
            if parts.len() != 2 {
                return Err(VectrillError::InvalidConfig(format!("Invalid cell reference: {}", reference)));
            }
            (Some(parts[0]), parts[1])
        } else {
            (None, reference)
        };
        
        let (column, row) = Self::parse_cell_part(cell_part)?;
        
        Ok(CellReference {
            column,
            row,
            sheet: sheet_part.map(|s| s.to_string()),
        })
    }
    
    /// Parse cell part (e.g., "A1" -> ("A", 1))
    fn parse_cell_part(cell_part: &str) -> Result<(String, u32), VectrillError> {
        if cell_part.is_empty() {
            return Err(VectrillError::InvalidConfig("Empty cell reference".to_string()));
        }
        
        let mut chars = cell_part.chars().peekable();
        let mut column = String::new();
        
        // Extract column letters
        while let Some(&ch) = chars.peek() {
            if ch.is_alphabetic() {
                column.push(ch);
                chars.next();
            } else {
                break;
            }
        }
        
        if column.is_empty() {
            return Err(VectrillError::InvalidConfig("Missing column in cell reference".to_string()));
        }
        
        // Extract row number
        let row_str: String = chars.collect();
        if row_str.is_empty() {
            return Err(VectrillError::InvalidConfig("Missing row in cell reference".to_string()));
        }
        
        let row = row_str.parse::<u32>()
            .map_err(|_| VectrillError::InvalidConfig("Invalid row number".to_string()))?;
        
        Ok((column, row))
    }
    
    /// Convert column letter(s) to column index (0-based)
    pub fn column_to_index(&self) -> Result<u32, VectrillError> {
        let mut index = 0u32;
        for (i, ch) in self.column.chars().enumerate() {
            if !ch.is_alphabetic() {
                return Err(VectrillError::InvalidConfig("Invalid column letter".to_string()));
            }
            let value = (ch.to_ascii_uppercase() as u8 - b'A' + 1) as u32;
            index = index * 26 + value;
        }
        Ok(index - 1) // Convert to 0-based
    }
    
    /// Convert column index (0-based) to column letter(s)
    pub fn index_to_column(index: u32) -> String {
        let mut column = String::new();
        let mut index = index + 1; // Convert to 1-based
        
        while index > 0 {
            index -= 1;
            column.push(char::from(b'A' + (index % 26) as u8));
            index /= 26;
        }
        
        column.chars().rev().collect()
    }
    
    /// Convert to string representation
    pub fn to_string(&self) -> String {
        match &self.sheet {
            Some(sheet) => format!("{}!{}{}", sheet, self.column, self.row),
            None => format!("{}{}", self.column, self.row),
        }
    }
}

/// Range reference utilities
#[derive(Debug, Clone)]
pub struct RangeReference {
    /// Start cell
    pub start: CellReference,
    /// End cell
    pub end: CellReference,
    /// Sheet name
    pub sheet: Option<String>,
}

impl RangeReference {
    /// Parse range reference from string (e.g., "A1:C10", "Sheet1!A1:C10")
    pub fn parse(reference: &str) -> Result<Self, VectrillError> {
        let (sheet_part, range_part) = if reference.contains('!') {
            let parts: Vec<&str> = reference.split('!').collect();
            if parts.len() != 2 {
                return Err(VectrillError::InvalidConfig(format!("Invalid range reference: {}", reference)));
            }
            (Some(parts[0]), parts[1])
        } else {
            (None, reference)
        };
        
        let cell_parts: Vec<&str> = range_part.split(':').collect();
        if cell_parts.len() != 2 {
            return Err(VectrillError::InvalidConfig("Range must contain start and end cells".to_string()));
        }
        
        let start = CellReference::parse(cell_parts[0])?;
        let end = CellReference::parse(cell_parts[1])?;
        
        // Validate that end cell is after start cell
        if start.column_to_index()? > end.column_to_index()? || start.row > end.row {
            return Err(VectrillError::InvalidConfig("End cell must be after start cell".to_string()));
        }
        
        Ok(RangeReference {
            start,
            end,
            sheet: sheet_part.map(|s| s.to_string()),
        })
    }
    
    /// Get the number of columns in the range
    pub fn column_count(&self) -> Result<u32, VectrillError> {
        Ok(self.end.column_to_index()? - self.start.column_to_index()? + 1)
    }
    
    /// Get the number of rows in the range
    pub fn row_count(&self) -> u32 {
        self.end.row - self.start.row + 1
    }
    
    /// Get the total number of cells in the range
    pub fn cell_count(&self) -> Result<u32, VectrillError> {
        Ok(self.column_count()? * self.row_count())
    }
    
    /// Convert to string representation
    pub fn to_string(&self) -> String {
        match &self.sheet {
            Some(sheet) => format!("{}!{}:{}", sheet, self.start.to_string(), self.end.to_string()),
            None => format!("{}:{}", self.start.to_string(), self.end.to_string()),
        }
    }
}

/// Range parser for parsing various range formats
pub struct RangeParser {
    /// Cache for parsed ranges
    cache: HashMap<String, RangeReference>,
}

impl RangeParser {
    /// Create new range parser
    pub fn new() -> Self {
        Self {
            cache: HashMap::new(),
        }
    }
    
    /// Parse range reference with caching
    pub fn parse(&mut self, reference: &str) -> Result<RangeReference, VectrillError> {
        if let Some(cached) = self.cache.get(reference) {
            return Ok(cached.clone());
        }
        
        let range = RangeReference::parse(reference)?;
        self.cache.insert(reference.to_string(), range.clone());
        Ok(range)
    }
    
    /// Clear cache
    pub fn clear_cache(&mut self) {
        self.cache.clear();
    }
}

impl Default for RangeParser {
    fn default() -> Self {
        Self::new()
    }
}

/// Formula parser utilities
pub struct FormulaParser;

impl FormulaParser {
    /// Extract cell references from a formula
    pub fn extract_cell_references(formula: &str) -> Vec<CellReference> {
        let mut references = Vec::new();
        let mut chars = formula.chars().peekable();
        let mut current = String::new();
        let mut in_reference = false;
        
        while let Some(&ch) = chars.peek() {
            if ch.is_alphabetic() && !in_reference {
                // Start of potential cell reference
                current.push(ch);
                in_reference = true;
                chars.next();
            } else if in_reference {
                if ch.is_alphanumeric() || ch == '$' {
                    current.push(ch);
                    chars.next();
                } else {
                    // End of cell reference
                    if let Ok(reference) = CellReference::parse(&current) {
                        references.push(reference);
                    }
                    current.clear();
                    in_reference = false;
                    chars.next();
                }
            } else {
                chars.next();
            }
        }
        
        // Handle last reference if formula ends with one
        if in_reference && !current.is_empty() {
            if let Ok(reference) = CellReference::parse(&current) {
                references.push(reference);
            }
        }
        
        references
    }
    
    /// Extract range references from a formula
    pub fn extract_range_references(formula: &str) -> Vec<RangeReference> {
        let mut references = Vec::new();
        let mut chars = formula.chars().peekable();
        let mut current = String::new();
        let mut in_reference = false;
        
        while let Some(&ch) = chars.peek() {
            if ch.is_alphabetic() && !in_reference {
                // Start of potential range reference
                current.push(ch);
                in_reference = true;
                chars.next();
            } else if in_reference {
                if ch.is_alphanumeric() || ch == '$' || ch == ':' || ch == '!' {
                    current.push(ch);
                    chars.next();
                } else {
                    // End of range reference
                    if current.contains(':') {
                        if let Ok(reference) = RangeReference::parse(&current) {
                            references.push(reference);
                        }
                    }
                    current.clear();
                    in_reference = false;
                    chars.next();
                }
            } else {
                chars.next();
            }
        }
        
        // Handle last reference if formula ends with one
        if in_reference && !current.is_empty() && current.contains(':') {
            if let Ok(reference) = RangeReference::parse(&current) {
                references.push(reference);
            }
        }
        
        references
    }
    
    /// Validate formula syntax (basic validation)
    pub fn validate_formula(formula: &str) -> Result<(), VectrillError> {
        if formula.is_empty() {
            return Ok(()); // Empty formula is valid
        }
        
        // Check for balanced parentheses
        let mut paren_count = 0;
        let mut chars = formula.chars();
        
        while let Some(ch) = chars.next() {
            match ch {
                '(' => paren_count += 1,
                ')' => {
                    paren_count -= 1;
                    if paren_count < 0 {
                        return Err(VectrillError::InvalidConfig("Unbalanced parentheses in formula".to_string()));
                    }
                }
                '"' => {
                    // Skip string literals
                    while let Some(next_ch) = chars.next() {
                        if next_ch == '"' {
                            break;
                        }
                    }
                }
                _ => {}
            }
        }
        
        if paren_count != 0 {
            return Err(VectrillError::InvalidConfig("Unbalanced parentheses in formula".to_string()));
        }
        
        // TODO: Add more validation rules
        Ok(())
    }
}

/// Data type utilities
pub struct DataTypeUtils;

impl DataTypeUtils {
    /// Infer data type from a sample of values
    pub fn infer_type_from_values(values: &[String]) -> crate::spreadsheet::api::DataType {
        if values.is_empty() {
            return crate::spreadsheet::api::DataType::Empty;
        }
        
        let mut type_scores = std::collections::HashMap::new();
        type_scores.insert(crate::spreadsheet::api::DataType::String, 0);
        type_scores.insert(crate::spreadsheet::api::DataType::Number, 0);
        type_scores.insert(crate::spreadsheet::api::DataType::Boolean, 0);
        type_scores.insert(crate::spreadsheet::api::DataType::Date, 0);
        type_scores.insert(crate::spreadsheet::api::DataType::Empty, 0);
        
        for value in values {
            let inferred_type = Self::infer_type_from_value(value);
            *type_scores.entry(inferred_type).or_insert(0) += 1;
        }
        
        type_scores.iter()
            .max_by_key(|(_, &score)| score)
            .map(|(t, _)| t.clone())
            .unwrap_or(crate::spreadsheet::api::DataType::String)
    }
    
    /// Infer data type from a single value
    pub fn infer_type_from_value(value: &str) -> crate::spreadsheet::api::DataType {
        if value.trim().is_empty() {
            crate::spreadsheet::api::DataType::Empty
        } else if value.parse::<f64>().is_ok() {
            crate::spreadsheet::api::DataType::Number
        } else if value.parse::<bool>().is_ok() {
            crate::spreadsheet::api::DataType::Boolean
        } else if Self::looks_like_date(value) {
            crate::spreadsheet::api::DataType::Date
        } else {
            crate::spreadsheet::api::DataType::String
        }
    }
    
    /// Check if string looks like a date
    fn looks_like_date(value: &str) -> bool {
        // Simple date detection - could be enhanced
        value.contains('/') || value.contains('-') || value.contains(':') ||
        value.to_lowercase().contains("am") || value.to_lowercase().contains("pm") ||
        value.len() == 8 && value.chars().all(|c| c.is_numeric()) // YYYYMMDD
    }
    
    /// Convert value to target type
    pub fn convert_value(value: &str, target_type: &crate::spreadsheet::api::DataType) -> Result<crate::spreadsheet::api::CellValue, VectrillError> {
        match target_type {
            crate::spreadsheet::api::DataType::String => {
                Ok(crate::spreadsheet::api::CellValue::String(value.to_string()))
            }
            crate::spreadsheet::api::DataType::Number => {
                value.parse::<f64>()
                    .map(crate::spreadsheet::api::CellValue::Number)
                    .map_err(|_| VectrillError::InvalidConfig(format!("Cannot convert '{}' to number", value)))
            }
            crate::spreadsheet::api::DataType::Boolean => {
                if let Ok(bool_val) = value.parse::<bool>() {
                    Ok(crate::spreadsheet::api::CellValue::Boolean(bool_val))
                } else if let Ok(num_val) = value.parse::<f64>() {
                    Ok(crate::spreadsheet::api::CellValue::Boolean(num_val != 0.0))
                } else {
                    let lower = value.to_lowercase();
                    match lower.as_str() {
                        "true" | "yes" | "1" | "on" => Ok(crate::spreadsheet::api::CellValue::Boolean(true)),
                        "false" | "no" | "0" | "off" => Ok(crate::spreadsheet::api::CellValue::Boolean(false)),
                        _ => Err(VectrillError::InvalidConfig(format!("Cannot convert '{}' to boolean", value))),
                    }
                }
            }
            crate::spreadsheet::api::DataType::Date => {
                // TODO: Implement proper date parsing
                Ok(crate::spreadsheet::api::CellValue::String(value.to_string()))
            }
            crate::spreadsheet::api::DataType::Empty => {
                Ok(crate::spreadsheet::api::CellValue::Empty)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_cell_reference_parsing() {
        let cell = CellReference::parse("A1").unwrap();
        assert_eq!(cell.column, "A");
        assert_eq!(cell.row, 1);
        assert_eq!(cell.sheet, None);
        
        let cell_with_sheet = CellReference::parse("Sheet1!A1").unwrap();
        assert_eq!(cell_with_sheet.column, "A");
        assert_eq!(cell_with_sheet.row, 1);
        assert_eq!(cell_with_sheet.sheet, Some("Sheet1".to_string()));
    }
    
    #[test]
    fn test_column_index_conversion() {
        let cell = CellReference::parse("A1").unwrap();
        assert_eq!(cell.column_to_index().unwrap(), 0);
        
        let cell = CellReference::parse("Z1").unwrap();
        assert_eq!(cell.column_to_index().unwrap(), 25);
        
        let cell = CellReference::parse("AA1").unwrap();
        assert_eq!(cell.column_to_index().unwrap(), 26);
    }
    
    #[test]
    fn test_index_to_column() {
        assert_eq!(CellReference::index_to_column(0), "A");
        assert_eq!(CellReference::index_to_column(25), "Z");
        assert_eq!(CellReference::index_to_column(26), "AA");
    }
    
    #[test]
    fn test_range_reference_parsing() {
        let range = RangeReference::parse("A1:C10").unwrap();
        assert_eq!(range.start.column, "A");
        assert_eq!(range.start.row, 1);
        assert_eq!(range.end.column, "C");
        assert_eq!(range.end.row, 10);
        
        assert_eq!(range.column_count().unwrap(), 3);
        assert_eq!(range.row_count(), 10);
        assert_eq!(range.cell_count().unwrap(), 30);
    }
    
    #[test]
    fn test_formula_validation() {
        assert!(FormulaParser::validate_formula("").is_ok());
        assert!(FormulaParser::validate_formula("=A1+B1").is_ok());
        assert!(FormulaParser::validate_formula("=SUM(A1:A10)").is_ok());
        assert!(FormulaParser::validate_formula("=SUM(A1:A10)").is_ok());
        
        assert!(FormulaParser::validate_formula("=SUM(A1:A10").is_err()); // Unbalanced
        assert!(FormulaParser::validate_formula("=SUM(A1:A10))").is_err()); // Unbalanced
    }
    
    #[test]
    fn test_cell_reference_extraction() {
        let references = FormulaParser::extract_cell_references("=A1+B1*C1");
        assert_eq!(references.len(), 3);
        
        let references = FormulaParser::extract_range_references("=SUM(A1:C10)");
        assert_eq!(references.len(), 1);
    }
    
    #[test]
    fn test_data_type_inference() {
        let values = vec!["1", "2", "3"];
        let data_type = DataTypeUtils::infer_type_from_values(&values);
        assert!(matches!(data_type, crate::spreadsheet::api::DataType::Number));
        
        let values = vec!["true", "false", "yes"];
        let data_type = DataTypeUtils::infer_type_from_values(&values);
        assert!(matches!(data_type, crate::spreadsheet::api::DataType::Boolean));
        
        let values = vec!["hello", "world", "test"];
        let data_type = DataTypeUtils::infer_type_from_values(&values);
        assert!(matches!(data_type, crate::spreadsheet::api::DataType::String));
    }
}
