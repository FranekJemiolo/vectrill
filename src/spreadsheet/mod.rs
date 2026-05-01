//! Spreadsheet Integration Module
//!
//! This module provides APIs and utilities for integrating Vectrill with
//! spreadsheet applications like Excel and Google Sheets.

pub mod api;
pub mod data_bridge;
pub mod templates;
pub mod utils;

// Re-export main components
pub use api::{
    DataType, OperationType, SpreadsheetAPI, SpreadsheetData, SpreadsheetRequest,
    SpreadsheetResponse,
};
pub use data_bridge::DataBridge;
pub use templates::{TemplateManager, TransformationTemplate};
pub use utils::{CellReference, RangeParser};
