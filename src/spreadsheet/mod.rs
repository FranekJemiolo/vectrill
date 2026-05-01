//! Spreadsheet Integration Module
//!
//! This module provides APIs and utilities for integrating Vectrill with
//! spreadsheet applications like Excel and Google Sheets.

pub mod api;
pub mod data_bridge;
pub mod templates;
pub mod utils;

#[cfg(windows)]
pub mod excel_addin;

#[cfg(not(windows))]
pub mod google_sheets;

pub mod real_time;

// Re-export main components
pub use api::{
    DataType, OperationType, SpreadsheetAPI, SpreadsheetData, SpreadsheetRequest,
    SpreadsheetResponse,
};
pub use data_bridge::DataBridge;
pub use templates::{TemplateManager, TransformationTemplate};
pub use utils::{CellReference, RangeParser};
