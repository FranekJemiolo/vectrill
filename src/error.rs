//! Error types for Vectrill

use thiserror::Error;

/// Result type alias for Vectrill operations
pub type Result<T> = std::result::Result<T, VectrillError>;

/// Main error type for Vectrill
#[derive(Error, Debug)]
pub enum VectrillError {
    /// Arrow-related errors
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// I/O errors
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// Invalid configuration
    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    /// Invalid schema
    #[error("Invalid schema: {0}")]
    InvalidSchema(String),

    /// Invalid expression
    #[error("Invalid expression: {0}")]
    InvalidExpression(String),

    /// Expression error
    #[error("Expression error: {0}")]
    ExpressionError(String),

    /// Arrow error (string wrapper)
    #[error("Arrow error: {0}")]
    ArrowError(String),

    /// Execution error
    #[error("Execution error: {0}")]
    Execution(String),

    /// Connector error
    #[error("Connector error: {0}")]
    Connector(String),

    /// Serialization error
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Generic error
    #[error("{0}")]
    Generic(String),
}
