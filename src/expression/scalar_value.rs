//! Scalar values for literals

use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;

/// Scalar value representation
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ScalarValue {
    Null,
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Utf8(String),
    Timestamp(i64),
    Date(i32),
}

impl ScalarValue {
    /// Get the data type of this scalar value
    pub fn data_type(&self) -> crate::expression::ExprType {
        match self {
            ScalarValue::Null => crate::expression::ExprType::Null,
            ScalarValue::Boolean(_) => crate::expression::ExprType::Boolean,
            ScalarValue::Int8(_) => crate::expression::ExprType::Int8,
            ScalarValue::Int16(_) => crate::expression::ExprType::Int16,
            ScalarValue::Int32(_) => crate::expression::ExprType::Int32,
            ScalarValue::Int64(_) => crate::expression::ExprType::Int64,
            ScalarValue::UInt8(_) => crate::expression::ExprType::UInt8,
            ScalarValue::UInt16(_) => crate::expression::ExprType::UInt16,
            ScalarValue::UInt32(_) => crate::expression::ExprType::UInt32,
            ScalarValue::UInt64(_) => crate::expression::ExprType::UInt64,
            ScalarValue::Float32(_) => crate::expression::ExprType::Float32,
            ScalarValue::Float64(_) => crate::expression::ExprType::Float64,
            ScalarValue::Utf8(_) => crate::expression::ExprType::Utf8,
            ScalarValue::Timestamp(_) => crate::expression::ExprType::Timestamp,
            ScalarValue::Date(_) => crate::expression::ExprType::Date,
        }
    }

    /// Check if this value is null
    pub fn is_null(&self) -> bool {
        matches!(self, ScalarValue::Null)
    }

    /// Convert to Arrow array (single element)
    pub fn to_array(&self) -> arrow::array::ArrayRef {
        match self {
            ScalarValue::Null => {
                Arc::new(arrow::array::new_null_array(&arrow::datatypes::DataType::Null, 1))
            }
            ScalarValue::Boolean(value) => {
                Arc::new(arrow::array::BooleanArray::from(vec![*value]))
            }
            ScalarValue::Int8(value) => {
                Arc::new(arrow::array::Int8Array::from(vec![*value]))
            }
            ScalarValue::Int16(value) => {
                Arc::new(arrow::array::Int16Array::from(vec![*value]))
            }
            ScalarValue::Int32(value) => {
                Arc::new(arrow::array::Int32Array::from(vec![*value]))
            }
            ScalarValue::Int64(value) => {
                Arc::new(arrow::array::Int64Array::from(vec![*value]))
            }
            ScalarValue::UInt8(value) => {
                Arc::new(arrow::array::UInt8Array::from(vec![*value]))
            }
            ScalarValue::UInt16(value) => {
                Arc::new(arrow::array::UInt16Array::from(vec![*value]))
            }
            ScalarValue::UInt32(value) => {
                Arc::new(arrow::array::UInt32Array::from(vec![*value]))
            }
            ScalarValue::UInt64(value) => {
                Arc::new(arrow::array::UInt64Array::from(vec![*value]))
            }
            ScalarValue::Float32(value) => {
                Arc::new(arrow::array::Float32Array::from(vec![*value]))
            }
            ScalarValue::Float64(value) => {
                Arc::new(arrow::array::Float64Array::from(vec![*value]))
            }
            ScalarValue::Utf8(value) => {
                Arc::new(arrow::array::StringArray::from(vec![value.as_str()]))
            }
            ScalarValue::Timestamp(value) => {
                Arc::new(arrow::array::TimestampMicrosecondArray::from(vec![*value]))
            }
            ScalarValue::Date(value) => {
                Arc::new(arrow::array::Date32Array::from(vec![*value]))
            }
        }
    }
}

// Conversion from basic types
impl From<bool> for ScalarValue {
    fn from(value: bool) -> Self {
        ScalarValue::Boolean(value)
    }
}

impl From<i8> for ScalarValue {
    fn from(value: i8) -> Self {
        ScalarValue::Int8(value)
    }
}

impl From<i16> for ScalarValue {
    fn from(value: i16) -> Self {
        ScalarValue::Int16(value)
    }
}

impl From<i32> for ScalarValue {
    fn from(value: i32) -> Self {
        ScalarValue::Int32(value)
    }
}

impl From<i64> for ScalarValue {
    fn from(value: i64) -> Self {
        ScalarValue::Int64(value)
    }
}

impl From<u8> for ScalarValue {
    fn from(value: u8) -> Self {
        ScalarValue::UInt8(value)
    }
}

impl From<u16> for ScalarValue {
    fn from(value: u16) -> Self {
        ScalarValue::UInt16(value)
    }
}

impl From<u32> for ScalarValue {
    fn from(value: u32) -> Self {
        ScalarValue::UInt32(value)
    }
}

impl From<u64> for ScalarValue {
    fn from(value: u64) -> Self {
        ScalarValue::UInt64(value)
    }
}

impl From<f32> for ScalarValue {
    fn from(value: f32) -> Self {
        ScalarValue::Float32(value)
    }
}

impl From<f64> for ScalarValue {
    fn from(value: f64) -> Self {
        ScalarValue::Float64(value)
    }
}

impl From<String> for ScalarValue {
    fn from(value: String) -> Self {
        ScalarValue::Utf8(value)
    }
}

impl From<&str> for ScalarValue {
    fn from(value: &str) -> Self {
        ScalarValue::Utf8(value.to_string())
    }
}

impl fmt::Display for ScalarValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ScalarValue::Null => write!(f, "NULL"),
            ScalarValue::Boolean(v) => write!(f, "{}", v),
            ScalarValue::Int8(v) => write!(f, "{}", v),
            ScalarValue::Int16(v) => write!(f, "{}", v),
            ScalarValue::Int32(v) => write!(f, "{}", v),
            ScalarValue::Int64(v) => write!(f, "{}", v),
            ScalarValue::UInt8(v) => write!(f, "{}", v),
            ScalarValue::UInt16(v) => write!(f, "{}", v),
            ScalarValue::UInt32(v) => write!(f, "{}", v),
            ScalarValue::UInt64(v) => write!(f, "{}", v),
            ScalarValue::Float32(v) => write!(f, "{}", v),
            ScalarValue::Float64(v) => write!(f, "{}", v),
            ScalarValue::Utf8(v) => write!(f, "{}", v),
            ScalarValue::Timestamp(v) => write!(f, "{}", v),
            ScalarValue::Date(v) => write!(f, "{}", v),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scalar_value_creation() {
        let bool_val = ScalarValue::Boolean(true);
        assert_eq!(bool_val.data_type(), crate::expression::ExprType::Boolean);

        let int_val = ScalarValue::Int32(42);
        assert_eq!(int_val.data_type(), crate::expression::ExprType::Int32);

        let string_val = ScalarValue::Utf8("test".to_string());
        assert_eq!(string_val.data_type(), crate::expression::ExprType::Utf8);
    }

    #[test]
    fn test_scalar_value_conversions() {
        let bool_val: ScalarValue = true.into();
        assert_eq!(bool_val, ScalarValue::Boolean(true));

        let int_val: ScalarValue = 42i64.into();
        assert_eq!(int_val, ScalarValue::Int64(42));

        let string_val: ScalarValue = "test".into();
        assert_eq!(string_val, ScalarValue::Utf8("test".to_string()));
    }

    #[test]
    fn test_scalar_value_display() {
        assert_eq!(ScalarValue::Boolean(true).to_string(), "true");
        assert_eq!(ScalarValue::Int32(42).to_string(), "42");
        assert_eq!(ScalarValue::Utf8("test".to_string()).to_string(), "test");
        assert_eq!(ScalarValue::Null.to_string(), "NULL");
    }

    #[test]
    fn test_to_array() {
        let val = ScalarValue::Int32(42);
        let array = val.to_array();
        assert_eq!(array.len(), 1);
        
        let int_array = array.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap();
        assert_eq!(int_array.value(0), 42);
    }
}
