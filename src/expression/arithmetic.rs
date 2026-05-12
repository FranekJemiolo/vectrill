//! Arithmetic operations for physical expressions

use arrow::array::*;
use arrow::datatypes::DataType;
use std::sync::Arc;
use crate::expression::physical::ExpressionError;

/// Arithmetic operations with broadcasting support
pub struct ArithmeticOps;

impl ArithmeticOps {
    /// Addition with broadcasting support
    pub fn add(left: &ArrayRef, right: &ArrayRef) -> Result<Arc<dyn arrow::array::Array>, ExpressionError> {
        match (left.data_type(), right.data_type()) {
            (DataType::Int64, DataType::Int64) => {
                let left_ints = left.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_ints = right.as_any().downcast_ref::<Int64Array>().unwrap();
                
                let result = Self::add_int64(left_ints, right_ints)?;
                Ok(Arc::new(result) as Arc<dyn arrow::array::Array>)
            }
            (DataType::Float64, DataType::Float64) => {
                let left_floats = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_floats = right.as_any().downcast_ref::<Float64Array>().unwrap();
                
                let result = Self::add_float64(left_floats, right_floats)?;
                Ok(Arc::new(result) as Arc<dyn arrow::array::Array>)
            }
            _ => {
                Err(ExpressionError::TypeMismatch {
                    expected: "Int64 or Float64".to_string(),
                    actual: format!("{:?} + {:?}", left.data_type(), right.data_type()),
                })
            }
        }
    }

    /// Subtraction with broadcasting support
    pub fn subtract(left: &ArrayRef, right: &ArrayRef) -> Result<Arc<dyn arrow::array::Array>, ExpressionError> {
        match (left.data_type(), right.data_type()) {
            (DataType::Int64, DataType::Int64) => {
                let left_ints = left.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_ints = right.as_any().downcast_ref::<Int64Array>().unwrap();
                
                let result = Self::subtract_int64(left_ints, right_ints)?;
                Ok(Arc::new(result) as Arc<dyn arrow::array::Array>)
            }
            (DataType::Float64, DataType::Float64) => {
                let left_floats = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_floats = right.as_any().downcast_ref::<Float64Array>().unwrap();
                
                let result = Self::subtract_float64(left_floats, right_floats)?;
                Ok(Arc::new(result) as Arc<dyn arrow::array::Array>)
            }
            _ => {
                Err(ExpressionError::TypeMismatch {
                    expected: "Int64 or Float64".to_string(),
                    actual: format!("{:?} - {:?}", left.data_type(), right.data_type()),
                })
            }
        }
    }

    /// Multiplication with broadcasting support
    pub fn multiply(left: &ArrayRef, right: &ArrayRef) -> Result<Arc<dyn arrow::array::Array>, ExpressionError> {
        match (left.data_type(), right.data_type()) {
            (DataType::Int64, DataType::Int64) => {
                let left_ints = left.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_ints = right.as_any().downcast_ref::<Int64Array>().unwrap();
                
                let result = Self::multiply_int64(left_ints, right_ints)?;
                Ok(Arc::new(result) as Arc<dyn arrow::array::Array>)
            }
            (DataType::Float64, DataType::Float64) => {
                let left_floats = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_floats = right.as_any().downcast_ref::<Float64Array>().unwrap();
                
                let result = Self::multiply_float64(left_floats, right_floats)?;
                Ok(Arc::new(result) as Arc<dyn arrow::array::Array>)
            }
            _ => {
                Err(ExpressionError::TypeMismatch {
                    expected: "Int64 or Float64".to_string(),
                    actual: format!("{:?} * {:?}", left.data_type(), right.data_type()),
                })
            }
        }
    }

    /// Division with broadcasting support
    pub fn divide(left: &ArrayRef, right: &ArrayRef) -> Result<Arc<dyn arrow::array::Array>, ExpressionError> {
        match (left.data_type(), right.data_type()) {
            (DataType::Float64, DataType::Float64) => {
                let left_floats = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_floats = right.as_any().downcast_ref::<Float64Array>().unwrap();
                
                let result = Self::divide_float64(left_floats, right_floats)?;
                Ok(Arc::new(result) as Arc<dyn arrow::array::Array>)
            }
            _ => {
                Err(ExpressionError::TypeMismatch {
                    expected: "Int64 or Float64".to_string(),
                    actual: format!("{:?} / {:?}", left.data_type(), right.data_type()),
                })
            }
        }
    }

    /// Int64 addition with broadcasting
    fn add_int64(left: &Int64Array, right: &Int64Array) -> Result<Int64Array, ExpressionError> {
        let len = left.len().max(right.len());
        let mut result = Vec::with_capacity(len);
        
        if left.len() == right.len() {
            for i in 0..len {
                result.push(left.value(i) + right.value(i));
            }
        } else if left.len() == 1 {
            let left_val = left.value(0);
            for i in 0..right.len() {
                result.push(left_val + right.value(i));
            }
        } else if right.len() == 1 {
            let right_val = right.value(0);
            for i in 0..left.len() {
                result.push(left.value(i) + right_val);
            }
        } else {
            return Err(ExpressionError::InvalidOperation {
                op: "add".to_string(),
                left_type: "Int64".to_string(),
                right_type: "Int64".to_string(),
            });
        }
        
        Ok(Int64Array::from(result))
    }

    /// Float64 addition with broadcasting
    fn add_float64(left: &Float64Array, right: &Float64Array) -> Result<Float64Array, ExpressionError> {
        let len = left.len().max(right.len());
        let mut result = Vec::with_capacity(len);
        
        if left.len() == right.len() {
            for i in 0..len {
                result.push(left.value(i) + right.value(i));
            }
        } else if left.len() == 1 {
            let left_val = left.value(0);
            for i in 0..right.len() {
                result.push(left_val + right.value(i));
            }
        } else if right.len() == 1 {
            let right_val = right.value(0);
            for i in 0..left.len() {
                result.push(left.value(i) + right_val);
            }
        } else {
            return Err(ExpressionError::InvalidOperation {
                op: "add".to_string(),
                left_type: "Float64".to_string(),
                right_type: "Float64".to_string(),
            });
        }
        
        Ok(Float64Array::from(result))
    }

    /// Int64 subtraction with broadcasting
    fn subtract_int64(left: &Int64Array, right: &Int64Array) -> Result<Int64Array, ExpressionError> {
        let len = left.len().max(right.len());
        let mut result = Vec::with_capacity(len);
        
        if left.len() == right.len() {
            for i in 0..len {
                result.push(left.value(i) - right.value(i));
            }
        } else if left.len() == 1 {
            let left_val = left.value(0);
            for i in 0..right.len() {
                result.push(left_val - right.value(i));
            }
        } else if right.len() == 1 {
            let right_val = right.value(0);
            for i in 0..left.len() {
                result.push(left.value(i) - right_val);
            }
        } else {
            return Err(ExpressionError::InvalidOperation {
                op: "subtract".to_string(),
                left_type: "Int64".to_string(),
                right_type: "Int64".to_string(),
            });
        }
        
        Ok(Int64Array::from(result))
    }

    /// Float64 subtraction with broadcasting
    fn subtract_float64(left: &Float64Array, right: &Float64Array) -> Result<Float64Array, ExpressionError> {
        let len = left.len().max(right.len());
        let mut result = Vec::with_capacity(len);
        
        if left.len() == right.len() {
            for i in 0..len {
                result.push(left.value(i) - right.value(i));
            }
        } else if left.len() == 1 {
            let left_val = left.value(0);
            for i in 0..right.len() {
                result.push(left_val - right.value(i));
            }
        } else if right.len() == 1 {
            let right_val = right.value(0);
            for i in 0..left.len() {
                result.push(left.value(i) - right_val);
            }
        } else {
            return Err(ExpressionError::InvalidOperation {
                op: "subtract".to_string(),
                left_type: "Float64".to_string(),
                right_type: "Float64".to_string(),
            });
        }
        
        Ok(Float64Array::from(result))
    }

    /// Int64 multiplication with broadcasting
    fn multiply_int64(left: &Int64Array, right: &Int64Array) -> Result<Int64Array, ExpressionError> {
        let len = left.len().max(right.len());
        let mut result = Vec::with_capacity(len);
        
        if left.len() == right.len() {
            for i in 0..len {
                result.push(left.value(i) * right.value(i));
            }
        } else if left.len() == 1 {
            let left_val = left.value(0);
            for i in 0..right.len() {
                result.push(left_val * right.value(i));
            }
        } else if right.len() == 1 {
            let right_val = right.value(0);
            for i in 0..left.len() {
                result.push(left.value(i) * right_val);
            }
        } else {
            return Err(ExpressionError::InvalidOperation {
                op: "multiply".to_string(),
                left_type: "Int64".to_string(),
                right_type: "Int64".to_string(),
            });
        }
        
        Ok(Int64Array::from(result))
    }

    /// Float64 multiplication with broadcasting
    fn multiply_float64(left: &Float64Array, right: &Float64Array) -> Result<Float64Array, ExpressionError> {
        let len = left.len().max(right.len());
        let mut result = Vec::with_capacity(len);
        
        if left.len() == right.len() {
            for i in 0..len {
                result.push(left.value(i) * right.value(i));
            }
        } else if left.len() == 1 {
            let left_val = left.value(0);
            for i in 0..right.len() {
                result.push(left_val * right.value(i));
            }
        } else if right.len() == 1 {
            let right_val = right.value(0);
            for i in 0..left.len() {
                result.push(left.value(i) * right_val);
            }
        } else {
            return Err(ExpressionError::InvalidOperation {
                op: "multiply".to_string(),
                left_type: "Float64".to_string(),
                right_type: "Float64".to_string(),
            });
        }
        
        Ok(Float64Array::from(result))
    }

    /// Float64 division with broadcasting
    fn divide_float64(left: &Float64Array, right: &Float64Array) -> Result<Float64Array, ExpressionError> {
        let len = left.len().max(right.len());
        let mut result = Vec::with_capacity(len);
        
        if left.len() == right.len() {
            for i in 0..len {
                let right_val = right.value(i);
                if right_val != 0.0 {
                    result.push(left.value(i) / right_val);
                } else {
                    return Err(ExpressionError::InvalidOperation {
                        op: "divide".to_string(),
                        left_type: "Float64".to_string(),
                        right_type: "Float64".to_string(),
                    });
                }
            }
        } else if left.len() == 1 {
            let left_val = left.value(0);
            for i in 0..right.len() {
                let right_val = right.value(i);
                if right_val != 0.0 {
                    result.push(left_val / right_val);
                } else {
                    return Err(ExpressionError::InvalidOperation {
                        op: "divide".to_string(),
                        left_type: "Float64".to_string(),
                        right_type: "Float64".to_string(),
                    });
                }
            }
        } else if right.len() == 1 {
            let right_val = right.value(0);
            if right_val != 0.0 {
                for i in 0..left.len() {
                    result.push(left.value(i) / right_val);
                }
            } else {
                return Err(ExpressionError::InvalidOperation {
                    op: "divide".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: "Float64".to_string(),
                });
            }
        } else {
            return Err(ExpressionError::InvalidOperation {
                op: "divide".to_string(),
                left_type: "Float64".to_string(),
                right_type: "Float64".to_string(),
            });
        }
        
        Ok(Float64Array::from(result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_arithmetic_add() {
        let left_data = vec![1, 2, 3];
        let right_data = vec![4, 5, 6];
        let left_array = Int64Array::from(left_data);
        let right_array = Int64Array::from(right_data);
        let left_ref = Arc::new(left_array) as ArrayRef;
        let right_ref = Arc::new(right_array) as ArrayRef;
        
        let result = ArithmeticOps::add(&left_ref, &right_ref).unwrap();
        let result_array = result.as_any().downcast_ref::<Int64Array>().unwrap();
        
        assert_eq!(result_array.len(), 3);
        assert_eq!(result_array.value(0), 5);
        assert_eq!(result_array.value(1), 7);
        assert_eq!(result_array.value(2), 9);
    }

    #[test]
    fn test_arithmetic_broadcasting() {
        let left_data = vec![2]; // scalar
        let right_data = vec![1, 2, 3];
        let left_array = Int64Array::from(left_data);
        let right_array = Int64Array::from(right_data);
        let left_ref = Arc::new(left_array) as ArrayRef;
        let right_ref = Arc::new(right_array) as ArrayRef;
        
        let result = ArithmeticOps::multiply(&left_ref, &right_ref).unwrap();
        let result_array = result.as_any().downcast_ref::<Int64Array>().unwrap();
        
        assert_eq!(result_array.len(), 3);
        assert_eq!(result_array.value(0), 2);
        assert_eq!(result_array.value(1), 4);
        assert_eq!(result_array.value(2), 6);
    }
}
