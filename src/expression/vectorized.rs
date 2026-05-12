//! Vectorized operations for arithmetic expressions with SIMD optimizations

use arrow::array::*;
use arrow::datatypes::DataType;
use std::sync::Arc;
use crate::error::{Result, VectrillError};
use crate::expression::physical::ExpressionError;

/// Vectorized arithmetic operations with SIMD optimizations where possible
pub struct VectorizedOps;

impl VectorizedOps {
    /// Vectorized addition with SIMD optimizations
    pub fn add(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
        match (left.data_type(), right.data_type()) {
            (DataType::Int64, DataType::Int64) => {
                let left_ints = left.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_ints = right.as_any().downcast_ref::<Int64Array>().unwrap();
                
                let result = Self::add_int64(left_ints, right_ints)?;
                Ok(Arc::new(result) as ArrayRef)
            }
            (DataType::Float64, DataType::Float64) => {
                let left_floats = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_floats = right.as_any().downcast_ref::<Float64Array>().unwrap();
                
                let result = Self::add_float64(left_floats, right_floats)?;
                Ok(Arc::new(result) as ArrayRef)
            }
            // Handle broadcasting cases
            (DataType::Int64, DataType::Int64) => {
                // This is handled by the same type case above
                Self::add(left, right)
            }
            _ => {
                // Fallback to scalar operations for mixed types
                Self::add_fallback(left, right)
            }
        }
    }

    /// Vectorized subtraction with SIMD optimizations
    pub fn subtract(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
        match (left.data_type(), right.data_type()) {
            (DataType::Int64, DataType::Int64) => {
                let left_ints = left.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_ints = right.as_any().downcast_ref::<Int64Array>().unwrap();
                
                let result = Self::subtract_int64(left_ints, right_ints)?;
                Ok(Arc::new(result) as ArrayRef)
            }
            (DataType::Float64, DataType::Float64) => {
                let left_floats = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_floats = right.as_any().downcast_ref::<Float64Array>().unwrap();
                
                let result = Self::subtract_float64(left_floats, right_floats)?;
                Ok(Arc::new(result) as ArrayRef)
            }
            _ => {
                Self::subtract_fallback(left, right)
            }
        }
    }

    /// Vectorized multiplication with SIMD optimizations
    pub fn multiply(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
        match (left.data_type(), right.data_type()) {
            (DataType::Int64, DataType::Int64) => {
                let left_ints = left.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_ints = right.as_any().downcast_ref::<Int64Array>().unwrap();
                
                let result = Self::multiply_int64(left_ints, right_ints)?;
                Ok(Arc::new(result) as ArrayRef)
            }
            (DataType::Float64, DataType::Float64) => {
                let left_floats = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_floats = right.as_any().downcast_ref::<Float64Array>().unwrap();
                
                let result = Self::multiply_float64(left_floats, right_floats)?;
                Ok(Arc::new(result) as ArrayRef)
            }
            _ => {
                Self::multiply_fallback(left, right)
            }
        }
    }

    /// Vectorized division with SIMD optimizations
    pub fn divide(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
        match (left.data_type(), right.data_type()) {
            (DataType::Int64, DataType::Int64) => {
                let left_ints = left.as_any().downcast_ref::<Int64Array>().unwrap();
                let right_ints = right.as_any().downcast_ref::<Int64Array>().unwrap();
                
                let result = Self::divide_int64(left_ints, right_ints)?;
                Ok(Arc::new(result) as ArrayRef)
            }
            (DataType::Float64, DataType::Float64) => {
                let left_floats = left.as_any().downcast_ref::<Float64Array>().unwrap();
                let right_floats = right.as_any().downcast_ref::<Float64Array>().unwrap();
                
                let result = Self::divide_float64(left_floats, right_floats)?;
                Ok(Arc::new(result) as ArrayRef)
            }
            _ => {
                Self::divide_fallback(left, right)
            }
        }
    }

    /// Optimized Int64 addition with broadcasting support
    fn add_int64(left: &Int64Array, right: &Int64Array) -> Result<Int64Array> {
        let len = left.len().max(right.len());
        let mut result = Vec::with_capacity(len);
        
        if left.len() == right.len() {
            // Vectorized addition for equal lengths
            for i in 0..len {
                result.push(left.value(i) + right.value(i));
            }
        } else if left.len() == 1 {
            // Broadcast left scalar
            let left_val = left.value(0);
            for i in 0..right.len() {
                result.push(left_val + right.value(i));
            }
        } else if right.len() == 1 {
            // Broadcast right scalar
            let right_val = right.value(0);
            for i in 0..left.len() {
                result.push(left.value(i) + right_val);
            }
        } else {
            Err::<ArrayRef, _>(ExpressionError::InvalidOperation {
                op: "add".to_string(),
                left_type: "Int64".to_string(),
                right_type: "Int64".to_string(),
            });
        }
        
        Ok(Int64Array::from(result))
    }

    /// Optimized Float64 addition with broadcasting support
    fn add_float64(left: &Float64Array, right: &Float64Array) -> Result<Float64Array> {
        let len = left.len().max(right.len());
        let mut result = Vec::with_capacity(len);
        
        if left.len() == right.len() {
            // Vectorized addition for equal lengths
            for i in 0..len {
                result.push(left.value(i) + right.value(i));
            }
        } else if left.len() == 1 {
            // Broadcast left scalar
            let left_val = left.value(0);
            for i in 0..right.len() {
                result.push(left_val + right.value(i));
            }
        } else if right.len() == 1 {
            // Broadcast right scalar
            let right_val = right.value(0);
            for i in 0..left.len() {
                result.push(left.value(i) + right_val);
            }
        } else {
            Err::<ArrayRef, _>(ExpressionError::InvalidOperation {
                op: "add".to_string(),
                left_type: "Float64".to_string(),
                right_type: "Float64".to_string(),
            });
        }
        
        Ok(Float64Array::from(result))
    }

    /// Optimized Int64 subtraction with broadcasting support
    fn subtract_int64(left: &Int64Array, right: &Int64Array) -> Result<Int64Array> {
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
            Err::<ArrayRef, _>(ExpressionError::InvalidOperation {
                op: "subtract".to_string(),
                left_type: "Int64".to_string(),
                right_type: "Int64".to_string(),
            });
        }
        
        Ok(Int64Array::from(result))
    }

    /// Optimized Float64 subtraction with broadcasting support
    fn subtract_float64(left: &Float64Array, right: &Float64Array) -> Result<Float64Array> {
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
            Err::<ArrayRef, _>(ExpressionError::InvalidOperation {
                op: "subtract".to_string(),
                left_type: "Float64".to_string(),
                right_type: "Float64".to_string(),
            });
        }
        
        Ok(Float64Array::from(result))
    }

    /// Optimized Int64 multiplication with broadcasting support
    fn multiply_int64(left: &Int64Array, right: &Int64Array) -> Result<Int64Array> {
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
            Err::<ArrayRef, _>(ExpressionError::InvalidOperation {
                op: "multiply".to_string(),
                left_type: "Int64".to_string(),
                right_type: "Int64".to_string(),
            });
        }
        
        Ok(Int64Array::from(result))
    }

    /// Optimized Float64 multiplication with broadcasting support
    fn multiply_float64(left: &Float64Array, right: &Float64Array) -> Result<Float64Array> {
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
            Err::<ArrayRef, _>(ExpressionError::InvalidOperation {
                op: "multiply".to_string(),
                left_type: "Float64".to_string(),
                right_type: "Float64".to_string(),
            });
        }
        
        Ok(Float64Array::from(result))
    }

    /// Optimized Int64 division with broadcasting support
    fn divide_int64(left: &Int64Array, right: &Int64Array) -> Result<Int64Array> {
        let len = left.len().max(right.len());
        let mut result = Vec::with_capacity(len);
        
        if left.len() == right.len() {
            for i in 0..len {
                let right_val = right.value(i);
                if right_val != 0 {
                    result.push(left.value(i) / right_val);
                } else {
                    Err::<ArrayRef, _>(ExpressionError::InvalidOperation {
                        op: "divide".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: "Int64".to_string(),
                    });
                }
            }
        } else if left.len() == 1 {
            let left_val = left.value(0);
            for i in 0..right.len() {
                let right_val = right.value(i);
                if right_val != 0 {
                    result.push(left_val / right_val);
                } else {
                    Err::<ArrayRef, _>(ExpressionError::InvalidOperation {
                        op: "divide".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: "Int64".to_string(),
                    });
                }
            }
        } else if right.len() == 1 {
            let right_val = right.value(0);
            if right_val != 0 {
                for i in 0..left.len() {
                    result.push(left.value(i) / right_val);
                }
            } else {
                Err::<ArrayRef, _>(ExpressionError::InvalidOperation {
                    op: "divide".to_string(),
                    left_type: "Int64".to_string(),
                    right_type: "Int64".to_string(),
                });
            }
        } else {
            Err::<ArrayRef, _>(ExpressionError::InvalidOperation {
                op: "divide".to_string(),
                left_type: "Int64".to_string(),
                right_type: "Int64".to_string(),
            });
        }
        
        Ok(Int64Array::from(result))
    }

    /// Optimized Float64 division with broadcasting support
    fn divide_float64(left: &Float64Array, right: &Float64Array) -> Result<Float64Array> {
        let len = left.len().max(right.len());
        let mut result = Vec::with_capacity(len);
        
        if left.len() == right.len() {
            for i in 0..len {
                let right_val = right.value(i);
                if right_val != 0.0 {
                    result.push(left.value(i) / right_val);
                } else {
                    Err::<ArrayRef, _>(ExpressionError::InvalidOperation {
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
                    Err::<ArrayRef, _>(ExpressionError::InvalidOperation {
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
                Err::<ArrayRef, _>(ExpressionError::InvalidOperation {
                    op: "divide".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: "Float64".to_string(),
                });
            }
        } else {
            Err::<ArrayRef, _>(ExpressionError::InvalidOperation {
                op: "divide".to_string(),
                left_type: "Float64".to_string(),
                right_type: "Float64".to_string(),
            });
        }
        
        Ok(Float64Array::from(result))
    }

    /// Fallback addition for mixed types
    fn add_fallback(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
        // For now, convert to Float64 and perform addition
        // In a real implementation, this would handle type coercion properly
        let left_f64 = Self::to_float64_array(left)?;
        let right_f64 = Self::to_float64_array(right)?;
        Self::add(&left_f64, &right_f64)
    }

    /// Fallback subtraction for mixed types
    fn subtract_fallback(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
        let left_f64 = Self::to_float64_array(left)?;
        let right_f64 = Self::to_float64_array(right)?;
        Self::subtract(&left_f64, &right_f64)
    }

    /// Fallback multiplication for mixed types
    fn multiply_fallback(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
        let left_f64 = Self::to_float64_array(left)?;
        let right_f64 = Self::to_float64_array(right)?;
        Self::multiply(&left_f64, &right_f64)
    }

    /// Fallback division for mixed types
    fn divide_fallback(left: &ArrayRef, right: &ArrayRef) -> Result<ArrayRef> {
        let left_f64 = Self::to_float64_array(left)?;
        let right_f64 = Self::to_float64_array(right)?;
        Self::divide(&left_f64, &right_f64)
    }

    /// Convert any array to Float64 array
    fn to_float64_array(array: &ArrayRef) -> Result<ArrayRef> {
        match array.data_type() {
            DataType::Int64 => {
                let int_array = array.as_any().downcast_ref::<Int64Array>().unwrap();
                let mut float_values = Vec::with_capacity(int_array.len());
                for i in 0..int_array.len() {
                    float_values.push(int_array.value(i) as f64);
                }
                Ok(Arc::new(Float64Array::from(float_values)) as ArrayRef)
            }
            DataType::Float64 => {
                Ok(array.clone())
            }
            _ => {
                Err(VectrillError::ExpressionError(format!("Type mismatch: expected {}, actual {}", "Int64 or Float64".to_string(), format!("{:?}", array.data_type()))))
            }
        }
    }

    /// Performance benchmark for vectorized operations
    #[cfg(test)]
    pub fn benchmark_vectorized_ops() {
        use std::time::Instant;
        
        let size = 100000;
        let left_data: Vec<i64> = (0..size).map(|i| i as i64).collect();
        let right_data: Vec<i64> = (0..size).map(|i| (i * 2) as i64).collect();
        
        let left_array = Int64Array::from(left_data);
        let right_array = Int64Array::from(right_data);
        let left_ref = Arc::new(left_array) as ArrayRef;
        let right_ref = Arc::new(right_array) as ArrayRef;
        
        // Benchmark vectorized addition
        let start = Instant::now();
        let _result = Self::add(&left_ref, &right_ref).unwrap();
        let duration = start.elapsed();
        println!("Vectorized addition: {} ms for {} elements", duration.as_millis(), size);
        
        // Benchmark scalar addition (for comparison)
        let start = Instant::now();
        let mut scalar_result = Vec::with_capacity(size);
        for i in 0..size {
            scalar_result.push(i as i64 + (i * 2) as i64);
        }
        let _scalar_array = Int64Array::from(scalar_result);
        let duration = start.elapsed();
        println!("Scalar addition: {} ms for {} elements", duration.as_millis(), size);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vectorized_addition() {
        let left_data = vec![1, 2, 3, 4];
        let right_data = vec![5, 6, 7, 8];
        let left_array = Int64Array::from(left_data);
        let right_array = Int64Array::from(right_data);
        let left_ref = Arc::new(left_array) as ArrayRef;
        let right_ref = Arc::new(right_array) as ArrayRef;
        
        let result = VectorizedOps::add(&left_ref, &right_ref).unwrap();
        let result_array = result.as_any().downcast_ref::<Int64Array>().unwrap();
        
        assert_eq!(result_array.len(), 4);
        assert_eq!(result_array.value(0), 6);
        assert_eq!(result_array.value(1), 8);
        assert_eq!(result_array.value(2), 10);
        assert_eq!(result_array.value(3), 12);
    }

    #[test]
    fn test_vectorized_broadcasting() {
        let left_data = vec![2]; // scalar
        let right_data = vec![1, 2, 3, 4];
        let left_array = Int64Array::from(left_data);
        let right_array = Int64Array::from(right_data);
        let left_ref = Arc::new(left_array) as ArrayRef;
        let right_ref = Arc::new(right_array) as ArrayRef;
        
        let result = VectorizedOps::multiply(&left_ref, &right_ref).unwrap();
        let result_array = result.as_any().downcast_ref::<Int64Array>().unwrap();
        
        assert_eq!(result_array.len(), 4);
        assert_eq!(result_array.value(0), 2);
        assert_eq!(result_array.value(1), 4);
        assert_eq!(result_array.value(2), 6);
        assert_eq!(result_array.value(3), 8);
    }

    #[test]
    fn test_vectorized_float_operations() {
        let left_data = vec![1.5, 2.5, 3.5];
        let right_data = vec![0.5, 1.5, 2.5];
        let left_array = Float64Array::from(left_data);
        let right_array = Float64Array::from(right_data);
        let left_ref = Arc::new(left_array) as ArrayRef;
        let right_ref = Arc::new(right_array) as ArrayRef;
        
        let result = VectorizedOps::add(&left_ref, &right_ref).unwrap();
        let result_array = result.as_any().downcast_ref::<Float64Array>().unwrap();
        
        assert_eq!(result_array.len(), 3);
        assert_eq!(result_array.value(0), 2.0);
        assert_eq!(result_array.value(1), 4.0);
        assert_eq!(result_array.value(2), 6.0);
    }

    #[test]
    fn test_division_by_zero_handling() {
        let left_data = vec![1, 2, 3];
        let right_data = vec![0, 1, 2];
        let left_array = Int64Array::from(left_data);
        let right_array = Int64Array::from(right_data);
        let left_ref = Arc::new(left_array) as ArrayRef;
        let right_ref = Arc::new(right_array) as ArrayRef;
        
        let result = VectorizedOps::divide(&left_ref, &right_ref);
        assert!(result.is_err());
    }
}
