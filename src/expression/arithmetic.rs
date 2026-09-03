//! Arithmetic operations for physical expressions

use crate::expression::physical::{align_operands, ExpressionError};
use arrow::array::*;
use std::sync::Arc;

/// Arithmetic operations with broadcasting support
pub struct ArithmeticOps;

impl ArithmeticOps {
    /// Addition with broadcasting support
    pub fn add(
        left: &ArrayRef,
        right: &ArrayRef,
    ) -> Result<Arc<dyn arrow::array::Array>, ExpressionError> {
        let (l, r) = align_operands(left, right)?;
        let res = arrow_arith::numeric::add(&l, &r).map_err(ExpressionError::ArrowError)?;
        Ok(Arc::new(res) as Arc<dyn arrow::array::Array>)
    }

    /// Subtraction with broadcasting support
    pub fn subtract(
        left: &ArrayRef,
        right: &ArrayRef,
    ) -> Result<Arc<dyn arrow::array::Array>, ExpressionError> {
        let (l, r) = align_operands(left, right)?;
        let res = arrow_arith::numeric::sub(&l, &r).map_err(ExpressionError::ArrowError)?;
        Ok(Arc::new(res) as Arc<dyn arrow::array::Array>)
    }

    /// Multiplication with broadcasting support
    pub fn multiply(
        left: &ArrayRef,
        right: &ArrayRef,
    ) -> Result<Arc<dyn arrow::array::Array>, ExpressionError> {
        let (l, r) = align_operands(left, right)?;
        let res = arrow_arith::numeric::mul(&l, &r).map_err(ExpressionError::ArrowError)?;
        Ok(Arc::new(res) as Arc<dyn arrow::array::Array>)
    }

    /// Division with broadcasting support
    pub fn divide(
        left: &ArrayRef,
        right: &ArrayRef,
    ) -> Result<Arc<dyn arrow::array::Array>, ExpressionError> {
        let (l, r) = align_operands(left, right)?;
        let res = arrow_arith::numeric::div(&l, &r).map_err(ExpressionError::ArrowError)?;
        Ok(Arc::new(res) as Arc<dyn arrow::array::Array>)
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
