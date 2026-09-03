//! Comparison operations for physical expressions

use crate::expression::physical::{align_operands, ExpressionError};
use arrow::array::*;
use std::sync::Arc;

/// Comparison operations
pub struct ComparisonOps;

impl ComparisonOps {
    /// Equality comparison
    pub fn equal(
        left: &ArrayRef,
        right: &ArrayRef,
    ) -> Result<Arc<dyn arrow::array::Array>, ExpressionError> {
        let (l, r) = align_operands(left, right)?;
        let res = arrow_ord::cmp::eq(&l, &r).map_err(ExpressionError::ArrowError)?;
        Ok(Arc::new(res) as Arc<dyn arrow::array::Array>)
    }

    /// Not equal comparison
    pub fn not_equal(
        left: &ArrayRef,
        right: &ArrayRef,
    ) -> Result<Arc<dyn arrow::array::Array>, ExpressionError> {
        let (l, r) = align_operands(left, right)?;
        let res = arrow_ord::cmp::neq(&l, &r).map_err(ExpressionError::ArrowError)?;
        Ok(Arc::new(res) as Arc<dyn arrow::array::Array>)
    }

    /// Less than comparison
    pub fn less_than(
        left: &ArrayRef,
        right: &ArrayRef,
    ) -> Result<Arc<dyn arrow::array::Array>, ExpressionError> {
        let (l, r) = align_operands(left, right)?;
        let res = arrow_ord::cmp::lt(&l, &r).map_err(ExpressionError::ArrowError)?;
        Ok(Arc::new(res) as Arc<dyn arrow::array::Array>)
    }

    /// Less than or equal comparison
    pub fn less_than_equal(
        left: &ArrayRef,
        right: &ArrayRef,
    ) -> Result<Arc<dyn arrow::array::Array>, ExpressionError> {
        let (l, r) = align_operands(left, right)?;
        let res = arrow_ord::cmp::lt_eq(&l, &r).map_err(ExpressionError::ArrowError)?;
        Ok(Arc::new(res) as Arc<dyn arrow::array::Array>)
    }

    /// Greater than comparison
    pub fn greater_than(
        left: &ArrayRef,
        right: &ArrayRef,
    ) -> Result<Arc<dyn arrow::array::Array>, ExpressionError> {
        let (l, r) = align_operands(left, right)?;
        let res = arrow_ord::cmp::gt(&l, &r).map_err(ExpressionError::ArrowError)?;
        Ok(Arc::new(res) as Arc<dyn arrow::array::Array>)
    }

    /// Greater than or equal comparison
    pub fn greater_than_equal(
        left: &ArrayRef,
        right: &ArrayRef,
    ) -> Result<Arc<dyn arrow::array::Array>, ExpressionError> {
        let (l, r) = align_operands(left, right)?;
        let res = arrow_ord::cmp::gt_eq(&l, &r).map_err(ExpressionError::ArrowError)?;
        Ok(Arc::new(res) as Arc<dyn arrow::array::Array>)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_comparison_less_than() {
        let left_data = vec![1, 2, 3];
        let right_data = vec![4, 5, 6];
        let left_array = Int64Array::from(left_data);
        let right_array = Int64Array::from(right_data);
        let left_ref = Arc::new(left_array) as ArrayRef;
        let right_ref = Arc::new(right_array) as ArrayRef;

        let result = ComparisonOps::less_than(&left_ref, &right_ref).unwrap();
        let result_array = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(result_array.len(), 3);
        assert_eq!(result_array.value(0), true);
        assert_eq!(result_array.value(1), true);
        assert_eq!(result_array.value(2), true);
    }

    #[test]
    fn test_comparison_greater_than() {
        let left_data = vec![7, 8, 9];
        let right_data = vec![4, 5, 6];
        let left_array = Int64Array::from(left_data);
        let right_array = Int64Array::from(right_data);
        let left_ref = Arc::new(left_array) as ArrayRef;
        let right_ref = Arc::new(right_array) as ArrayRef;

        let result = ComparisonOps::greater_than(&left_ref, &right_ref).unwrap();
        let result_array = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(result_array.len(), 3);
        assert_eq!(result_array.value(0), true);
        assert_eq!(result_array.value(1), true);
        assert_eq!(result_array.value(2), true);
    }

    #[test]
    fn test_comparison_equal_and_not_equal() {
        let left_ref = Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef;
        let right_ref = Arc::new(Int64Array::from(vec![1, 5, 3])) as ArrayRef;

        let eq_res = ComparisonOps::equal(&left_ref, &right_ref).unwrap();
        let eq_arr = eq_res.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(eq_arr.value(0), true);
        assert_eq!(eq_arr.value(1), false);
        assert_eq!(eq_arr.value(2), true);

        let neq_res = ComparisonOps::not_equal(&left_ref, &right_ref).unwrap();
        let neq_arr = neq_res.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(neq_arr.value(0), false);
        assert_eq!(neq_arr.value(1), true);
        assert_eq!(neq_arr.value(2), false);
    }
}
