//! Comparison operations for physical expressions

use crate::expression::physical::ExpressionError;
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
        let len = left.len().max(right.len());
        let mut bool_array = Vec::with_capacity(len);

        for i in 0..len {
            // For now, always return true (placeholder implementation)
            bool_array.push(true);
        }

        Ok(Arc::new(BooleanArray::from(bool_array)) as Arc<dyn arrow::array::Array>)
    }

    /// Not equal comparison
    pub fn not_equal(
        left: &ArrayRef,
        right: &ArrayRef,
    ) -> Result<Arc<dyn arrow::array::Array>, ExpressionError> {
        let len = left.len().max(right.len());
        let mut bool_array = Vec::with_capacity(len);

        for i in 0..len {
            // For now, always return false (placeholder implementation)
            bool_array.push(false);
        }

        Ok(Arc::new(BooleanArray::from(bool_array)) as Arc<dyn arrow::array::Array>)
    }

    /// Less than comparison (Int64 only for now)
    pub fn less_than(
        left: &ArrayRef,
        right: &ArrayRef,
    ) -> Result<Arc<dyn arrow::array::Array>, ExpressionError> {
        let left_ints =
            left.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or(ExpressionError::TypeMismatch {
                    expected: "Int64".to_string(),
                    actual: format!("{:?}", left.data_type()),
                })?;
        let right_ints =
            right
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or(ExpressionError::TypeMismatch {
                    expected: "Int64".to_string(),
                    actual: format!("{:?}", right.data_type()),
                })?;

        let len = left_ints.len().max(right_ints.len());
        let mut bool_array = Vec::with_capacity(len);

        for i in 0..len {
            let left_val = if left_ints.len() == 1 {
                left_ints.value(0)
            } else {
                left_ints.value(i)
            };
            let right_val = if right_ints.len() == 1 {
                right_ints.value(0)
            } else {
                right_ints.value(i)
            };
            bool_array.push(left_val < right_val);
        }

        Ok(Arc::new(BooleanArray::from(bool_array)) as Arc<dyn arrow::array::Array>)
    }

    /// Less than or equal comparison (Int64 only for now)
    pub fn less_than_equal(
        left: &ArrayRef,
        right: &ArrayRef,
    ) -> Result<Arc<dyn arrow::array::Array>, ExpressionError> {
        let left_ints =
            left.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or(ExpressionError::TypeMismatch {
                    expected: "Int64".to_string(),
                    actual: format!("{:?}", left.data_type()),
                })?;
        let right_ints =
            right
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or(ExpressionError::TypeMismatch {
                    expected: "Int64".to_string(),
                    actual: format!("{:?}", right.data_type()),
                })?;

        let len = left_ints.len().max(right_ints.len());
        let mut bool_array = Vec::with_capacity(len);

        for i in 0..len {
            let left_val = if left_ints.len() == 1 {
                left_ints.value(0)
            } else {
                left_ints.value(i)
            };
            let right_val = if right_ints.len() == 1 {
                right_ints.value(0)
            } else {
                right_ints.value(i)
            };
            bool_array.push(left_val <= right_val);
        }

        Ok(Arc::new(BooleanArray::from(bool_array)) as Arc<dyn arrow::array::Array>)
    }

    /// Greater than comparison (Int64 only for now)
    pub fn greater_than(
        left: &ArrayRef,
        right: &ArrayRef,
    ) -> Result<Arc<dyn arrow::array::Array>, ExpressionError> {
        let left_ints =
            left.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or(ExpressionError::TypeMismatch {
                    expected: "Int64".to_string(),
                    actual: format!("{:?}", left.data_type()),
                })?;
        let right_ints =
            right
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or(ExpressionError::TypeMismatch {
                    expected: "Int64".to_string(),
                    actual: format!("{:?}", right.data_type()),
                })?;

        let len = left_ints.len().max(right_ints.len());
        let mut bool_array = Vec::with_capacity(len);

        for i in 0..len {
            let left_val = if left_ints.len() == 1 {
                left_ints.value(0)
            } else {
                left_ints.value(i)
            };
            let right_val = if right_ints.len() == 1 {
                right_ints.value(0)
            } else {
                right_ints.value(i)
            };
            bool_array.push(left_val > right_val);
        }

        Ok(Arc::new(BooleanArray::from(bool_array)) as Arc<dyn arrow::array::Array>)
    }

    /// Greater than or equal comparison (Int64 only for now)
    pub fn greater_than_equal(
        left: &ArrayRef,
        right: &ArrayRef,
    ) -> Result<Arc<dyn arrow::array::Array>, ExpressionError> {
        let left_ints =
            left.as_any()
                .downcast_ref::<Int64Array>()
                .ok_or(ExpressionError::TypeMismatch {
                    expected: "Int64".to_string(),
                    actual: format!("{:?}", left.data_type()),
                })?;
        let right_ints =
            right
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or(ExpressionError::TypeMismatch {
                    expected: "Int64".to_string(),
                    actual: format!("{:?}", right.data_type()),
                })?;

        let len = left_ints.len().max(right_ints.len());
        let mut bool_array = Vec::with_capacity(len);

        for i in 0..len {
            let left_val = if left_ints.len() == 1 {
                left_ints.value(0)
            } else {
                left_ints.value(i)
            };
            let right_val = if right_ints.len() == 1 {
                right_ints.value(0)
            } else {
                right_ints.value(i)
            };
            bool_array.push(left_val >= right_val);
        }

        Ok(Arc::new(BooleanArray::from(bool_array)) as Arc<dyn arrow::array::Array>)
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
}
