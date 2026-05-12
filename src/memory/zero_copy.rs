//! Zero-copy operations for Arrow array sharing

use crate::error::Result;
use arrow::array::*;
use arrow::buffer::{Buffer, OffsetBuffer};
use arrow::datatypes::*;
use std::sync::Arc;

/// Zero-copy operations for efficient Arrow array sharing
pub struct ZeroCopyOps;

impl ZeroCopyOps {
    /// Create a zero-copy view of an array with different data type
    pub fn cast_view(array: &ArrayRef) -> Result<ArrayRef> {
        // For now, just return the array as-is since true zero-copy casting
        // requires more complex Arrow buffer manipulation
        // This is a placeholder that can be enhanced later
        Ok(array.clone())
    }

    /// Create a zero-copy slice of an array
    pub fn slice(array: &ArrayRef, offset: usize, length: usize) -> Result<ArrayRef> {
        if offset + length > array.len() {
            return Err(crate::error::VectrillError::InvalidExpression(format!(
                "Slice out of bounds: offset={}, length={}, array_len={}",
                offset,
                length,
                array.len()
            )));
        }

        let sliced = array.slice(offset, length);
        Ok(sliced)
    }

    /// Create a zero-copy view with only selected columns (for struct arrays)
    pub fn select_columns(array: &ArrayRef, column_indices: &[usize]) -> Result<ArrayRef> {
        match array.data_type() {
            DataType::Struct(fields) => {
                let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
                let selected_arrays: Vec<ArrayRef> = column_indices
                    .iter()
                    .map(|&i| struct_array.column(i).clone())
                    .collect();

                let selected_fields: Vec<Field> = column_indices
                    .iter()
                    .map(|&i| fields[i].as_ref().clone())
                    .collect();

                let new_struct = StructArray::new(selected_fields.into(), selected_arrays, None);
                Ok(Arc::new(new_struct) as ArrayRef)
            }
            _ => Err(crate::error::VectrillError::InvalidExpression(
                "Column selection only supported for struct arrays".to_string(),
            )),
        }
    }

    /// Create a zero-copy view with reordered columns
    pub fn reorder_columns(array: &ArrayRef, new_order: &[usize]) -> Result<ArrayRef> {
        Self::select_columns(array, new_order)
    }

    /// Create a zero-copy view of a string array as UTF8
    pub fn string_as_utf8_view(array: &ArrayRef) -> Result<ArrayRef> {
        match array.data_type() {
            DataType::Utf8 => Ok(array.clone()),
            DataType::LargeUtf8 => {
                // For LargeUtf8, we need to handle differently
                Err(crate::error::VectrillError::InvalidExpression(
                    "LargeUtf8 to Utf8 conversion not zero-copy compatible".to_string(),
                ))
            }
            _ => Err(crate::error::VectrillError::InvalidExpression(
                "Array is not a UTF8 string array".to_string(),
            )),
        }
    }

    /// Create a zero-copy view of binary data
    pub fn binary_view(array: &ArrayRef) -> Result<ArrayRef> {
        match array.data_type() {
            DataType::Binary => Ok(array.clone()),
            _ => Err(crate::error::VectrillError::InvalidExpression(
                "Array is not a binary array".to_string(),
            )),
        }
    }

    /// Create a zero-copy view with null mask removed
    pub fn drop_nulls(array: &ArrayRef) -> Result<ArrayRef> {
        if !array.null_count() > 0 {
            return Err(crate::error::VectrillError::InvalidExpression(
                "Cannot drop nulls from array with null values".to_string(),
            ));
        }

        // If no nulls, return the array as-is (zero-copy)
        Ok(array.clone())
    }

    /// Create a zero-copy view with boolean mask applied
    pub fn filter_mask(array: &ArrayRef, mask: &BooleanArray) -> Result<ArrayRef> {
        if array.len() != mask.len() {
            return Err(crate::error::VectrillError::InvalidExpression(format!(
                "Array length {} does not match mask length {}",
                array.len(),
                mask.len()
            )));
        }

        // This would require actual filtering, which is not zero-copy
        // For now, return error as this operation cannot be zero-copy
        Err(crate::error::VectrillError::InvalidExpression(
            "Filtering with mask cannot be zero-copy operation".to_string(),
        ))
    }

    /// Create a zero-copy view of a list array's elements
    pub fn list_elements(array: &ArrayRef) -> Result<ArrayRef> {
        match array.data_type() {
            DataType::List(_field) => {
                let list_array = array.as_any().downcast_ref::<ListArray>().unwrap();
                Ok(list_array.values().clone())
            }
            DataType::LargeList(_field) => {
                let list_array = array.as_any().downcast_ref::<LargeListArray>().unwrap();
                Ok(list_array.values().clone())
            }
            _ => Err(crate::error::VectrillError::InvalidExpression(
                "Array is not a list array".to_string(),
            )),
        }
    }

    /// Create a zero-copy view of a map array's values
    pub fn map_values(array: &ArrayRef) -> Result<ArrayRef> {
        match array.data_type() {
            DataType::Map(_field, _) => {
                let map_array = array.as_any().downcast_ref::<MapArray>().unwrap();
                Ok(map_array.values().clone())
            }
            _ => Err(crate::error::VectrillError::InvalidExpression(
                "Array is not a map array".to_string(),
            )),
        }
    }

    /// Share an array between multiple consumers without copying
    pub fn share_array(array: &ArrayRef) -> ArrayRef {
        // Simply clone the Arc, which is zero-copy
        array.clone()
    }

    /// Create a zero-copy view with a new schema (for struct arrays)
    pub fn change_schema(array: &ArrayRef, new_fields: Vec<Field>) -> Result<ArrayRef> {
        match array.data_type() {
            DataType::Struct(_) => {
                let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
                if new_fields.len() != struct_array.columns().len() {
                    return Err(crate::error::VectrillError::InvalidExpression(
                        "New schema must have same number of fields as original".to_string(),
                    ));
                }

                let new_struct =
                    StructArray::new(new_fields.into(), struct_array.columns().to_vec(), None);
                Ok(Arc::new(new_struct) as ArrayRef)
            }
            _ => Err(crate::error::VectrillError::InvalidExpression(
                "Schema change only supported for struct arrays".to_string(),
            )),
        }
    }

    /// Create a zero-copy view with metadata preserved
    pub fn preserve_metadata(array: &ArrayRef) -> ArrayRef {
        // Arrow arrays already preserve metadata in zero-copy operations
        array.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_copy_slice() {
        let data = vec![1, 2, 3, 4, 5];
        let array = Int64Array::from(data);
        let array_ref = Arc::new(array) as ArrayRef;

        let sliced = ZeroCopyOps::slice(&array_ref, 1, 3).unwrap();
        assert_eq!(sliced.len(), 3);

        let sliced_int = sliced.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(sliced_int.value(0), 2);
        assert_eq!(sliced_int.value(1), 3);
        assert_eq!(sliced_int.value(2), 4);
    }

    #[test]
    fn test_zero_copy_share() {
        let data = vec![1.0, 2.0, 3.0];
        let array = Float64Array::from(data);
        let array_ref = Arc::new(array) as ArrayRef;

        let shared = ZeroCopyOps::share_array(&array_ref);
        assert!(Arc::ptr_eq(&array_ref, &shared));
    }

    #[test]
    fn test_zero_copy_cast_view() {
        let data = vec![1, 2, 3, 4];
        let array = Int64Array::from(data);
        let array_ref = Arc::new(array) as ArrayRef;

        // Cast Int64 to Float64 (zero-copy view)
        let casted = ZeroCopyOps::cast_view(&array_ref).unwrap();
        let casted_float = casted.as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(casted_float.len(), 4);
        assert_eq!(casted_float.value(0), 1.0);
        assert_eq!(casted_float.value(1), 2.0);
    }

    #[test]
    fn test_zero_copy_string_view() {
        let data = vec!["hello", "world", "test"];
        let array = StringArray::from(data);
        let array_ref = Arc::new(array) as ArrayRef;

        let string_view = ZeroCopyOps::string_as_utf8_view(&array_ref).unwrap();
        assert_eq!(string_view.len(), 3);
        let string_array = string_view.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(string_array.value(0), "hello");
        assert_eq!(string_array.value(1), "world");
        assert_eq!(string_array.value(2), "test");
    }

    #[test]
    fn test_zero_copy_list_elements() {
        let values = Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let field = arrow::datatypes::Field::new("item", arrow::datatypes::DataType::Int32, true);
        let offsets = arrow::buffer::OffsetBuffer::new(vec![0i32, 2, 4, 6].into());
        let list_array =
            ListArray::try_new(Arc::new(field), offsets, Arc::new(values), None).unwrap();

        let array_ref = Arc::new(list_array) as ArrayRef;
        let elements = ZeroCopyOps::list_elements(&array_ref).unwrap();

        assert_eq!(elements.len(), 6);
        let int_elements = elements.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(int_elements.value(0), 1);
        assert_eq!(int_elements.value(5), 6);
    }
}
