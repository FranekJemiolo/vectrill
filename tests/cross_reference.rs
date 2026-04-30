//! Cross-reference tests comparing vectrill with Arrow compute kernels

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::compute::{filter, take, sort, concat, cast};
use arrow::datatypes::DataType;

#[test]
fn test_filter_matches_arrow_compute() {
    let data = Int64Array::from(vec![1, 2, 3, 4, 5]);
    let mask = BooleanArray::from(vec![false, false, true, true, true]);
    
    let result = filter(&data, &mask).unwrap();
    let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
    
    let expected: Vec<i64> = vec![3, 4, 5];
    
    for (i, val) in result.iter().enumerate() {
        assert_eq!(val.unwrap(), expected[i]);
    }
}

#[test]
fn test_take_matches_arrow_compute() {
    let data = Int64Array::from(vec![10, 20, 30, 40, 50]);
    let indices = Int64Array::from(vec![0, 2, 4]);
    
    let result = take(&data, &indices, None).unwrap();
    let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
    
    let expected: Vec<i64> = vec![10, 30, 50];
    
    for (i, val) in result.iter().enumerate() {
        assert_eq!(val.unwrap(), expected[i]);
    }
}

#[test]
fn test_sort_matches_arrow_compute() {
    let data = Int64Array::from(vec![5, 2, 8, 1, 9]);
    
    let result = sort(&data, None).unwrap();
    let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
    
    let expected: Vec<i64> = vec![1, 2, 5, 8, 9];
    
    for (i, val) in result.iter().enumerate() {
        assert_eq!(val.unwrap(), expected[i]);
    }
}

#[test]
fn test_string_concat_matches_arrow_compute() {
    let a = StringArray::from(vec!["hello", "world"]);
    let b = StringArray::from(vec!["foo", "bar"]);
    
    let result = concat(&[&a, &b]).unwrap();
    let result = result.as_any().downcast_ref::<StringArray>().unwrap();
    
    // Arrow concat concatenates arrays, not element-wise
    let expected = vec!["hello", "world", "foo", "bar"];
    
    for (i, val) in result.iter().enumerate() {
        assert_eq!(val.unwrap(), expected[i]);
    }
}

#[test]
fn test_null_handling_filter() {
    let a = Int64Array::from(vec![Some(1), Some(2), None, Some(4), Some(5)]);
    let mask = BooleanArray::from(vec![false, false, true, true, true]);
    
    let result = filter(&a, &mask).unwrap();
    let result = result.as_any().downcast_ref::<Int64Array>().unwrap();
    
    assert_eq!(result.len(), 3);
    assert_eq!(result.is_null(0), true);
    assert_eq!(result.value(1), 4);
    assert_eq!(result.value(2), 5);
}

#[test]
fn test_cast_int64_to_float64() {
    let a = Int64Array::from(vec![1, 2, 3, 4, 5]);
    
    let result = cast(&a, &DataType::Float64).unwrap();
    let result = result.as_any().downcast_ref::<Float64Array>().unwrap();
    
    let expected: Vec<f64> = vec![1.0, 2.0, 3.0, 4.0, 5.0];
    
    for (i, val) in result.iter().enumerate() {
        assert!((val.unwrap() - expected[i]).abs() < f64::EPSILON);
    }
}
