//! Function registry for custom and built-in functions

use arrow::array::{Array, ArrayRef};
use arrow::datatypes::DataType;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::physical::{ExpressionError, Result};

/// Function signature for custom functions
pub type FunctionSignature = fn(&[ArrayRef]) -> Result<ArrayRef>;

/// Function metadata
#[derive(Debug, Clone)]
pub struct FunctionMetadata {
    pub name: String,
    pub return_type: DataType,
    pub variadic: bool,
    pub min_args: usize,
    pub max_args: usize,
}

/// Global function registry
pub struct FunctionRegistry {
    functions: RwLock<HashMap<String, (FunctionSignature, FunctionMetadata)>>,
}

impl FunctionRegistry {
    /// Create a new function registry
    pub fn new() -> Self {
        let registry = Self {
            functions: RwLock::new(HashMap::new()),
        };

        // Register built-in functions
        registry.register_builtin_functions();

        registry
    }

    /// Register a custom function
    pub fn register_function(
        &self,
        name: impl Into<String>,
        signature: FunctionSignature,
        metadata: FunctionMetadata,
    ) -> Result<()> {
        let name = name.into();
        let mut functions = self.functions.write().unwrap();

        if functions.contains_key(&name) {
            return Err(ExpressionError::UnsupportedExpression(format!(
                "Function {} already registered",
                name
            )));
        }

        functions.insert(name, (signature, metadata));
        Ok(())
    }

    /// Get a function by name
    pub fn get_function(&self, name: &str) -> Option<(FunctionSignature, FunctionMetadata)> {
        let functions = self.functions.read().unwrap();
        functions.get(name).cloned()
    }

    /// Check if a function exists
    pub fn has_function(&self, name: &str) -> bool {
        let functions = self.functions.read().unwrap();
        functions.contains_key(name)
    }

    /// List all registered functions
    pub fn list_functions(&self) -> Vec<String> {
        let functions = self.functions.read().unwrap();
        functions.keys().cloned().collect()
    }

    /// Register built-in functions
    fn register_builtin_functions(&self) {
        // Math functions
        self.register_builtin("abs", abs_function, DataType::Float64, 1, 1);
        self.register_builtin("sqrt", sqrt_function, DataType::Float64, 1, 1);
        self.register_builtin("pow", pow_function, DataType::Float64, 2, 2);
        self.register_builtin("floor", floor_function, DataType::Float64, 1, 1);
        self.register_builtin("ceil", ceil_function, DataType::Float64, 1, 1);
        self.register_builtin("round", round_function, DataType::Float64, 1, 2);

        // String functions
        self.register_builtin("length", length_function, DataType::Int64, 1, 1);
        self.register_builtin("upper", upper_function, DataType::Utf8, 1, 1);
        self.register_builtin("lower", lower_function, DataType::Utf8, 1, 1);
        self.register_builtin("trim", trim_function, DataType::Utf8, 1, 1);
        self.register_builtin("concat", concat_function, DataType::Utf8, 2, usize::MAX);
        self.register_builtin("substring", substring_function, DataType::Utf8, 3, 3);

        // Conditional functions
        self.register_builtin("coalesce", coalesce_function, DataType::Null, 1, usize::MAX);

        // Aggregate functions (simplified - actual aggregation requires state)
        self.register_builtin("sum", sum_function, DataType::Float64, 1, 1);
        self.register_builtin("avg", avg_function, DataType::Float64, 1, 1);
        self.register_builtin("min", min_function, DataType::Null, 1, 1);
        self.register_builtin("max", max_function, DataType::Null, 1, 1);
        self.register_builtin("count", count_function, DataType::Int64, 1, 1);
    }

    fn register_builtin(
        &self,
        name: &str,
        signature: FunctionSignature,
        return_type: DataType,
        min_args: usize,
        max_args: usize,
    ) {
        let metadata = FunctionMetadata {
            name: name.to_string(),
            return_type,
            variadic: max_args == usize::MAX,
            min_args,
            max_args,
        };

        let mut functions = self.functions.write().unwrap();
        functions.insert(name.to_string(), (signature, metadata));
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// Global function registry instance
static GLOBAL_REGISTRY: once_cell::sync::Lazy<FunctionRegistry> =
    once_cell::sync::Lazy::new(FunctionRegistry::new);

/// Get the global function registry
pub fn global_registry() -> &'static FunctionRegistry {
    &GLOBAL_REGISTRY
}

// Built-in function implementations

fn abs_function(args: &[ArrayRef]) -> Result<ArrayRef> {
    use arrow::array::{Float64Array, Int64Array};

    if args.len() != 1 {
        return Err(ExpressionError::InvalidArgumentCount {
            function: "abs".to_string(),
            expected: 1,
            actual: args.len(),
        });
    }

    let array = &args[0];

    if let Some(floats) = array.as_any().downcast_ref::<Float64Array>() {
        let result: Vec<f64> = floats
            .iter()
            .map(|v| v.map(|x| x.abs()).unwrap_or(0.0))
            .collect();
        Ok(Arc::new(Float64Array::from(result)))
    } else if let Some(ints) = array.as_any().downcast_ref::<Int64Array>() {
        let result: Vec<i64> = ints
            .iter()
            .map(|v| v.map(|x| x.abs()).unwrap_or(0))
            .collect();
        Ok(Arc::new(Int64Array::from(result)))
    } else {
        Ok(array.clone())
    }
}

fn sqrt_function(args: &[ArrayRef]) -> Result<ArrayRef> {
    use arrow::array::Float64Array;

    if args.len() != 1 {
        return Err(ExpressionError::InvalidArgumentCount {
            function: "sqrt".to_string(),
            expected: 1,
            actual: args.len(),
        });
    }

    let array = &args[0];
    let floats =
        array
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or(ExpressionError::TypeMismatch {
                expected: "Float64".to_string(),
                actual: format!("{:?}", array.data_type()),
            })?;

    let result: Vec<f64> = floats
        .iter()
        .map(|v| v.map(|x| x.sqrt()).unwrap_or(0.0))
        .collect();
    Ok(Arc::new(Float64Array::from(result)))
}

fn pow_function(args: &[ArrayRef]) -> Result<ArrayRef> {
    use arrow::array::Float64Array;

    if args.len() != 2 {
        return Err(ExpressionError::InvalidArgumentCount {
            function: "pow".to_string(),
            expected: 2,
            actual: args.len(),
        });
    }

    let base = &args[0];
    let exp = &args[1];

    let base_floats =
        base.as_any()
            .downcast_ref::<Float64Array>()
            .ok_or(ExpressionError::TypeMismatch {
                expected: "Float64".to_string(),
                actual: format!("{:?}", base.data_type()),
            })?;

    let exp_floats =
        exp.as_any()
            .downcast_ref::<Float64Array>()
            .ok_or(ExpressionError::TypeMismatch {
                expected: "Float64".to_string(),
                actual: format!("{:?}", exp.data_type()),
            })?;

    let len = base_floats.len();
    let mut result = Vec::with_capacity(len);

    for i in 0..len {
        let b = base_floats.value(i);
        let e = if exp_floats.len() == 1 {
            exp_floats.value(0)
        } else {
            exp_floats.value(i)
        };
        result.push(b.powf(e));
    }

    Ok(Arc::new(Float64Array::from(result)))
}

fn floor_function(args: &[ArrayRef]) -> Result<ArrayRef> {
    use arrow::array::Float64Array;

    if args.len() != 1 {
        return Err(ExpressionError::InvalidArgumentCount {
            function: "floor".to_string(),
            expected: 1,
            actual: args.len(),
        });
    }

    let array = &args[0];
    let floats =
        array
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or(ExpressionError::TypeMismatch {
                expected: "Float64".to_string(),
                actual: format!("{:?}", array.data_type()),
            })?;

    let result: Vec<f64> = floats
        .iter()
        .map(|v| v.map(|x| x.floor()).unwrap_or(0.0))
        .collect();
    Ok(Arc::new(Float64Array::from(result)))
}

fn ceil_function(args: &[ArrayRef]) -> Result<ArrayRef> {
    use arrow::array::Float64Array;

    if args.len() != 1 {
        return Err(ExpressionError::InvalidArgumentCount {
            function: "ceil".to_string(),
            expected: 1,
            actual: args.len(),
        });
    }

    let array = &args[0];
    let floats =
        array
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or(ExpressionError::TypeMismatch {
                expected: "Float64".to_string(),
                actual: format!("{:?}", array.data_type()),
            })?;

    let result: Vec<f64> = floats
        .iter()
        .map(|v| v.map(|x| x.ceil()).unwrap_or(0.0))
        .collect();
    Ok(Arc::new(Float64Array::from(result)))
}

fn round_function(args: &[ArrayRef]) -> Result<ArrayRef> {
    use arrow::array::{Float64Array, Int64Array};

    if args.is_empty() || args.len() > 2 {
        return Err(ExpressionError::InvalidArgumentCount {
            function: "round".to_string(),
            expected: 1,
            actual: args.len(),
        });
    }

    let array = &args[0];
    let decimals = if args.len() == 2 {
        let dec_array = args[1].as_any().downcast_ref::<Int64Array>();
        dec_array
            .map(|arr| arr.value(0))
            .unwrap_or(0)
    } else {
        0
    };

    let floats =
        array
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or(ExpressionError::TypeMismatch {
                expected: "Float64".to_string(),
                actual: format!("{:?}", array.data_type()),
            })?;

    let multiplier = 10_f64.powi(decimals as i32);
    let result: Vec<f64> = floats
        .iter()
        .map(|v| {
            v.map(|x| (x * multiplier).round() / multiplier)
                .unwrap_or(0.0)
        })
        .collect();

    Ok(Arc::new(Float64Array::from(result)))
}

fn length_function(args: &[ArrayRef]) -> Result<ArrayRef> {
    use arrow::array::{Int64Array, StringArray};

    if args.len() != 1 {
        return Err(ExpressionError::InvalidArgumentCount {
            function: "length".to_string(),
            expected: 1,
            actual: args.len(),
        });
    }

    let array = &args[0];

    if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
        let result: Vec<i64> = strings
            .iter()
            .map(|s| s.map(|x| x.len() as i64).unwrap_or(0))
            .collect();
        Ok(Arc::new(Int64Array::from(result)))
    } else {
        let len = array.len() as i64;
        Ok(Arc::new(Int64Array::from(vec![len])))
    }
}

fn upper_function(args: &[ArrayRef]) -> Result<ArrayRef> {
    use arrow::array::StringArray;

    if args.len() != 1 {
        return Err(ExpressionError::InvalidArgumentCount {
            function: "upper".to_string(),
            expected: 1,
            actual: args.len(),
        });
    }

    let array = &args[0];
    let strings =
        array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(ExpressionError::TypeMismatch {
                expected: "Utf8".to_string(),
                actual: format!("{:?}", array.data_type()),
            })?;

    let result: Vec<Option<String>> = strings
        .iter()
        .map(|s| s.map(|x| x.to_uppercase()))
        .collect();
    Ok(Arc::new(StringArray::from(result)))
}

fn lower_function(args: &[ArrayRef]) -> Result<ArrayRef> {
    use arrow::array::StringArray;

    if args.len() != 1 {
        return Err(ExpressionError::InvalidArgumentCount {
            function: "lower".to_string(),
            expected: 1,
            actual: args.len(),
        });
    }

    let array = &args[0];
    let strings =
        array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(ExpressionError::TypeMismatch {
                expected: "Utf8".to_string(),
                actual: format!("{:?}", array.data_type()),
            })?;

    let result: Vec<Option<String>> = strings
        .iter()
        .map(|s| s.map(|x| x.to_lowercase()))
        .collect();
    Ok(Arc::new(StringArray::from(result)))
}

fn trim_function(args: &[ArrayRef]) -> Result<ArrayRef> {
    use arrow::array::StringArray;

    if args.len() != 1 {
        return Err(ExpressionError::InvalidArgumentCount {
            function: "trim".to_string(),
            expected: 1,
            actual: args.len(),
        });
    }

    let array = &args[0];
    let strings =
        array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(ExpressionError::TypeMismatch {
                expected: "Utf8".to_string(),
                actual: format!("{:?}", array.data_type()),
            })?;

    let result: Vec<Option<String>> = strings
        .iter()
        .map(|s| s.map(|x| x.trim().to_string()))
        .collect();
    Ok(Arc::new(StringArray::from(result)))
}

fn concat_function(args: &[ArrayRef]) -> Result<ArrayRef> {
    use arrow::array::StringArray;

    if args.len() < 2 {
        return Err(ExpressionError::InvalidArgumentCount {
            function: "concat".to_string(),
            expected: 2,
            actual: args.len(),
        });
    }

    let string_arrays: Vec<&StringArray> = args
        .iter()
        .map(|arr| {
            arr.as_any()
                .downcast_ref::<StringArray>()
                .ok_or(ExpressionError::TypeMismatch {
                    expected: "Utf8".to_string(),
                    actual: format!("{:?}", arr.data_type()),
                })
        })
        .collect::<Result<_>>()?;

    let len = string_arrays[0].len();
    let mut result = Vec::with_capacity(len);

    for i in 0..len {
        let mut concat_string = String::new();
        for arr in &string_arrays {
            if arr.is_valid(i) {
                concat_string.push_str(arr.value(i));
            }
        }
        result.push(Some(concat_string));
    }

    Ok(Arc::new(StringArray::from(result)))
}

fn substring_function(args: &[ArrayRef]) -> Result<ArrayRef> {
    use arrow::array::{Int64Array, StringArray};

    if args.len() != 3 {
        return Err(ExpressionError::InvalidArgumentCount {
            function: "substring".to_string(),
            expected: 3,
            actual: args.len(),
        });
    }

    let string_array =
        args[0]
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(ExpressionError::TypeMismatch {
                expected: "Utf8".to_string(),
                actual: format!("{:?}", args[0].data_type()),
            })?;

    let start_array =
        args[1]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or(ExpressionError::TypeMismatch {
                expected: "Int64".to_string(),
                actual: format!("{:?}", args[1].data_type()),
            })?;

    let length_array =
        args[2]
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or(ExpressionError::TypeMismatch {
                expected: "Int64".to_string(),
                actual: format!("{:?}", args[2].data_type()),
            })?;

    let len = string_array.len();
    let mut result = Vec::with_capacity(len);

    for i in 0..len {
        if string_array.is_valid(i) {
            let s = string_array.value(i);
            let start = if start_array.len() == 1 {
                start_array.value(0) as usize
            } else {
                start_array.value(i) as usize
            };
            let length = if length_array.len() == 1 {
                length_array.value(0) as usize
            } else {
                length_array.value(i) as usize
            };

            let end = (start + length).min(s.len());
            result.push(s.get(start..end).map(|x| x.to_string()));
        } else {
            result.push(None);
        }
    }

    Ok(Arc::new(StringArray::from(result)))
}

fn coalesce_function(args: &[ArrayRef]) -> Result<ArrayRef> {
    // Return the first non-null value from arguments
    // Simplified implementation - just return first argument
    if args.is_empty() {
        return Err(ExpressionError::InvalidArgumentCount {
            function: "coalesce".to_string(),
            expected: 1,
            actual: 0,
        });
    }

    Ok(args[0].clone())
}

// Simplified aggregate functions - actual aggregation requires state management
fn sum_function(args: &[ArrayRef]) -> Result<ArrayRef> {
    use arrow::array::{Float64Array, Int64Array};

    if args.len() != 1 {
        return Err(ExpressionError::InvalidArgumentCount {
            function: "sum".to_string(),
            expected: 1,
            actual: args.len(),
        });
    }

    let array = &args[0];

    if let Some(floats) = array.as_any().downcast_ref::<Float64Array>() {
        let sum: f64 = floats.iter().map(|v| v.unwrap_or(0.0)).sum();
        Ok(Arc::new(Float64Array::from(vec![sum])))
    } else if let Some(ints) = array.as_any().downcast_ref::<Int64Array>() {
        let sum: i64 = ints.iter().map(|v| v.unwrap_or(0)).sum();
        Ok(Arc::new(Float64Array::from(vec![sum as f64])))
    } else {
        Ok(Arc::new(Float64Array::from(vec![0.0])))
    }
}

fn avg_function(args: &[ArrayRef]) -> Result<ArrayRef> {
    use arrow::array::{Float64Array, Int64Array};

    if args.len() != 1 {
        return Err(ExpressionError::InvalidArgumentCount {
            function: "avg".to_string(),
            expected: 1,
            actual: args.len(),
        });
    }

    let array = &args[0];

    if let Some(floats) = array.as_any().downcast_ref::<Float64Array>() {
        let values: Vec<f64> = floats.iter().flatten().collect();
        let avg = if values.is_empty() {
            0.0
        } else {
            values.iter().sum::<f64>() / values.len() as f64
        };
        Ok(Arc::new(Float64Array::from(vec![avg])))
    } else if let Some(ints) = array.as_any().downcast_ref::<Int64Array>() {
        let values: Vec<i64> = ints.iter().flatten().collect();
        let avg = if values.is_empty() {
            0.0
        } else {
            values.iter().sum::<i64>() as f64 / values.len() as f64
        };
        Ok(Arc::new(Float64Array::from(vec![avg])))
    } else {
        Ok(Arc::new(Float64Array::from(vec![0.0])))
    }
}

fn min_function(args: &[ArrayRef]) -> Result<ArrayRef> {
    use arrow::array::{Float64Array, Int64Array};

    if args.len() != 1 {
        return Err(ExpressionError::InvalidArgumentCount {
            function: "min".to_string(),
            expected: 1,
            actual: args.len(),
        });
    }

    let array = &args[0];

    if let Some(floats) = array.as_any().downcast_ref::<Float64Array>() {
        let min = floats
            .iter()
            .flatten()
            .min_by(|a, b| a.partial_cmp(b).unwrap());
        Ok(Arc::new(Float64Array::from(vec![min.unwrap_or(0.0)])))
    } else if let Some(ints) = array.as_any().downcast_ref::<Int64Array>() {
        let min = ints.iter().flatten().min();
        Ok(Arc::new(Int64Array::from(vec![min.unwrap_or(0)])))
    } else {
        Ok(array.clone())
    }
}

fn max_function(args: &[ArrayRef]) -> Result<ArrayRef> {
    use arrow::array::{Float64Array, Int64Array};

    if args.len() != 1 {
        return Err(ExpressionError::InvalidArgumentCount {
            function: "max".to_string(),
            expected: 1,
            actual: args.len(),
        });
    }

    let array = &args[0];

    if let Some(floats) = array.as_any().downcast_ref::<Float64Array>() {
        let max = floats
            .iter()
            .flatten()
            .max_by(|a, b| a.partial_cmp(b).unwrap());
        Ok(Arc::new(Float64Array::from(vec![max.unwrap_or(0.0)])))
    } else if let Some(ints) = array.as_any().downcast_ref::<Int64Array>() {
        let max = ints.iter().flatten().max();
        Ok(Arc::new(Int64Array::from(vec![max.unwrap_or(0)])))
    } else {
        Ok(array.clone())
    }
}

fn count_function(args: &[ArrayRef]) -> Result<ArrayRef> {
    use arrow::array::Int64Array;

    if args.len() != 1 {
        return Err(ExpressionError::InvalidArgumentCount {
            function: "count".to_string(),
            expected: 1,
            actual: args.len(),
        });
    }

    let array = &args[0];
    let count = array.len() as i64;
    Ok(Arc::new(Int64Array::from(vec![count])))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, StringArray};

    #[test]
    fn test_function_registry() {
        let registry = FunctionRegistry::new();

        assert!(registry.has_function("abs"));
        assert!(registry.has_function("sqrt"));
        assert!(registry.has_function("upper"));
        assert!(registry.has_function("lower"));
        assert!(registry.has_function("concat"));
        assert!(registry.has_function("sum"));
        assert!(registry.has_function("avg"));

        assert!(!registry.has_function("nonexistent"));
    }

    #[test]
    fn test_abs_function() {
        let array = Arc::new(Float64Array::from(vec![-1.0, -2.0, 3.0, -4.0]));
        let result = abs_function(&[array]).unwrap();

        let floats = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(floats.value(0), 1.0);
        assert_eq!(floats.value(1), 2.0);
        assert_eq!(floats.value(2), 3.0);
        assert_eq!(floats.value(3), 4.0);
    }

    #[test]
    fn test_upper_function() {
        let array = Arc::new(StringArray::from(vec!["hello", "world"]));
        let result = upper_function(&[array]).unwrap();

        let strings = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.value(0), "HELLO");
        assert_eq!(strings.value(1), "WORLD");
    }

    #[test]
    fn test_concat_function() {
        let array1 = Arc::new(StringArray::from(vec!["hello", "foo"]));
        let array2 = Arc::new(StringArray::from(vec!["world", "bar"]));
        let result = concat_function(&[array1, array2]).unwrap();

        let strings = result.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(strings.value(0), "helloworld");
        assert_eq!(strings.value(1), "foobar");
    }

    #[test]
    fn test_sum_function() {
        let array = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]));
        let result = sum_function(&[array]).unwrap();

        let floats = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(floats.value(0), 10.0);
    }

    #[test]
    fn test_avg_function() {
        let array = Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]));
        let result = avg_function(&[array]).unwrap();

        let floats = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(floats.value(0), 2.5);
    }

    #[test]
    fn test_custom_function_registration() {
        let registry = FunctionRegistry::new();

        let custom_fn: FunctionSignature = |_args| Ok(Arc::new(Float64Array::from(vec![42.0])));

        let metadata = FunctionMetadata {
            name: "custom".to_string(),
            return_type: DataType::Float64,
            variadic: false,
            min_args: 0,
            max_args: 0,
        };

        assert!(registry
            .register_function("custom", custom_fn, metadata)
            .is_ok());
        assert!(registry.has_function("custom"));
    }
}
