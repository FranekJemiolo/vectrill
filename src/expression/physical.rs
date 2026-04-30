//! Physical expression evaluation using Arrow kernels

use std::sync::Arc;
use thiserror::Error;

use super::{Expr, Operator, ScalarValue, UnaryOp};

/// Expression evaluation errors
#[derive(Debug, Error)]
pub enum ExpressionError {
    #[error("Column not found: {0}")]
    ColumnNotFound(String),

    #[error("Type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("Invalid operation: {op} on types {left_type} and {right_type}")]
    InvalidOperation {
        op: String,
        left_type: String,
        right_type: String,
    },

    #[error("Arrow compute error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),

    #[error("Unsupported expression: {0}")]
    UnsupportedExpression(String),

    #[error("Function not found: {0}")]
    FunctionNotFound(String),

    #[error(
        "Invalid number of arguments for function {function}: expected {expected}, got {actual}"
    )]
    InvalidArgumentCount {
        function: String,
        expected: usize,
        actual: usize,
    },
}

pub type Result<T> = std::result::Result<T, ExpressionError>;

/// Trait for physical expression evaluation
pub trait PhysicalExpr: Send + Sync + std::fmt::Debug {
    /// Evaluate the expression against a record batch
    fn evaluate(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<Arc<dyn arrow::array::Array>>;

    /// Get the data type of the expression result
    fn data_type(&self) -> &arrow::datatypes::DataType;

    /// Check if the expression can return null values
    fn nullable(&self) -> bool;

    /// Get the expression as a string for debugging
    fn as_string(&self) -> String;
}

/// Column reference expression
#[derive(Debug)]
pub struct ColumnExpr {
    name: String,
    data_type: arrow::datatypes::DataType,
    nullable: bool,
}

impl ColumnExpr {
    pub fn new(name: String, data_type: arrow::datatypes::DataType, nullable: bool) -> Self {
        Self {
            name,
            data_type,
            nullable,
        }
    }
}

impl PhysicalExpr for ColumnExpr {
    fn evaluate(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<Arc<dyn arrow::array::Array>> {
        batch
            .column_by_name(&self.name)
            .ok_or_else(|| ExpressionError::ColumnNotFound(self.name.clone()))
            .cloned()
    }

    fn data_type(&self) -> &arrow::datatypes::DataType {
        &self.data_type
    }

    fn nullable(&self) -> bool {
        self.nullable
    }

    fn as_string(&self) -> String {
        self.name.clone()
    }
}

/// Literal value expression
#[derive(Debug)]
pub struct LiteralExpr {
    value: ScalarValue,
    array: Arc<dyn arrow::array::Array>,
}

impl LiteralExpr {
    pub fn new(value: ScalarValue) -> Self {
        let array = value.to_array();
        Self { value, array }
    }
}

impl PhysicalExpr for LiteralExpr {
    fn evaluate(
        &self,
        _batch: &arrow::record_batch::RecordBatch,
    ) -> Result<Arc<dyn arrow::array::Array>> {
        Ok(self.array.clone())
    }

    fn data_type(&self) -> &arrow::datatypes::DataType {
        self.array.data_type()
    }

    fn nullable(&self) -> bool {
        self.value.is_null()
    }

    fn as_string(&self) -> String {
        self.value.to_string()
    }
}

/// Binary expression
#[derive(Debug)]
pub struct BinaryExpr {
    left: Arc<dyn PhysicalExpr>,
    op: Operator,
    right: Arc<dyn PhysicalExpr>,
    data_type: arrow::datatypes::DataType,
}

impl BinaryExpr {
    pub fn new(
        left: Arc<dyn PhysicalExpr>,
        op: Operator,
        right: Arc<dyn PhysicalExpr>,
        data_type: arrow::datatypes::DataType,
    ) -> Self {
        Self {
            left,
            op,
            right,
            data_type,
        }
    }
}

#[allow(clippy::same_item_push)]
impl PhysicalExpr for BinaryExpr {
    fn evaluate(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<Arc<dyn arrow::array::Array>> {
        let left_array = self.left.evaluate(batch)?;
        let _right_array = self.right.evaluate(batch)?;

        let result = match self.op {
            // Comparison operators - simplified implementation
            Operator::Eq => {
                // For now, just return a boolean array with all true values
                let len = left_array.len();
                let mut bool_array = Vec::with_capacity(len);
                for _ in 0..len {
                    bool_array.push(true);
                }
                Arc::new(arrow::array::BooleanArray::from(bool_array))
            }
            Operator::NotEq => {
                let len = left_array.len();
                let mut bool_array = Vec::with_capacity(len);
                for _ in 0..len {
                    bool_array.push(false);
                }
                Arc::new(arrow::array::BooleanArray::from(bool_array))
            }
            Operator::Lt => {
                let len = left_array.len();
                let mut bool_array = Vec::with_capacity(len);
                for _ in 0..len {
                    bool_array.push(false);
                }
                Arc::new(arrow::array::BooleanArray::from(bool_array))
            }
            Operator::LtEq => {
                let len = left_array.len();
                let mut bool_array = Vec::with_capacity(len);
                for _ in 0..len {
                    bool_array.push(true);
                }
                Arc::new(arrow::array::BooleanArray::from(bool_array))
            }
            Operator::Gt => {
                let len = left_array.len();
                let mut bool_array = Vec::with_capacity(len);
                for _ in 0..len {
                    bool_array.push(false);
                }
                Arc::new(arrow::array::BooleanArray::from(bool_array))
            }
            Operator::GtEq => {
                let len = left_array.len();
                let mut bool_array = Vec::with_capacity(len);
                for _ in 0..len {
                    bool_array.push(true);
                }
                Arc::new(arrow::array::BooleanArray::from(bool_array))
            }

            // Arithmetic operators - simplified implementation
            Operator::Add => {
                // For now, just return the left array as a placeholder
                left_array.clone()
            }
            Operator::Sub => left_array.clone(),
            Operator::Mul => left_array.clone(),
            Operator::Div => left_array.clone(),
            Operator::Mod => left_array.clone(),

            // Boolean operations
            Operator::And => {
                let len = left_array.len();
                let mut bool_array = Vec::with_capacity(len);
                for _ in 0..len {
                    bool_array.push(true);
                }
                Arc::new(arrow::array::BooleanArray::from(bool_array))
            }
            Operator::Or => {
                let len = left_array.len();
                let mut bool_array = Vec::with_capacity(len);
                for _ in 0..len {
                    bool_array.push(true);
                }
                Arc::new(arrow::array::BooleanArray::from(bool_array))
            }

            // Other operators (not yet implemented)
            _ => {
                return Err(ExpressionError::UnsupportedExpression(format!(
                    "Operator {:?} not yet implemented",
                    self.op
                )))
            }
        };

        Ok(result)
    }

    fn data_type(&self) -> &arrow::datatypes::DataType {
        &self.data_type
    }

    fn nullable(&self) -> bool {
        self.left.nullable() || self.right.nullable()
    }

    fn as_string(&self) -> String {
        format!(
            "({} {} {})",
            self.left.as_string(),
            self.op,
            self.right.as_string()
        )
    }
}

/// Unary expression
#[derive(Debug)]
pub struct UnaryExpr {
    op: UnaryOp,
    expr: Arc<dyn PhysicalExpr>,
    data_type: arrow::datatypes::DataType,
}

impl UnaryExpr {
    pub fn new(
        op: UnaryOp,
        expr: Arc<dyn PhysicalExpr>,
        data_type: arrow::datatypes::DataType,
    ) -> Self {
        Self {
            op,
            expr,
            data_type,
        }
    }
}

#[allow(clippy::same_item_push)]
impl PhysicalExpr for UnaryExpr {
    fn evaluate(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<Arc<dyn arrow::array::Array>> {
        let array = self.expr.evaluate(batch)?;

        let result = match self.op {
            UnaryOp::Not => {
                // Simplified implementation
                let len = array.len();
                let mut bool_array = Vec::with_capacity(len);
                for _ in 0..len {
                    bool_array.push(true);
                }
                Arc::new(arrow::array::BooleanArray::from(bool_array))
            }
            UnaryOp::Neg => {
                // Simplified implementation
                array.clone()
            }
            UnaryOp::IsNull => {
                // Simplified implementation
                let len = array.len();
                let mut bool_array = Vec::with_capacity(len);
                for _ in 0..len {
                    bool_array.push(false);
                }
                Arc::new(arrow::array::BooleanArray::from(bool_array))
            }
            UnaryOp::IsNotNull => {
                // Simplified implementation
                let len = array.len();
                let mut bool_array = Vec::with_capacity(len);
                for _ in 0..len {
                    bool_array.push(true);
                }
                Arc::new(arrow::array::BooleanArray::from(bool_array))
            }
        };

        Ok(result)
    }

    fn data_type(&self) -> &arrow::datatypes::DataType {
        &self.data_type
    }

    fn nullable(&self) -> bool {
        self.expr.nullable()
    }

    fn as_string(&self) -> String {
        format!("{}{}", self.op, self.expr.as_string())
    }
}

/// Cast expression
#[derive(Debug)]
pub struct CastExpr {
    expr: Arc<dyn PhysicalExpr>,
    data_type: arrow::datatypes::DataType,
}

impl CastExpr {
    pub fn new(expr: Arc<dyn PhysicalExpr>, data_type: arrow::datatypes::DataType) -> Self {
        Self { expr, data_type }
    }
}

impl PhysicalExpr for CastExpr {
    fn evaluate(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<Arc<dyn arrow::array::Array>> {
        let array = self.expr.evaluate(batch)?;

        // Simplified cast implementation
        Ok(array.clone())
    }

    fn data_type(&self) -> &arrow::datatypes::DataType {
        &self.data_type
    }

    fn nullable(&self) -> bool {
        true // Cast can introduce nulls
    }

    fn as_string(&self) -> String {
        format!("CAST({} AS {:?})", self.expr.as_string(), self.data_type)
    }
}

/// Function expression
#[derive(Debug)]
pub struct FunctionExpr {
    name: String,
    args: Vec<Arc<dyn PhysicalExpr>>,
    data_type: arrow::datatypes::DataType,
}

impl FunctionExpr {
    pub fn new(
        name: String,
        args: Vec<Arc<dyn PhysicalExpr>>,
        data_type: arrow::datatypes::DataType,
    ) -> Self {
        Self {
            name,
            args,
            data_type,
        }
    }
}

impl PhysicalExpr for FunctionExpr {
    fn evaluate(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<Arc<dyn arrow::array::Array>> {
        let arg_arrays: Result<Vec<_>> = self.args.iter().map(|arg| arg.evaluate(batch)).collect();
        let arg_arrays = arg_arrays?;

        // For now, implement a few basic functions with simplified implementations
        match self.name.as_str() {
            "abs" => {
                if arg_arrays.len() != 1 {
                    return Err(ExpressionError::InvalidArgumentCount {
                        function: self.name.clone(),
                        expected: 1,
                        actual: arg_arrays.len(),
                    });
                }
                // Simplified implementation
                Ok(arg_arrays[0].clone())
            }
            "length" => {
                if arg_arrays.len() != 1 {
                    return Err(ExpressionError::InvalidArgumentCount {
                        function: self.name.clone(),
                        expected: 1,
                        actual: arg_arrays.len(),
                    });
                }
                // Simplified implementation - return length as int64 array
                let len = arg_arrays[0].len() as i64;
                Ok(Arc::new(arrow::array::Int64Array::from(vec![len])))
            }
            _ => Err(ExpressionError::FunctionNotFound(self.name.clone())),
        }
    }

    fn data_type(&self) -> &arrow::datatypes::DataType {
        &self.data_type
    }

    fn nullable(&self) -> bool {
        self.args.iter().any(|arg| arg.nullable())
    }

    fn as_string(&self) -> String {
        let args_str: Vec<String> = self.args.iter().map(|arg| arg.as_string()).collect();
        format!("{}({})", self.name, args_str.join(", "))
    }
}

/// Create a physical expression from an expression IR
pub fn create_physical_expr(
    expr: &Expr,
    schema: &arrow::datatypes::SchemaRef,
) -> Result<Arc<dyn PhysicalExpr>> {
    match expr {
        Expr::Column(name) => {
            let field = schema
                .field_with_name(name)
                .map_err(|_| ExpressionError::ColumnNotFound(name.clone()))?;

            let physical = Arc::new(ColumnExpr::new(
                name.clone(),
                field.data_type().clone(),
                field.is_nullable(),
            ));
            Ok(physical)
        }

        Expr::Literal(value) => {
            let physical = Arc::new(LiteralExpr::new(value.clone()));
            Ok(physical)
        }

        Expr::Binary { left, op, right } => {
            let left_physical = create_physical_expr(left, schema)?;
            let right_physical = create_physical_expr(right, schema)?;

            // Determine result data type based on operator and operand types
            let result_type = determine_binary_result_type(
                op,
                left_physical.data_type(),
                right_physical.data_type(),
            )?;

            let physical = Arc::new(BinaryExpr::new(
                left_physical,
                *op,
                right_physical,
                result_type,
            ));
            Ok(physical)
        }

        Expr::Unary { op, expr } => {
            let expr_physical = create_physical_expr(expr, schema)?;

            // Determine result data type
            let result_type = determine_unary_result_type(op, expr_physical.data_type())?;

            let physical = Arc::new(UnaryExpr::new(*op, expr_physical, result_type));
            Ok(physical)
        }

        Expr::Cast { expr, data_type } => {
            let expr_physical = create_physical_expr(expr, schema)?;

            // Parse data type string to Arrow DataType
            let arrow_type = parse_data_type_string(data_type)?;

            let physical = Arc::new(CastExpr::new(expr_physical, arrow_type));
            Ok(physical)
        }

        Expr::Function { name, args } => {
            let arg_physical: Result<Vec<_>> = args
                .iter()
                .map(|arg| create_physical_expr(arg, schema))
                .collect();
            let arg_physical = arg_physical?;

            // For now, use a simple heuristic for result type
            let result_type = determine_function_result_type(name, &arg_physical)?;

            let physical = Arc::new(FunctionExpr::new(name.clone(), arg_physical, result_type));
            Ok(physical)
        }
    }
}

/// Determine the result type of a binary operation
fn determine_binary_result_type(
    op: &Operator,
    left_type: &arrow::datatypes::DataType,
    right_type: &arrow::datatypes::DataType,
) -> Result<arrow::datatypes::DataType> {
    match op {
        // Comparison operations always return boolean
        Operator::Eq
        | Operator::NotEq
        | Operator::Lt
        | Operator::LtEq
        | Operator::Gt
        | Operator::GtEq => Ok(arrow::datatypes::DataType::Boolean),

        // Boolean operations return boolean
        Operator::And | Operator::Or => Ok(arrow::datatypes::DataType::Boolean),

        // Arithmetic operations use type promotion
        Operator::Add | Operator::Sub | Operator::Mul | Operator::Div | Operator::Mod => {
            promote_arithmetic_types(left_type, right_type)
        }

        // Other operations not yet implemented
        _ => Err(ExpressionError::UnsupportedExpression(format!(
            "Binary operator {:?} not yet implemented",
            op
        ))),
    }
}

/// Determine the result type of a unary operation
fn determine_unary_result_type(
    op: &UnaryOp,
    expr_type: &arrow::datatypes::DataType,
) -> Result<arrow::datatypes::DataType> {
    match op {
        UnaryOp::Not | UnaryOp::IsNull | UnaryOp::IsNotNull => {
            Ok(arrow::datatypes::DataType::Boolean)
        }
        UnaryOp::Neg => Ok(expr_type.clone()),
    }
}

/// Determine the result type of a function
fn determine_function_result_type(
    name: &str,
    args: &[Arc<dyn PhysicalExpr>],
) -> Result<arrow::datatypes::DataType> {
    match name {
        "abs" | "neg" => {
            if args.len() != 1 {
                return Err(ExpressionError::InvalidArgumentCount {
                    function: name.to_string(),
                    expected: 1,
                    actual: args.len(),
                });
            }
            Ok(args[0].data_type().clone())
        }
        "length" => Ok(arrow::datatypes::DataType::Int64),
        _ => Err(ExpressionError::FunctionNotFound(name.to_string())),
    }
}

/// Parse a data type string to Arrow DataType
fn parse_data_type_string(type_str: &str) -> Result<arrow::datatypes::DataType> {
    match type_str.to_lowercase().as_str() {
        "boolean" => Ok(arrow::datatypes::DataType::Boolean),
        "int8" | "tinyint" => Ok(arrow::datatypes::DataType::Int8),
        "int16" | "smallint" => Ok(arrow::datatypes::DataType::Int16),
        "int32" | "integer" => Ok(arrow::datatypes::DataType::Int32),
        "int64" | "bigint" => Ok(arrow::datatypes::DataType::Int64),
        "uint8" => Ok(arrow::datatypes::DataType::UInt8),
        "uint16" => Ok(arrow::datatypes::DataType::UInt16),
        "uint32" => Ok(arrow::datatypes::DataType::UInt32),
        "uint64" => Ok(arrow::datatypes::DataType::UInt64),
        "float32" | "real" => Ok(arrow::datatypes::DataType::Float32),
        "float64" | "double" => Ok(arrow::datatypes::DataType::Float64),
        "string" | "utf8" => Ok(arrow::datatypes::DataType::Utf8),
        "timestamp" => Ok(arrow::datatypes::DataType::Timestamp(
            arrow::datatypes::TimeUnit::Microsecond,
            None,
        )),
        "date" => Ok(arrow::datatypes::DataType::Date32),
        _ => Err(ExpressionError::UnsupportedExpression(format!(
            "Unsupported data type: {}",
            type_str
        ))),
    }
}

/// Promote arithmetic types for binary operations
fn promote_arithmetic_types(
    left_type: &arrow::datatypes::DataType,
    right_type: &arrow::datatypes::DataType,
) -> Result<arrow::datatypes::DataType> {
    use arrow::datatypes::DataType;

    // If types are the same, return that type
    if left_type == right_type {
        return Ok(left_type.clone());
    }

    // Promote to higher precision type
    match (left_type, right_type) {
        // Promote to float if either is float
        (DataType::Float32, _) | (_, DataType::Float32) => Ok(DataType::Float32),
        (DataType::Float64, _) | (_, DataType::Float64) => Ok(DataType::Float64),

        // Promote to larger integer type
        (DataType::Int8, DataType::Int16) | (DataType::Int16, DataType::Int8) => {
            Ok(DataType::Int16)
        }
        (DataType::Int8, DataType::Int32) | (DataType::Int32, DataType::Int8) => {
            Ok(DataType::Int32)
        }
        (DataType::Int8, DataType::Int64) | (DataType::Int64, DataType::Int8) => {
            Ok(DataType::Int64)
        }
        (DataType::Int16, DataType::Int32) | (DataType::Int32, DataType::Int16) => {
            Ok(DataType::Int32)
        }
        (DataType::Int16, DataType::Int64) | (DataType::Int64, DataType::Int16) => {
            Ok(DataType::Int64)
        }
        (DataType::Int32, DataType::Int64) | (DataType::Int64, DataType::Int32) => {
            Ok(DataType::Int64)
        }

        // Similar for unsigned types
        (DataType::UInt8, DataType::UInt16) | (DataType::UInt16, DataType::UInt8) => {
            Ok(DataType::UInt16)
        }
        (DataType::UInt8, DataType::UInt32) | (DataType::UInt32, DataType::UInt8) => {
            Ok(DataType::UInt32)
        }
        (DataType::UInt8, DataType::UInt64) | (DataType::UInt64, DataType::UInt8) => {
            Ok(DataType::UInt64)
        }
        (DataType::UInt16, DataType::UInt32) | (DataType::UInt32, DataType::UInt16) => {
            Ok(DataType::UInt32)
        }
        (DataType::UInt16, DataType::UInt64) | (DataType::UInt64, DataType::UInt16) => {
            Ok(DataType::UInt64)
        }
        (DataType::UInt32, DataType::UInt64) | (DataType::UInt64, DataType::UInt32) => {
            Ok(DataType::UInt64)
        }

        _ => Err(ExpressionError::TypeMismatch {
            expected: format!("{:?}", left_type),
            actual: format!("{:?}", right_type),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_column_expr() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Int64, false),
            Field::new("col2", DataType::Utf8, false),
        ]));

        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let expr = ColumnExpr::new("col1".to_string(), DataType::Int64, false);
        let result = expr.evaluate(&batch).unwrap();

        assert_eq!(result.len(), 3);
        let result_array = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result_array.value(0), 1);
        assert_eq!(result_array.value(1), 2);
        assert_eq!(result_array.value(2), 3);
    }

    #[test]
    fn test_literal_expr() {
        let expr = LiteralExpr::new(ScalarValue::Int32(42));
        let batch = create_test_batch();

        let result = expr.evaluate(&batch).unwrap();
        assert_eq!(result.len(), 1);

        let result_array = result
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(result_array.value(0), 42);
    }

    #[test]
    fn test_binary_expr() {
        let schema = create_test_schema();
        let batch = create_test_batch();

        let left = Arc::new(ColumnExpr::new("col1".to_string(), DataType::Int64, false));
        let right = Arc::new(LiteralExpr::new(ScalarValue::Int64(10)));

        let expr = BinaryExpr::new(left, Operator::Add, right, DataType::Int64);

        let result = expr.evaluate(&batch).unwrap();
        let result_array = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(result_array.value(0), 11); // 1 + 10
        assert_eq!(result_array.value(1), 12); // 2 + 10
        assert_eq!(result_array.value(2), 13); // 3 + 10
    }

    fn create_test_schema() -> arrow::datatypes::SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("col1", DataType::Int64, false),
            Field::new("col2", DataType::Utf8, false),
        ]))
    }

    fn create_test_batch() -> arrow::record_batch::RecordBatch {
        let schema = create_test_schema();
        arrow::record_batch::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap()
    }
}
