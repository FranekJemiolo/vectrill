//! Physical expression evaluation using Arrow kernels

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use thiserror::Error;

use super::{global_registry, Expr, Operator, ScalarValue, UnaryOp};
use arrow::array::ArrayRef;

/// Expression evaluation errors
#[derive(Debug, Error)]
pub enum ExpressionError {
    #[error("Expression cache miss: {0}")]
    CacheMiss(String),

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

/// Expression cache for performance optimization
pub struct ExpressionCache {
    cache: Mutex<HashMap<String, Arc<dyn PhysicalExpr>>>,
    max_size: usize,
}

impl ExpressionCache {
    pub fn new(max_size: usize) -> Self {
        Self {
            cache: Mutex::new(HashMap::new()),
            max_size,
        }
    }

    /// Get a cached expression or create a new one
    pub fn get_or_create<F>(&self, key: &str, create_fn: F) -> Result<Arc<dyn PhysicalExpr>>
    where
        F: FnOnce() -> Result<Arc<dyn PhysicalExpr>>,
    {
        {
            let cache = self.cache.lock().unwrap();
            if let Some(expr) = cache.get(key) {
                return Ok(expr.clone());
            }
        }

        // Create new expression outside the lock to avoid deadlocks on recursive expressions
        let expr = create_fn()?;

        let mut cache = self.cache.lock().unwrap();
        if cache.len() < self.max_size {
            cache.insert(key.to_string(), expr.clone());
        }

        Ok(expr)
    }

    /// Clear the cache
    pub fn clear(&self) {
        let mut cache = self.cache.lock().unwrap();
        cache.clear();
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        let cache = self.cache.lock().unwrap();
        CacheStats {
            size: cache.len(),
            max_size: self.max_size,
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub size: usize,
    pub max_size: usize,
}

/// Global expression cache instance
static GLOBAL_EXPRESSION_CACHE: once_cell::sync::Lazy<Arc<ExpressionCache>> =
    once_cell::sync::Lazy::new(|| Arc::new(ExpressionCache::new(1000)));

/// Get the global expression cache
pub fn global_expression_cache() -> Arc<ExpressionCache> {
    GLOBAL_EXPRESSION_CACHE.clone()
}

/// Performance counters for expression operations
pub struct ExpressionCounters {
    pub evaluations: std::sync::atomic::AtomicU64,
    pub cache_hits: std::sync::atomic::AtomicU64,
    pub cache_misses: std::sync::atomic::AtomicU64,
}

impl Default for ExpressionCounters {
    fn default() -> Self {
        Self::new()
    }
}

impl ExpressionCounters {
    pub fn new() -> Self {
        Self {
            evaluations: std::sync::atomic::AtomicU64::new(0),
            cache_hits: std::sync::atomic::AtomicU64::new(0),
            cache_misses: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn record_evaluation(&self) {
        self.evaluations
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn record_cache_hit(&self) {
        self.cache_hits
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn record_cache_miss(&self) {
        self.cache_misses
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn get_stats(&self) -> ExpressionStats {
        ExpressionStats {
            total_evaluations: self.evaluations.load(std::sync::atomic::Ordering::Relaxed),
            cache_hits: self.cache_hits.load(std::sync::atomic::Ordering::Relaxed),
            cache_misses: self.cache_misses.load(std::sync::atomic::Ordering::Relaxed),
        }
    }
}

/// Expression performance statistics
#[derive(Debug, Clone)]
pub struct ExpressionStats {
    pub total_evaluations: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

impl ExpressionStats {
    pub fn cache_hit_rate(&self) -> f64 {
        if self.cache_hits + self.cache_misses == 0 {
            0.0
        } else {
            self.cache_hits as f64 / (self.cache_hits + self.cache_misses) as f64
        }
    }
}

/// Global expression counters
static GLOBAL_EXPRESSION_COUNTERS: once_cell::sync::Lazy<std::sync::Mutex<ExpressionCounters>> =
    once_cell::sync::Lazy::new(|| std::sync::Mutex::new(ExpressionCounters::new()));

/// Get the global expression counters
pub fn global_expression_counters() -> &'static std::sync::Mutex<ExpressionCounters> {
    &GLOBAL_EXPRESSION_COUNTERS
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
        // Record evaluation for performance monitoring
        let counters = global_expression_counters();
        counters.lock().unwrap().record_evaluation();

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

/// Broadcast an array to a target length if needed (e.g. for scalar arrays of length 1)
pub fn broadcast_if_needed(array: &ArrayRef, target_len: usize) -> Result<ArrayRef> {
    if array.len() == target_len || target_len == 0 {
        return Ok(array.clone());
    }
    if array.len() == 1 {
        let indices = arrow::array::UInt32Array::from(vec![0u32; target_len]);
        let taken = arrow::compute::take(array.as_ref(), &indices, None)
            .map_err(ExpressionError::ArrowError)?;
        return Ok(taken);
    }
    Err(ExpressionError::ArrowError(
        arrow::error::ArrowError::InvalidArgumentError(format!(
            "Array length mismatch: cannot broadcast length {} to {}",
            array.len(),
            target_len
        )),
    ))
}

/// Align operands by broadcasting and promoting types
pub fn align_operands(left: &ArrayRef, right: &ArrayRef) -> Result<(ArrayRef, ArrayRef)> {
    let target_len = left.len().max(right.len());
    let left_b = broadcast_if_needed(left, target_len)?;
    let right_b = broadcast_if_needed(right, target_len)?;
    if left_b.data_type() == right_b.data_type() {
        return Ok((left_b, right_b));
    }
    let target_type = promote_arithmetic_types(left_b.data_type(), right_b.data_type())?;
    let left_casted = if left_b.data_type() != &target_type {
        arrow_cast::cast(&left_b, &target_type).map_err(ExpressionError::ArrowError)?
    } else {
        left_b
    };
    let right_casted = if right_b.data_type() != &target_type {
        arrow_cast::cast(&right_b, &target_type).map_err(ExpressionError::ArrowError)?
    } else {
        right_b
    };
    Ok((left_casted, right_casted))
}

impl PhysicalExpr for BinaryExpr {
    fn evaluate(
        &self,
        batch: &arrow::record_batch::RecordBatch,
    ) -> Result<Arc<dyn arrow::array::Array>> {
        // Record evaluation for performance monitoring
        let counters = global_expression_counters();
        counters.lock().unwrap().record_evaluation();

        let left_array = self.left.evaluate(batch)?;
        let right_array = self.right.evaluate(batch)?;

        let result: Arc<dyn arrow::array::Array> = match self.op {
            // Comparison operators
            Operator::Eq => {
                let (l, r) = align_operands(&left_array, &right_array)?;
                Arc::new(arrow_ord::cmp::eq(&l, &r).map_err(ExpressionError::ArrowError)?) as _
            }
            Operator::NotEq => {
                let (l, r) = align_operands(&left_array, &right_array)?;
                Arc::new(arrow_ord::cmp::neq(&l, &r).map_err(ExpressionError::ArrowError)?) as _
            }
            Operator::Lt => {
                let (l, r) = align_operands(&left_array, &right_array)?;
                Arc::new(arrow_ord::cmp::lt(&l, &r).map_err(ExpressionError::ArrowError)?) as _
            }
            Operator::LtEq => {
                let (l, r) = align_operands(&left_array, &right_array)?;
                Arc::new(arrow_ord::cmp::lt_eq(&l, &r).map_err(ExpressionError::ArrowError)?) as _
            }
            Operator::Gt => {
                let (l, r) = align_operands(&left_array, &right_array)?;
                Arc::new(arrow_ord::cmp::gt(&l, &r).map_err(ExpressionError::ArrowError)?) as _
            }
            Operator::GtEq => {
                let (l, r) = align_operands(&left_array, &right_array)?;
                Arc::new(arrow_ord::cmp::gt_eq(&l, &r).map_err(ExpressionError::ArrowError)?) as _
            }

            // Arithmetic operators
            Operator::Add => {
                let (l, r) = align_operands(&left_array, &right_array)?;
                Arc::new(arrow_arith::numeric::add(&l, &r).map_err(ExpressionError::ArrowError)?)
                    as _
            }
            Operator::Sub => {
                let (l, r) = align_operands(&left_array, &right_array)?;
                Arc::new(arrow_arith::numeric::sub(&l, &r).map_err(ExpressionError::ArrowError)?)
                    as _
            }
            Operator::Mul => {
                let (l, r) = align_operands(&left_array, &right_array)?;
                Arc::new(arrow_arith::numeric::mul(&l, &r).map_err(ExpressionError::ArrowError)?)
                    as _
            }
            Operator::Div => {
                let (l, r) = align_operands(&left_array, &right_array)?;
                Arc::new(arrow_arith::numeric::div(&l, &r).map_err(ExpressionError::ArrowError)?)
                    as _
            }
            Operator::Mod => {
                let (l, r) = align_operands(&left_array, &right_array)?;
                Arc::new(arrow_arith::numeric::rem(&l, &r).map_err(ExpressionError::ArrowError)?)
                    as _
            }

            // Boolean operations
            Operator::And => {
                let (l, r) = align_operands(&left_array, &right_array)?;
                let left_bool = l
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .ok_or_else(|| ExpressionError::TypeMismatch {
                        expected: "Boolean".to_string(),
                        actual: format!("{:?}", l.data_type()),
                    })?;
                let right_bool = r
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .ok_or_else(|| ExpressionError::TypeMismatch {
                        expected: "Boolean".to_string(),
                        actual: format!("{:?}", r.data_type()),
                    })?;
                Arc::new(
                    arrow::compute::kernels::boolean::and(left_bool, right_bool)
                        .map_err(ExpressionError::ArrowError)?,
                ) as _
            }
            Operator::Or => {
                let (l, r) = align_operands(&left_array, &right_array)?;
                let left_bool = l
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .ok_or_else(|| ExpressionError::TypeMismatch {
                        expected: "Boolean".to_string(),
                        actual: format!("{:?}", l.data_type()),
                    })?;
                let right_bool = r
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .ok_or_else(|| ExpressionError::TypeMismatch {
                        expected: "Boolean".to_string(),
                        actual: format!("{:?}", r.data_type()),
                    })?;
                Arc::new(
                    arrow::compute::kernels::boolean::or(left_bool, right_bool)
                        .map_err(ExpressionError::ArrowError)?,
                ) as _
            }

            // For other operators, return the left array as placeholder
            _ => left_array,
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
        let counters = global_expression_counters();
        counters.lock().unwrap().record_evaluation();

        let array = self.expr.evaluate(batch)?;

        let result: Arc<dyn arrow::array::Array> = match self.op {
            UnaryOp::Not => {
                let bool_array = array
                    .as_any()
                    .downcast_ref::<arrow::array::BooleanArray>()
                    .ok_or_else(|| ExpressionError::TypeMismatch {
                        expected: "Boolean".to_string(),
                        actual: format!("{:?}", array.data_type()),
                    })?;
                Arc::new(
                    arrow::compute::kernels::boolean::not(bool_array)
                        .map_err(ExpressionError::ArrowError)?,
                ) as _
            }
            UnaryOp::Neg => {
                Arc::new(arrow_arith::numeric::neg(&array).map_err(ExpressionError::ArrowError)?)
                    as _
            }
            UnaryOp::IsNull => {
                Arc::new(arrow::compute::is_null(&array).map_err(ExpressionError::ArrowError)?) as _
            }
            UnaryOp::IsNotNull => {
                Arc::new(arrow::compute::is_not_null(&array).map_err(ExpressionError::ArrowError)?)
                    as _
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
        // Record evaluation for performance monitoring
        let counters = global_expression_counters();
        counters.lock().unwrap().record_evaluation();

        let expr_array = self.expr.evaluate(batch)?;
        let result =
            arrow_cast::cast(&expr_array, &self.data_type).map_err(ExpressionError::ArrowError)?;
        Ok(result)
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
        // Record evaluation for performance monitoring
        let counters = global_expression_counters();
        counters.lock().unwrap().record_evaluation();

        let args: Result<Vec<_>> = self.args.iter().map(|arg| arg.evaluate(batch)).collect();
        let arg_arrays = args?;

        // Use the global function registry to look up and execute the function
        if let Some((func, metadata)) = global_registry().get_function(&self.name) {
            // Validate argument count
            if !metadata.variadic {
                if arg_arrays.len() < metadata.min_args || arg_arrays.len() > metadata.max_args {
                    return Err(ExpressionError::InvalidArgumentCount {
                        function: self.name.clone(),
                        expected: metadata.min_args,
                        actual: arg_arrays.len(),
                    });
                }
            } else {
                if arg_arrays.len() < metadata.min_args {
                    return Err(ExpressionError::InvalidArgumentCount {
                        function: self.name.clone(),
                        expected: metadata.min_args,
                        actual: arg_arrays.len(),
                    });
                }
            }

            // Execute the function
            func(&arg_arrays)
        } else {
            Err(ExpressionError::FunctionNotFound(self.name.clone()))
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

/// Helper to construct a schema signature for cache keys
fn schema_signature(schema: &arrow::datatypes::Schema) -> String {
    let mut sig = String::new();
    for f in schema.fields() {
        sig.push_str(f.name());
        sig.push(':');
        sig.push_str(&format!("{:?}", f.data_type()));
        if f.is_nullable() {
            sig.push('?');
        }
        sig.push(';');
    }
    sig
}

/// Create a physical expression from an expression IR with caching
pub fn create_physical_expr(
    expr: &Expr,
    schema: &arrow::datatypes::SchemaRef,
) -> Result<Arc<dyn PhysicalExpr>> {
    // Create cache key combining schema signature and expression string
    let cache_key = format!("{}::{}", schema_signature(schema), expr.as_string());

    // Use global cache for performance optimization
    let cache = global_expression_cache();

    cache.get_or_create(&cache_key, || create_physical_expr_internal(expr, schema))
}

/// Internal function to create physical expression without caching
fn create_physical_expr_internal(
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
            let left_physical = create_physical_expr_internal(left, schema)?;
            let right_physical = create_physical_expr_internal(right, schema)?;

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
            let expr_physical = create_physical_expr_internal(expr, schema)?;

            // Determine result data type
            let result_type = determine_unary_result_type(op, expr_physical.data_type())?;

            let physical = Arc::new(UnaryExpr::new(*op, expr_physical, result_type));
            Ok(physical)
        }

        Expr::Cast { expr, data_type } => {
            let expr_physical = create_physical_expr_internal(expr, schema)?;

            // Parse data type string to Arrow DataType
            let arrow_type = parse_data_type_string(data_type)?;

            let physical = Arc::new(CastExpr::new(expr_physical, arrow_type));
            Ok(physical)
        }

        Expr::Function { name, args } => {
            let arg_physical: Result<Vec<_>> = args
                .iter()
                .map(|arg| create_physical_expr_internal(arg, schema))
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
    use arrow::array::{Array, Int64Array, StringArray};
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
        let _schema = create_test_schema();
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

    #[test]
    fn test_nested_expression_no_deadlock() {
        let schema = create_test_schema();
        let batch = create_test_batch();

        // (col1 + 10) > 12 -> evaluates to [false, false, true]
        let ast = Expr::Binary {
            left: Box::new(Expr::Binary {
                left: Box::new(Expr::Column("col1".to_string())),
                op: Operator::Add,
                right: Box::new(Expr::Literal(ScalarValue::Int64(10))),
            }),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(ScalarValue::Int64(12))),
        };

        let physical =
            create_physical_expr(&ast, &schema).expect("Should compile without deadlock");
        let result = physical.evaluate(&batch).expect("Should evaluate");
        let bools = result
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();

        assert_eq!(bools.len(), 3);
        assert!(!bools.value(0)); // 11 > 12 -> false
        assert!(!bools.value(1)); // 12 > 12 -> false
        assert!(bools.value(2)); // 13 > 12 -> true
    }

    #[test]
    fn test_comparison_null_handling() {
        let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int64, true)]));
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(10), None, Some(30)]))],
        )
        .unwrap();

        let ast = Expr::Binary {
            left: Box::new(Expr::Column("val".to_string())),
            op: Operator::Gt,
            right: Box::new(Expr::Literal(ScalarValue::Int64(15))),
        };

        let physical = create_physical_expr(&ast, &schema).unwrap();
        let result = physical.evaluate(&batch).unwrap();
        let bools = result
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();

        assert_eq!(bools.len(), 3);
        assert!(!bools.is_null(0));
        assert!(!bools.value(0)); // 10 > 15 -> false
        assert!(bools.is_null(1)); // NULL > 15 -> NULL
        assert!(!bools.is_null(2));
        assert!(bools.value(2)); // 30 > 15 -> true
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
