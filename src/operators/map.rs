//! Map operator - computes new columns based on expressions

use std::sync::Arc;

use crate::expression::{PhysicalExpr, create_physical_expr, Expr, ExpressionError};
use crate::error::VectrillError;

/// Map operator that computes new columns based on expressions
#[derive(Debug)]
pub struct MapOperator {
    /// The expressions to compute
    expressions: Vec<(String, Arc<dyn PhysicalExpr>)>,
}

impl MapOperator {
    /// Create a new map operator with the given expressions
    pub fn new(expressions: Vec<(String, Arc<dyn PhysicalExpr>)>) -> Self {
        Self { expressions }
    }
    
    /// Create a map operator from a list of (column_name, expression) pairs
    pub fn from_exprs(
        exprs: Vec<(String, &Expr)>,
        schema: arrow::datatypes::SchemaRef,
    ) -> Result<Self, VectrillError> {
        let mut physical_exprs = Vec::new();
        
        for (name, expr) in exprs {
            let physical_expr = create_physical_expr(expr, &schema)
                .map_err(|e| VectrillError::ExpressionError(e.to_string()))?;
            physical_exprs.push((name, physical_expr));
        }
        
        Ok(Self::new(physical_exprs))
    }
    
    /// Apply the map operator to a record batch
    pub fn apply(&self, batch: &arrow::record_batch::RecordBatch) -> Result<arrow::record_batch::RecordBatch, VectrillError> {
        if self.expressions.is_empty() {
            return Ok(batch.clone());
        }
        
        // Evaluate all expressions
        let mut new_columns = Vec::new();
        let mut new_fields = Vec::new();
        
        // Keep original columns
        for i in 0..batch.num_columns() {
            new_columns.push(batch.column(i).clone());
            new_fields.push(batch.schema().field(i).clone());
        }
        
        // Add computed columns
        for (name, expr) in &self.expressions {
            let result_array = expr.evaluate(batch)
                .map_err(|e| VectrillError::ExpressionError(e.to_string()))?;
            
            let field = arrow::datatypes::Field::new(
                name.as_str(),
                result_array.data_type().clone(),
                true, // Computed columns can be nullable
            );
            
            new_columns.push(result_array);
            new_fields.push(field);
        }
        
        // Create new schema and batch
        let new_schema = Arc::new(arrow::datatypes::Schema::new(new_fields));
        let new_batch = arrow::record_batch::RecordBatch::try_new(new_schema, new_columns)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))?;
        
        Ok(new_batch)
    }
    
    /// Get the names of the computed columns
    pub fn computed_columns(&self) -> Vec<&String> {
        self.expressions.iter().map(|(name, _)| name).collect()
    }
}

/// Projection operator - selects and computes specific columns
#[derive(Debug)]
pub struct ProjectionOperator {
    /// The expressions to project
    expressions: Vec<(String, Arc<dyn PhysicalExpr>)>,
}

impl ProjectionOperator {
    /// Create a new projection operator
    pub fn new(expressions: Vec<(String, Arc<dyn PhysicalExpr>)>) -> Self {
        Self { expressions }
    }
    
    /// Create a projection operator from expressions
    pub fn from_exprs(
        exprs: Vec<(String, &Expr)>,
        schema: arrow::datatypes::SchemaRef,
    ) -> Result<Self, VectrillError> {
        let mut physical_exprs = Vec::new();
        
        for (name, expr) in exprs {
            let physical_expr = create_physical_expr(expr, &schema)
                .map_err(|e| VectrillError::ExpressionError(e.to_string()))?;
            physical_exprs.push((name, physical_expr));
        }
        
        Ok(Self::new(physical_exprs))
    }
    
    /// Apply the projection to a record batch
    pub fn apply(&self, batch: &arrow::record_batch::RecordBatch) -> Result<arrow::record_batch::RecordBatch, VectrillError> {
        if self.expressions.is_empty() {
            return Ok(arrow::record_batch::RecordBatch::new_empty(batch.schema()));
        }
        
        // Evaluate all expressions
        let mut columns = Vec::new();
        let mut fields = Vec::new();
        
        for (name, expr) in &self.expressions {
            let result_array = expr.evaluate(batch)
                .map_err(|e| VectrillError::ExpressionError(e.to_string()))?;
            
            let field = arrow::datatypes::Field::new(
                name.as_str(),
                result_array.data_type().clone(),
                true, // Projected columns can be nullable
            );
            
            columns.push(result_array);
            fields.push(field);
        }
        
        // Create new schema and batch
        let new_schema = Arc::new(arrow::datatypes::Schema::new(fields));
        let new_batch = arrow::record_batch::RecordBatch::try_new(new_schema, columns)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))?;
        
        Ok(new_batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use crate::expression::{Expr, ScalarValue, Operator};

    #[test]
    fn test_map_operator() {
        // Create test schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]));
        
        // Create test batch
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        ).unwrap();
        
        // Create expressions: a + b as sum, a * 2 as doubled
        let sum_expr = Expr::binary(
            Expr::column("a"),
            Operator::Add,
            Expr::column("b"),
        );
        
        let doubled_expr = Expr::binary(
            Expr::column("a"),
            Operator::Mul,
            Expr::literal(ScalarValue::Int64(2)),
        );
        
        let exprs = vec![
            ("sum".to_string(), &sum_expr),
            ("doubled".to_string(), &doubled_expr),
        ];
        
        // Create map operator
        let map_op = MapOperator::from_exprs(exprs, schema).unwrap();
        
        // Apply map
        let result = map_op.apply(&batch).unwrap();
        
        // Verify results
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 4); // Original 2 + 2 computed
        
        // Check computed columns
        let sum_array = result.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(sum_array.value(0), 11); // 1 + 10
        assert_eq!(sum_array.value(1), 22); // 2 + 20
        assert_eq!(sum_array.value(2), 33); // 3 + 30
        
        let doubled_array = result.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(doubled_array.value(0), 2); // 1 * 2
        assert_eq!(doubled_array.value(1), 4); // 2 * 2
        assert_eq!(doubled_array.value(2), 6); // 3 * 2
    }

    #[test]
    fn test_projection_operator() {
        // Create test schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
            Field::new("c", DataType::Utf8, false),
        ]));
        
        // Create test batch
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec!["x", "y", "z"])),
            ],
        ).unwrap();
        
        // Create projection: a, a + b as sum
        let a_expr = Expr::column("a");
        let sum_expr = Expr::binary(
            Expr::column("a"),
            Operator::Add,
            Expr::column("b"),
        );
        
        let exprs = vec![
            ("a".to_string(), &a_expr),
            ("sum".to_string(), &sum_expr),
        ];
        
        // Create projection operator
        let proj_op = ProjectionOperator::from_exprs(exprs, schema).unwrap();
        
        // Apply projection
        let result = proj_op.apply(&batch).unwrap();
        
        // Verify results
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 2); // Only projected columns
        
        // Check projected columns
        let a_array = result.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(a_array.value(0), 1);
        assert_eq!(a_array.value(1), 2);
        assert_eq!(a_array.value(2), 3);
        
        let sum_array = result.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(sum_array.value(0), 11); // 1 + 10
        assert_eq!(sum_array.value(1), 22); // 2 + 20
        assert_eq!(sum_array.value(2), 33); // 3 + 30
    }

    #[test]
    fn test_map_operator_empty() {
        // Create test schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
        ]));
        
        // Create test batch
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
            ],
        ).unwrap();
        
        // Create map operator with no expressions
        let map_op = MapOperator::new(vec![]);
        
        // Apply map
        let result = map_op.apply(&batch).unwrap();
        
        // Should return the original batch unchanged
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.num_columns(), 1);
        
        let a_array = result.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(a_array.value(0), 1);
        assert_eq!(a_array.value(1), 2);
        assert_eq!(a_array.value(2), 3);
    }
}
