//! Filter operator for streaming data processing

use std::sync::Arc;

use arrow::record_batch::RecordBatch;

use crate::error::VectrillError;
use crate::expression::{create_physical_expr, Expr, PhysicalExpr};
use crate::operators::pipeline::Operator as PipelineOperator;
use crate::optimization::fusion::FusableOperator;

/// Filter operator that filters rows based on a predicate expression
pub struct FilterOperator {
    predicate: Arc<dyn PhysicalExpr>,
}

impl FilterOperator {
    /// Create a new filter operator with a physical predicate expression
    pub fn new(predicate: Arc<dyn PhysicalExpr>) -> Self {
        Self { predicate }
    }

    /// Create a new filter operator from a logical expression
    pub fn from_expr(
        expr: &Expr,
        schema: arrow::datatypes::SchemaRef,
    ) -> Result<Self, VectrillError> {
        let physical_predicate = create_physical_expr(expr, &schema)
            .map_err(|e| VectrillError::ExpressionError(e.to_string()))?;

        // Validate that predicate returns boolean
        if physical_predicate.data_type() != &arrow::datatypes::DataType::Boolean {
            return Err(VectrillError::ExpressionError(format!(
                "Predicate must return boolean, got {:?}",
                physical_predicate.data_type()
            )));
        }

        Ok(Self::new(physical_predicate))
    }

    /// Apply filter to a RecordBatch
    pub fn apply(&self, batch: &RecordBatch) -> Result<RecordBatch, VectrillError> {
        // Evaluate predicate against the batch
        let mask = self
            .predicate
            .evaluate(batch)
            .map_err(|e| VectrillError::ExpressionError(e.to_string()))?;

        // Downcast to BooleanArray
        let mask = mask
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .ok_or_else(|| {
                VectrillError::ExpressionError("Predicate must return boolean array".to_string())
            })?;

        // Apply the filter using Arrow compute kernel
        let filtered_batch = arrow::compute::filter_record_batch(batch, mask)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))?;

        Ok(filtered_batch)
    }
}

impl PipelineOperator for FilterOperator {
    fn process(&mut self, batch: RecordBatch) -> crate::error::Result<RecordBatch> {
        self.apply(&batch)
    }
}

impl FusableOperator for FilterOperator {
    fn expressions(&self) -> Vec<&Expr> {
        vec![]
    }

    fn predicate(&self) -> Option<&Expr> {
        None
    }

    fn projection(&self) -> Option<&[String]> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expression::{Expr, Operator, ScalarValue};
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn test_filter_operator() {
        // Create schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        // Create test batch
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec![
                    "Alice", "Bob", "Charlie", "David", "Eve",
                ])),
            ],
        )
        .unwrap();

        // Create predicate: id > 3
        let predicate_expr = Expr::binary(
            Expr::column("id"),
            Operator::Gt,
            Expr::literal(ScalarValue::Int64(3)),
        );

        // Create filter operator
        let filter_op = FilterOperator::from_expr(&predicate_expr, schema).unwrap();

        // Apply filter
        let result = filter_op.apply(&batch).unwrap();

        // Verify results: only id = 4, 5 match
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 2);

        let id_array = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 4);
        assert_eq!(id_array.value(1), 5);
    }

    #[test]
    fn test_filter_boolean_predicate() {
        // Create test schema
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("active", DataType::Boolean, false),
        ]));

        // Create test batch
        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(arrow::array::BooleanArray::from(vec![
                    true, false, true, false, true,
                ])),
            ],
        )
        .unwrap();

        // Create predicate: active = true
        let predicate_expr = Expr::binary(
            Expr::column("active"),
            Operator::Eq,
            Expr::literal(ScalarValue::Boolean(true)),
        );

        // Create filter operator
        let filter_op = FilterOperator::from_expr(&predicate_expr, schema).unwrap();

        // Apply filter
        let result = filter_op.apply(&batch).unwrap();

        // Verify results: rows with id 1, 3, 5 match
        assert_eq!(result.num_rows(), 3);

        let id_array = result
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(id_array.value(0), 1);
        assert_eq!(id_array.value(1), 3);
        assert_eq!(id_array.value(2), 5);
    }
}
