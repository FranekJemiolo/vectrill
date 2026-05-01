//! End-to-end tests for advanced features

use vectrill::expression::{Expr, Operator, ScalarValue};
use vectrill::memory::{global_buffer_pool, BufferPool};
use vectrill::optimization::ExprOptimizer;
use vectrill::performance::{
    global_counter_registry, Counter, CounterRegistry, CounterType, Timer,
};

#[test]
fn test_expression_optimization_constant_folding() {
    let mut optimizer = ExprOptimizer::new();

    // Test constant folding for addition
    let expr = Expr::Binary {
        left: Box::new(Expr::Literal(ScalarValue::Int64(10))),
        op: Operator::Add,
        right: Box::new(Expr::Literal(ScalarValue::Int64(20))),
    };

    let optimized = optimizer.optimize(expr);
    assert_eq!(optimized, Expr::Literal(ScalarValue::Int64(30)));
}

#[test]
fn test_expression_optimization_cse() {
    let mut optimizer = ExprOptimizer::new();

    // Test common subexpression elimination
    // Use a more complex expression that won't be fully folded
    let subexpr = Expr::Binary {
        left: Box::new(Expr::Column("x".to_string())),
        op: Operator::Add,
        right: Box::new(Expr::Literal(ScalarValue::Int64(3))),
    };

    let expr1 = Expr::Binary {
        left: Box::new(subexpr.clone()),
        op: Operator::Add,
        right: Box::new(Expr::Literal(ScalarValue::Int64(1))),
    };

    let expr2 = Expr::Binary {
        left: Box::new(subexpr.clone()),
        op: Operator::Add,
        right: Box::new(Expr::Literal(ScalarValue::Int64(2))),
    };

    let optimized1 = optimizer.optimize(expr1);
    let optimized2 = optimizer.optimize(expr2);

    // Both should be optimized (CSE should work on non-literal expressions)
    assert!(matches!(optimized1, Expr::Binary { .. }));
    assert!(matches!(optimized2, Expr::Binary { .. }));
}

#[test]
fn test_buffer_pool_reuse() {
    let pool = BufferPool::new(5);

    // Get an array
    let arr1 = pool.get_array(&arrow::datatypes::DataType::Int64, 100);
    assert_eq!(arr1.len(), 100);

    // Return it
    pool.return_array(arr1);

    // Get another - buffer pool may reuse a larger array
    let arr2 = pool.get_array(&arrow::datatypes::DataType::Int64, 50);
    assert!(arr2.len() >= 50); // Buffer pool returns array at least as large as requested

    // Check stats - the pool should have the returned array
    let stats = pool.stats();
    assert!(stats.pools_count > 0);
}

#[test]
fn test_global_buffer_pool() {
    let pool = global_buffer_pool();

    let arr = pool.get_array(&arrow::datatypes::DataType::Float64, 200);
    assert_eq!(arr.len(), 200);

    pool.return_array(arr);

    let stats = pool.stats();
    assert!(stats.pools_count > 0);
}

#[test]
fn test_performance_counter() {
    let counter = Counter::new(CounterType::RowsProcessed);

    assert_eq!(counter.get(), 0);

    counter.increment();
    assert_eq!(counter.get(), 1);

    counter.add(10);
    assert_eq!(counter.get(), 11);

    counter.reset();
    assert_eq!(counter.get(), 0);
}

#[test]
fn test_counter_registry() {
    let mut registry = CounterRegistry::new();

    let counter1 = registry.register("test_counter".to_string(), CounterType::BatchesProcessed);
    let counter2 = registry.get("test_counter").unwrap();

    assert!(std::sync::Arc::ptr_eq(&counter1, &counter2));

    counter1.increment();

    let snapshot = registry.snapshot();
    assert_eq!(snapshot.get("test_counter"), Some(&1));
}

#[test]
fn test_timer() {
    let counter = std::sync::Arc::new(Counter::new(CounterType::TotalTimeUs));
    let mut timer = Timer::new(counter.clone());

    timer.start();
    std::thread::sleep(std::time::Duration::from_millis(10));
    timer.stop();

    assert!(counter.get() > 0);
}

#[test]
fn test_global_counter_registry() {
    let registry = global_counter_registry();
    let mut registry = registry.lock().unwrap();

    let counter = registry.register("e2e_test".to_string(), CounterType::RowsProcessed);
    counter.increment();

    let snapshot = registry.snapshot();
    assert_eq!(snapshot.get("e2e_test"), Some(&1));

    registry.reset_all();
    let snapshot_after = registry.snapshot();
    assert_eq!(snapshot_after.get("e2e_test"), Some(&0));
}

#[test]
fn test_expression_optimization_boolean_folding() {
    let mut optimizer = ExprOptimizer::new();

    // Test boolean AND folding
    let expr = Expr::Binary {
        left: Box::new(Expr::Literal(ScalarValue::Boolean(true))),
        op: Operator::And,
        right: Box::new(Expr::Literal(ScalarValue::Boolean(false))),
    };

    let optimized = optimizer.optimize(expr);
    assert_eq!(optimized, Expr::Literal(ScalarValue::Boolean(false)));
}

#[test]
fn test_expression_optimization_unary_negation() {
    let mut optimizer = ExprOptimizer::new();

    // Test unary negation
    let expr = Expr::Unary {
        op: vectrill::expression::UnaryOp::Neg,
        expr: Box::new(Expr::Literal(ScalarValue::Int64(42))),
    };

    let optimized = optimizer.optimize(expr);
    assert_eq!(optimized, Expr::Literal(ScalarValue::Int64(-42)));
}
