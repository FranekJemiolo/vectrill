use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;
use vectrill::operators::{FilterOperator, MapOperator, Operator};
use vectrill::expression::{Expr, Operator as ExprOp, ScalarValue, create_physical_expr};
use vectrill::sequencer::{Sequencer, SequencerConfig};

fn create_realistic_batch(size: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));

    // More realistic data distribution
    let categories = ["A", "B", "C", "D", "E"];
    
    // Use deterministic but realistic patterns
    let ids = Int64Array::from_iter((0..size).map(|i| Some((i as i64 * 7919) % 1_000_000))); // Prime multiplier for distribution
    let cats = StringArray::from_iter((0..size).map(|i| Some(categories[(i * 7) % categories.len()].to_string())));
    let values = Int64Array::from_iter((0..size).map(|i| Some(((i as i64 * 31) % 1000) + 100))); // Int64 values 100-1099
    let timestamps = Int64Array::from_iter((0..size).map(|i| Some((i as i64) * 1000 + ((i as i64 * 37) % 1000))));

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(ids),
            Arc::new(cats),
            Arc::new(values),
            Arc::new(timestamps),
        ],
    ).unwrap()
}

fn bench_filter_realistic(c: &mut Criterion) {
    let mut group = c.benchmark_group("filter_realistic");

    for size in [1000, 10000, 100000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let batch = create_realistic_batch(size);
            
            // Filter: value > 500
            let expr = Expr::binary(
                Expr::column("value"),
                ExprOp::Gt,
                Expr::literal(ScalarValue::Int64(500)),
            );
            
            let physical_expr = create_physical_expr(&expr, &batch.schema()).unwrap();
            let mut filter_op = FilterOperator::new(physical_expr);

            b.iter(|| {
                black_box(filter_op.process(black_box(batch.clone())).unwrap());
            });
        });
    }

    group.finish();
}

fn bench_map_realistic(c: &mut Criterion) {
    let mut group = c.benchmark_group("map_realistic");

    for size in [1000, 10000, 100000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let batch = create_realistic_batch(size);
            
            // Map: value * 2 + 10
            let expr = Expr::binary(
                Expr::binary(
                    Expr::column("value"),
                    ExprOp::Mul,
                    Expr::literal(ScalarValue::Int64(2)),
                ),
                ExprOp::Add,
                Expr::literal(ScalarValue::Int64(10)),
            );
            
            let physical_expr = create_physical_expr(&expr, &batch.schema()).unwrap();
            let mut map_op = MapOperator::new(vec![("computed_value".to_string(), physical_expr)]);

            b.iter(|| {
                black_box(map_op.process(black_box(batch.clone())).unwrap());
            });
        });
    }

    group.finish();
}

fn bench_sequencer_realistic(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequencer_realistic");

    for size in [1000, 10000, 100000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let config = SequencerConfig::default();
            let mut sequencer = Sequencer::new(config);
            
            // Pre-populate with some data
            for _ in 0..10 {
                let batch = create_realistic_batch(size / 10);
                sequencer.ingest(batch).unwrap();
            }

            b.iter(|| {
                let batch = create_realistic_batch(size);
                black_box(sequencer.ingest(black_box(batch)).unwrap());
                // Don't measure flush - just ingest operation
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_filter_realistic, bench_map_realistic, bench_sequencer_realistic);
criterion_main!(benches);
