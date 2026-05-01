use arrow::array::{Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;
use vectrill::operators::{FilterOperator, MapOperator, Operator};
use vectrill::expression::{Expr, ExprType, BinaryOp};
use vectrill::sequencer::{Sequencer, SequencerConfig};

fn create_realistic_batch(size: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("value", DataType::Float64, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));

    // More realistic data distribution
    let mut rng = fastrand::Rng::new();
    let categories = ["A", "B", "C", "D", "E"];
    
    let ids = Int64Array::from_iter((0..size).map(|i| Some(rng.i64(0..1_000_000))));
    let cats = StringArray::from_iter((0..size).map(|_| Some(categories[rng.usize(0..categories.len())].to_string())));
    let values = Float64Array::from_iter((0..size).map(|_| Some(rng.f64() * 1000.0)));
    let timestamps = Int64Array::from_iter((0..size).map(|i| Some((i as i64) * 1000 + rng.i64(0..1000))));

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
            
            // Filter: value > 500.0
            let expr = Expr::Binary {
                left: Box::new(Expr::Column("value".to_string())),
                op: BinaryOp::GreaterThan,
                right: Box::new(Expr::Literal(ExprType::Float64(500.0))),
            };
            
            let mut filter_op = FilterOperator::new(expr);

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
            
            // Map: value * 2.0 + 10.0
            let expr = Expr::Binary {
                left: Box::new(Expr::Binary {
                    left: Box::new(Expr::Column("value".to_string())),
                    op: BinaryOp::Multiply,
                    right: Box::new(Expr::Literal(ExprType::Float64(2.0))),
                }),
                op: BinaryOp::Add,
                right: Box::new(Expr::Literal(ExprType::Float64(10.0))),
            };
            
            let mut map_op = MapOperator::new(expr, "computed_value".to_string());

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
                // Don't measure flush - just the ingest operation
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_filter_realistic, bench_map_realistic, bench_sequencer_realistic);
criterion_main!(benches);
