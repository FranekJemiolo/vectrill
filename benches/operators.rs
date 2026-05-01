use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;
use vectrill::operators::{Operator, PassThroughOperator};

fn create_test_batch(size: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
    ]));

    let ids = Int64Array::from_iter(0..size as i64);
    let values = Float64Array::from_iter((0..size).map(|i| i as f64));

    RecordBatch::try_new(schema, vec![Arc::new(ids), Arc::new(values)]).unwrap()
}

fn bench_passthrough_operator(c: &mut Criterion) {
    let mut group = c.benchmark_group("passthrough_operator");

    for size in [100, 1000, 10000, 100000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let batch = create_test_batch(size);
            let mut op = PassThroughOperator;

            b.iter(|| {
                black_box(op.process(black_box(batch.clone())).unwrap());
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_passthrough_operator);
criterion_main!(benches);
