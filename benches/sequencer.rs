use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::sync::Arc;
use vectrill::sequencer::{Sequencer, SequencerConfig};

fn create_test_batch(size: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let timestamps = Int64Array::from_iter(0..size as i64);
    let keys = StringArray::from_iter((0..size).map(|i| Some(format!("key_{}", i % 10))));
    let values = Int64Array::from_iter((0..size).map(|i| Some(i as i64)));

    RecordBatch::try_new(
        schema,
        vec![Arc::new(timestamps), Arc::new(keys), Arc::new(values)],
    )
    .unwrap()
}

fn bench_sequencer_ingest(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequencer_ingest");

    for size in [100, 1000, 10000, 100000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let config = SequencerConfig::default();
            let mut sequencer = Sequencer::new(config);
            let batch = create_test_batch(size);

            b.iter(|| {
                black_box(sequencer.ingest(black_box(batch.clone())).unwrap());
            });
        });
    }

    group.finish();
}

fn bench_sequencer_flush(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequencer_flush");

    for size in [100, 1000, 10000, 100000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let config = SequencerConfig::default();
            let mut sequencer = Sequencer::new(config);
            let batch = create_test_batch(size);
            sequencer.ingest(batch).unwrap();

            b.iter(|| {
                let result = sequencer.flush();
                black_box(result);
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_sequencer_ingest, bench_sequencer_flush);
criterion_main!(benches);
