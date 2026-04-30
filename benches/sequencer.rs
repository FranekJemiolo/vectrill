use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use vectrill::sequencer::{Sequencer, SequencerConfig};
use arrow::array::{Int64Array, Float64Array};
use arrow::record_batch::RecordBatch;
use arrow::datatypes::{Schema, Field, DataType};
use std::sync::Arc;

fn create_test_batch(size: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));

    let ids = Int64Array::from_iter(0..size as i64);
    let values = Float64Array::from_iter((0..size).map(|i| i as f64));
    let timestamps = Int64Array::from_iter((0..size).map(|i| i as i64 * 1000));

    RecordBatch::try_new(schema, vec![
        Arc::new(ids),
        Arc::new(values),
        Arc::new(timestamps),
    ]).unwrap()
}

fn bench_sequencer_ingest(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequencer_ingest");
    
    for size in [100, 1000, 10000, 100000].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(size), size, |b, &size| {
            let config = SequencerConfig::default();
            let mut sequencer = Sequencer::new(config);
            let batch = create_test_batch(size);
            
            b.iter(|| {
                sequencer.ingest(black_box(batch.clone())).unwrap();
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
            
            sequencer.ingest(batch.clone()).unwrap();
            
            b.iter(|| {
                black_box(sequencer.flush().unwrap());
            });
        });
    }
    
    group.finish();
}

criterion_group!(benches, bench_sequencer_ingest, bench_sequencer_flush);
criterion_main!(benches);
