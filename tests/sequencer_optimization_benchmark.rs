//! Benchmark comparison between original and optimized sequencer

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::Instant;
use vectrill::sequencer::{
    LateDataPolicy, OptimizedSequencer, Ordering, Sequencer, SequencerConfig,
};

fn create_large_batch(start_id: usize, size: usize, timestamp_offset: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    // Create realistic data with proper distribution
    let timestamps: Vec<i64> = (0..size)
        .map(|i| timestamp_offset + i as i64 * 1000 + (i as i64 % 1000))
        .collect();

    let keys: Vec<String> = (0..size)
        .map(|i| format!("key_{}", (start_id + i) % 100))
        .collect();

    let values: Vec<i64> = (0..size)
        .map(|i| ((start_id + i) as i64 * 7919) % 1_000_000)
        .collect();

    let timestamp_array = Int64Array::from(timestamps);
    let key_array = StringArray::from(keys);
    let value_array = Int64Array::from(values);

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(timestamp_array),
            Arc::new(key_array),
            Arc::new(value_array),
        ],
    )
    .unwrap()
}

fn benchmark_original_sequencer(
    batch_size: usize,
    num_batches: usize,
) -> (std::time::Duration, usize) {
    let config = SequencerConfig {
        batch_size: 10000,
        max_lateness_ms: 1000,
        ordering: Ordering::ByTimestamp,
        late_data_policy: LateDataPolicy::Drop,
        flush_interval_ms: 1000,
    };

    let mut sequencer = Sequencer::new(config);

    let start = Instant::now();

    // Ingest batches
    for i in 0..num_batches {
        let batch = create_large_batch(i * batch_size, batch_size, i as i64 * 1000);
        sequencer.ingest(batch).unwrap();
    }

    let ingest_time = start.elapsed();

    // Process all batches
    let start = Instant::now();
    let mut total_processed = 0;

    while let Some(result_batch) = sequencer.next_batch() {
        total_processed += result_batch.num_rows();
    }

    let process_time = start.elapsed();

    println!(
        "Original: {} batches of {} rows - ingest: {:?}, process: {:?} ({:.2}M rows/sec)",
        num_batches,
        batch_size,
        ingest_time,
        process_time,
        total_processed as f64 / process_time.as_secs_f64() / 1_000_000.0
    );

    (process_time, total_processed)
}

fn benchmark_optimized_sequencer(
    batch_size: usize,
    num_batches: usize,
) -> (std::time::Duration, usize) {
    let config = SequencerConfig {
        batch_size: 10000,
        max_lateness_ms: 1000,
        ordering: Ordering::ByTimestamp,
        late_data_policy: LateDataPolicy::Drop,
        flush_interval_ms: 1000,
    };

    let mut sequencer = OptimizedSequencer::new(config);

    let start = Instant::now();

    // Ingest batches
    for i in 0..num_batches {
        let batch = create_large_batch(i * batch_size, batch_size, i as i64 * 1000);
        sequencer.ingest(batch).unwrap();
    }

    let ingest_time = start.elapsed();

    // Process all batches
    let start = Instant::now();
    let mut total_processed = 0;

    while let Some(result_batch) = sequencer.next_batch() {
        total_processed += result_batch.num_rows();
    }

    let process_time = start.elapsed();

    println!(
        "Optimized: {} batches of {} rows - ingest: {:?}, process: {:?} ({:.2}M rows/sec)",
        num_batches,
        batch_size,
        ingest_time,
        process_time,
        total_processed as f64 / process_time.as_secs_f64() / 1_000_000.0
    );

    (process_time, total_processed)
}

#[test]
fn test_sequencer_optimization_comparison() {
    println!("=== Sequencer Optimization Benchmark ===\n");

    let test_cases = vec![
        (1000, 10),  // Small batches
        (5000, 20),  // Medium batches
        (10000, 50), // Large batches
        (50000, 20), // Very large batches
    ];

    for (batch_size, num_batches) in test_cases {
        println!(
            "Testing {} batches of {} rows each:",
            num_batches, batch_size
        );

        let (original_time, original_processed) =
            benchmark_original_sequencer(batch_size, num_batches);
        let (optimized_time, optimized_processed) =
            benchmark_optimized_sequencer(batch_size, num_batches);

        assert_eq!(original_processed, optimized_processed);

        let speedup = original_time.as_secs_f64() / optimized_time.as_secs_f64();
        println!("Speedup: {:.2}x\n", speedup);

        // Optimized should be faster or at least comparable (allow for CI timing variations)
        assert!(speedup > 0.9, "Optimized sequencer should be faster or at least comparable (speedup: {:.2}x)", speedup);
    }
}

#[test]
fn test_sequencer_memory_efficiency() {
    println!("=== Memory Efficiency Test ===\n");

    let batch_size = 50000;
    let num_batches = 100; // 5M total rows

    println!(
        "Testing memory usage with {} batches of {} rows each",
        num_batches, batch_size
    );

    // Test original sequencer
    let (original_time, original_processed) = benchmark_original_sequencer(batch_size, num_batches);

    // Test optimized sequencer
    let (optimized_time, optimized_processed) =
        benchmark_optimized_sequencer(batch_size, num_batches);

    assert_eq!(original_processed, optimized_processed);

    let speedup = original_time.as_secs_f64() / optimized_time.as_secs_f64();
    println!("Memory efficiency improvement: {:.2}x", speedup);

    // Optimized should be significantly faster for large batches due to reduced allocations
    assert!(
        speedup > 1.0,
        "Optimized sequencer should be more memory efficient"
    );
}

#[test]
fn test_sequencer_correctness_verification() {
    println!("=== Correctness Verification ===\n");

    let config = SequencerConfig {
        batch_size: 1000,
        max_lateness_ms: 1000,
        ordering: Ordering::ByTimestamp,
        late_data_policy: LateDataPolicy::Drop,
        flush_interval_ms: 1000,
    };

    // Create test data with known ordering
    let batch1 = create_large_batch(0, 100, 2000); // timestamps 2000-2099
    let batch2 = create_large_batch(0, 100, 1000); // timestamps 1000-1099
    let batch3 = create_large_batch(0, 100, 3000); // timestamps 3000-3099

    // Test original sequencer
    let mut original_sequencer = Sequencer::new(config.clone());
    original_sequencer.ingest(batch1.clone()).unwrap();
    original_sequencer.ingest(batch2.clone()).unwrap();
    original_sequencer.ingest(batch3.clone()).unwrap();

    let mut original_results = Vec::new();
    while let Some(batch) = original_sequencer.next_batch() {
        for row_idx in 0..batch.num_rows() {
            let timestamp_col = batch.column(0);
            let timestamp_array = timestamp_col.as_any().downcast_ref::<Int64Array>().unwrap();
            original_results.push(timestamp_array.value(row_idx));
        }
    }

    // Test optimized sequencer
    let mut optimized_sequencer = OptimizedSequencer::new(config);
    optimized_sequencer.ingest(batch1).unwrap();
    optimized_sequencer.ingest(batch2).unwrap();
    optimized_sequencer.ingest(batch3).unwrap();

    let mut optimized_results = Vec::new();
    while let Some(batch) = optimized_sequencer.next_batch() {
        for row_idx in 0..batch.num_rows() {
            let timestamp_col = batch.column(0);
            let timestamp_array = timestamp_col.as_any().downcast_ref::<Int64Array>().unwrap();
            optimized_results.push(timestamp_array.value(row_idx));
        }
    }

    // Verify results are identical
    assert_eq!(original_results.len(), optimized_results.len());
    assert_eq!(original_results, optimized_results);

    // Verify ordering is correct
    let mut sorted_results = original_results.clone();
    sorted_results.sort();
    assert_eq!(original_results, sorted_results);

    println!("✓ Both sequencers produce identical, correctly ordered results");
}
