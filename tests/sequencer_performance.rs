//! Comprehensive sequencer performance tests with larger batches

use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;
use std::time::Instant;
use vectrill::sequencer::{LateDataPolicy, Ordering, Sequencer, SequencerConfig};

fn create_large_batch(start_id: usize, size: usize, timestamp_offset: i64) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    // Create realistic data with proper distribution
    let timestamps: Vec<i64> = (0..size)
        .map(|i| (timestamp_offset + i as i64 * 1000 + (i as i64 % 1000)))
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

fn create_sorted_batch(start_id: usize, size: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]));

    // Create pre-sorted data (already ordered by timestamp)
    let timestamps: Vec<i64> = (0..size).map(|i| (i as i64 * 1000)).collect();

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

#[test]
fn test_sequencer_large_batch_performance() {
    let config = SequencerConfig {
        batch_size: 10000,
        max_lateness_ms: 1000,
        ordering: Ordering::ByTimestamp,
        late_data_policy: LateDataPolicy::Drop,
        flush_interval_ms: 1000,
    };

    let mut sequencer = Sequencer::new(config.clone());

    // Test with progressively larger batches
    let batch_sizes = vec![1000, 5000, 10000, 50000, 100000];

    for &size in &batch_sizes {
        let start = Instant::now();

        // Create and ingest multiple batches
        let num_batches = 10;
        for i in 0..num_batches {
            let batch = create_large_batch(i * size, size, i as i64 * 1000);
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
            "Batch size {}: {} rows ingested in {:?}, {} rows processed in {:?} ({:.2}M rows/sec)",
            size,
            size * num_batches,
            ingest_time,
            total_processed,
            process_time,
            total_processed as f64 / process_time.as_secs_f64() / 1_000_000.0
        );

        // Verify correctness
        assert_eq!(total_processed, size * num_batches);

        // Reset sequencer for next test
        sequencer = Sequencer::new(config.clone());
    }
}

#[test]
fn test_sequencer_sorted_vs_unsorted_performance() {
    let config = SequencerConfig {
        batch_size: 10000,
        max_lateness_ms: 1000,
        ordering: Ordering::ByTimestamp,
        late_data_policy: LateDataPolicy::Drop,
        flush_interval_ms: 1000,
    };

    let batch_size = 10000;
    let num_batches = 20;

    // Test with unsorted batches (requires heap operations)
    let mut sequencer_unsorted = Sequencer::new(config.clone());
    let start = Instant::now();

    for i in 0..num_batches {
        let batch = create_large_batch(i * batch_size, batch_size, (num_batches - i) as i64 * 1000);
        sequencer_unsorted.ingest(batch).unwrap();
    }

    let ingest_time_unsorted = start.elapsed();

    let start = Instant::now();
    let mut total_processed = 0;

    while let Some(result_batch) = sequencer_unsorted.next_batch() {
        total_processed += result_batch.num_rows();
    }

    let process_time_unsorted = start.elapsed();

    // Test with sorted batches (minimal heap operations)
    let mut sequencer_sorted = Sequencer::new(config.clone());
    let start = Instant::now();

    for i in 0..num_batches {
        let batch = create_sorted_batch(i * batch_size, batch_size);
        sequencer_sorted.ingest(batch).unwrap();
    }

    let ingest_time_sorted = start.elapsed();

    let start = Instant::now();
    total_processed = 0;

    while let Some(result_batch) = sequencer_sorted.next_batch() {
        total_processed += result_batch.num_rows();
    }

    let process_time_sorted = start.elapsed();

    println!(
        "Unsorted: ingest {:?}, process {:?} ({:.2}M rows/sec)",
        ingest_time_unsorted,
        process_time_unsorted,
        (batch_size * num_batches) as f64 / process_time_unsorted.as_secs_f64() / 1_000_000.0
    );

    println!(
        "Sorted: ingest {:?}, process {:?} ({:.2}M rows/sec)",
        ingest_time_sorted,
        process_time_sorted,
        (batch_size * num_batches) as f64 / process_time_sorted.as_secs_f64() / 1_000_000.0
    );

    let speedup = process_time_unsorted.as_secs_f64() / process_time_sorted.as_secs_f64();
    println!("Sorted vs Unsorted speedup: {:.2}x", speedup);

    // Sorted should generally be faster due to reduced heap operations
    // Allow some variance in timing results
    assert!(
        speedup > 0.5,
        "Sorted batches should be at least somewhat faster or comparable"
    );
}

#[test]
fn test_sequencer_batch_size_impact() {
    let base_config = SequencerConfig {
        max_lateness_ms: 1000,
        ordering: Ordering::ByTimestamp,
        late_data_policy: LateDataPolicy::Drop,
        batch_size: 1000, // Will be overridden
        flush_interval_ms: 1000,
    };

    let batch_sizes = vec![1000, 5000, 10000, 20000, 50000];
    let input_batch_size = 10000;
    let num_input_batches = 50;

    for &output_batch_size in &batch_sizes {
        let mut config = base_config.clone();
        config.batch_size = output_batch_size;
        let mut sequencer = Sequencer::new(config.clone());

        let start = Instant::now();

        // Ingest all batches
        for i in 0..num_input_batches {
            let batch = create_large_batch(i * input_batch_size, input_batch_size, i as i64 * 1000);
            sequencer.ingest(batch).unwrap();
        }

        let ingest_time = start.elapsed();

        // Process all batches
        let start = Instant::now();
        let mut total_output_batches = 0;

        while let Some(_) = sequencer.next_batch() {
            total_output_batches += 1;
        }

        let process_time = start.elapsed();

        println!("Output batch size {}: {} input batches -> {} output batches, process time: {:?} ({:.2}M rows/sec)",
                 output_batch_size, num_input_batches, total_output_batches, process_time,
                 (input_batch_size * num_input_batches) as f64 / process_time.as_secs_f64() / 1_000_000.0);
    }
}

#[test]
fn test_sequencer_memory_usage() {
    let config = SequencerConfig {
        batch_size: 10000,
        max_lateness_ms: 1000,
        ordering: Ordering::ByTimestamp,
        late_data_policy: LateDataPolicy::Drop,
        flush_interval_ms: 1000,
    };

    let mut sequencer = Sequencer::new(config.clone());

    // Test memory usage with large batches
    let batch_size = 50000;
    let num_batches = 100; // 5M total rows

    println!(
        "Testing memory usage with {} batches of {} rows each",
        num_batches, batch_size
    );

    for i in 0..num_batches {
        let batch = create_large_batch(i * batch_size, batch_size, i as i64 * 1000);
        sequencer.ingest(batch).unwrap();

        if i % 10 == 0 {
            println!(
                "Ingested {} batches, pending: {}",
                i + 1,
                sequencer.pending_batches()
            );
        }
    }

    let start = Instant::now();
    let mut total_processed = 0;

    while let Some(result_batch) = sequencer.next_batch() {
        total_processed += result_batch.num_rows();
    }

    let process_time = start.elapsed();

    println!(
        "Processed {} rows in {:?} ({:.2}M rows/sec)",
        total_processed,
        process_time,
        total_processed as f64 / process_time.as_secs_f64() / 1_000_000.0
    );

    assert_eq!(total_processed, batch_size * num_batches);
}

#[test]
fn test_sequencer_watermark_performance() {
    let config = SequencerConfig {
        batch_size: 10000,
        max_lateness_ms: 1000,
        ordering: Ordering::ByTimestamp,
        late_data_policy: LateDataPolicy::Drop,
        flush_interval_ms: 1000,
    };

    let mut sequencer = Sequencer::new(config.clone());

    let batch_size = 10000;
    let num_batches = 50;

    // Create batches with increasing timestamps to test watermark advancement
    for i in 0..num_batches {
        let batch = create_large_batch(i * batch_size, batch_size, i as i64 * 10000);
        sequencer.ingest(batch).unwrap();
    }

    let start = Instant::now();
    let mut total_processed = 0;

    while let Some(result_batch) = sequencer.next_batch() {
        total_processed += result_batch.num_rows();
    }

    let process_time = start.elapsed();

    println!(
        "Watermark test: {} rows processed in {:?} ({:.2}M rows/sec), final watermark: {}",
        total_processed,
        process_time,
        total_processed as f64 / process_time.as_secs_f64() / 1_000_000.0,
        sequencer.watermark()
    );

    assert_eq!(total_processed, batch_size * num_batches);
}
