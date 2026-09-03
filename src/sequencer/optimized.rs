//! Optimized sequencer implementation with performance improvements

use crate::{error::Result, RecordBatch};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::DataType;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

use super::{
    config::{LateDataPolicy, SequencerConfig},
    heap::{Cursor, HeapItem},
};

/// Optimized event sequencer with performance improvements
pub struct OptimizedSequencer {
    config: SequencerConfig,
    cursors: Vec<Cursor>,
    heap: BinaryHeap<Reverse<HeapItem>>,
    output_buffer: Vec<(usize, usize)>, // (batch_id, row_idx) instead of Arc<RecordBatch>
    batches: Vec<Arc<RecordBatch>>,     // Store batches separately
    watermark: i64,
    max_timestamps: Vec<i64>,
}

impl OptimizedSequencer {
    /// Create a new optimized sequencer
    pub fn new(config: SequencerConfig) -> Self {
        Self {
            config,
            cursors: Vec::new(),
            heap: BinaryHeap::new(),
            output_buffer: Vec::new(),
            batches: Vec::new(),
            watermark: i64::MIN,
            max_timestamps: Vec::new(),
        }
    }

    /// Ingest a batch from a connector
    pub fn ingest(&mut self, batch: RecordBatch) -> Result<()> {
        let batch = Arc::new(batch);
        let cursor_id = self.batches.len();
        self.batches.push(batch.clone());

        // Create a cursor for this batch
        let cursor = Cursor::new(batch.clone());
        self.cursors.push(cursor);
        self.max_timestamps.push(i64::MIN);

        // Push the first row from this batch into the heap
        if let Some(timestamp) = self.get_timestamp(&batch, 0) {
            self.max_timestamps[cursor_id] = timestamp;
            self.heap.push(Reverse(HeapItem {
                timestamp,
                cursor_id,
                row_idx: 0,
            }));
        }

        Ok(())
    }

    /// Get the next ordered batch
    pub fn next_batch(&mut self) -> Option<RecordBatch> {
        // Process events until we have enough for a batch or watermark advances
        while self.output_buffer.len() < self.config.batch_size && !self.heap.is_empty() {
            if let Some(item) = self.heap.pop() {
                let item = item.0;

                // Check if this event is late
                if item.timestamp < self.watermark {
                    match self.config.late_data_policy {
                        LateDataPolicy::Drop => {
                            // Skip late event
                            self.advance_cursor(item.cursor_id, item.row_idx);
                            continue;
                        }
                        LateDataPolicy::Allow => {
                            // Allow late event
                        }
                        LateDataPolicy::SideOutput => {
                            // TODO: Send to side output
                            self.advance_cursor(item.cursor_id, item.row_idx);
                            continue;
                        }
                    }
                }

                // Add to output buffer - store batch_id and row_idx instead of Arc<RecordBatch>
                self.output_buffer.push((item.cursor_id, item.row_idx));

                // Advance the cursor
                self.advance_cursor(item.cursor_id, item.row_idx);
            }
        }

        // Check if we should flush
        if self.output_buffer.len() >= self.config.batch_size || self.heap.is_empty() {
            self.flush()
        } else {
            None
        }
    }

    /// Flush the current output buffer as a batch
    pub fn flush(&mut self) -> Option<RecordBatch> {
        if self.output_buffer.is_empty() {
            return None;
        }

        // Sort by timestamp (should already be sorted due to heap)
        self.output_buffer.sort_by_key(|(batch_id, row_idx)| {
            let batch = &self.batches[*batch_id];
            Self::get_timestamp_static(batch, *row_idx).unwrap_or(i64::MIN)
        });

        // Build output batch using vectorized take / interleave kernels
        let first_batch = &self.batches[self.output_buffer[0].0];
        let schema = first_batch.schema();

        let first_batch_id = self.output_buffer[0].0;
        let same_batch = self
            .output_buffer
            .iter()
            .all(|(b_id, _)| *b_id == first_batch_id);

        let result = if same_batch {
            let indices = arrow::array::UInt32Array::from(
                self.output_buffer
                    .iter()
                    .map(|(_, row)| *row as u32)
                    .collect::<Vec<_>>(),
            );
            let mut cols = Vec::with_capacity(schema.fields().len());
            let mut ok = true;
            for col_idx in 0..schema.fields().len() {
                match arrow::compute::take(first_batch.column(col_idx).as_ref(), &indices, None) {
                    Ok(col) => cols.push(col),
                    Err(_) => {
                        ok = false;
                        break;
                    }
                }
            }
            if ok {
                RecordBatch::try_new(schema.clone(), cols).ok()
            } else {
                None
            }
        } else {
            let mut unique_batch_ids: Vec<usize> = Vec::new();
            for (b_id, _) in &self.output_buffer {
                if !unique_batch_ids.contains(b_id) {
                    unique_batch_ids.push(*b_id);
                }
            }

            let coordinates: Vec<(usize, usize)> = self
                .output_buffer
                .iter()
                .map(|(b_id, row)| {
                    let batch_pos = unique_batch_ids.iter().position(|id| id == b_id).unwrap();
                    (batch_pos, *row)
                })
                .collect();

            let mut cols = Vec::with_capacity(schema.fields().len());
            let mut ok = true;
            for col_idx in 0..schema.fields().len() {
                let column_arrays: Vec<&dyn arrow::array::Array> = unique_batch_ids
                    .iter()
                    .map(|id| self.batches[*id].column(col_idx).as_ref())
                    .collect();
                match arrow::compute::interleave(&column_arrays, &coordinates) {
                    Ok(col) => cols.push(col),
                    Err(_) => {
                        ok = false;
                        break;
                    }
                }
            }
            if ok {
                RecordBatch::try_new(schema.clone(), cols).ok()
            } else {
                None
            }
        };

        // Clear buffer
        self.output_buffer.clear();

        // Update watermark
        self.update_watermark();

        // Free memory when all items are emitted
        if self.heap.is_empty() {
            self.batches.clear();
            self.cursors.clear();
            self.max_timestamps.clear();
        }

        result
    }

    /// Get the current watermark
    pub fn watermark(&self) -> i64 {
        self.watermark
    }

    /// Get the number of pending batches
    pub fn pending_batches(&self) -> usize {
        self.cursors.len()
    }

    /// Advance a cursor to the next row
    fn advance_cursor(&mut self, cursor_id: usize, _current_row: usize) {
        let batch = self.batches[cursor_id].clone();
        let cursor = &mut self.cursors[cursor_id];
        cursor.index += 1;

        if cursor.index < cursor.len {
            if let Some(timestamp) = Self::get_timestamp_static(&batch, cursor.index) {
                self.max_timestamps[cursor_id] = self.max_timestamps[cursor_id].max(timestamp);
                self.heap.push(Reverse(HeapItem {
                    timestamp,
                    cursor_id,
                    row_idx: cursor.index,
                }));
            }
        }
    }

    /// Update the watermark based on active cursors or latest timestamps
    fn update_watermark(&mut self) {
        if self.max_timestamps.is_empty() {
            return;
        }

        let active_max_timestamps: Vec<i64> = self
            .cursors
            .iter()
            .enumerate()
            .filter(|(_, c)| c.has_more())
            .map(|(id, _)| self.max_timestamps[id])
            .collect();

        let min_max = if !active_max_timestamps.is_empty() {
            *active_max_timestamps.iter().min().unwrap()
        } else {
            *self.max_timestamps.iter().max().unwrap_or(&i64::MIN)
        };

        self.watermark = min_max.saturating_sub(self.config.max_lateness_ms);
    }

    /// Get timestamp from a batch at a specific row
    fn get_timestamp(&self, batch: &RecordBatch, row_idx: usize) -> Option<i64> {
        Self::get_timestamp_static(batch, row_idx)
    }

    /// Static helper to get timestamp from a batch
    fn get_timestamp_static(batch: &RecordBatch, row_idx: usize) -> Option<i64> {
        let column = batch.column(0);
        if column.data_type() == &DataType::Int64 {
            let array = column.as_any().downcast_ref::<Int64Array>()?;
            Some(array.value(row_idx))
        } else {
            None
        }
    }

    /// Static helper to get key from a batch
    #[allow(dead_code)]
    fn get_key_static(batch: &RecordBatch, row_idx: usize) -> Option<String> {
        if batch.num_columns() > 1 {
            let column = batch.column(1);
            if column.data_type() == &DataType::Utf8 {
                let array = column.as_any().downcast_ref::<StringArray>()?;
                Some(array.value(row_idx).to_string())
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Static helper to get value from a batch
    #[allow(dead_code)]
    fn get_value_static(batch: &RecordBatch, row_idx: usize) -> Option<i64> {
        if batch.num_columns() > 2 {
            let column = batch.column(2);
            if column.data_type() == &DataType::Int64 {
                let array = column.as_any().downcast_ref::<Int64Array>()?;
                Some(array.value(row_idx))
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sequencer::config::{Ordering, SequencerConfig};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn create_test_batch(start_ts: i64, size: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let timestamps: Vec<i64> = (0..size).map(|i| start_ts + i as i64).collect();
        let keys: Vec<String> = (0..size).map(|i| format!("key_{}", i)).collect();
        let values: Vec<i64> = (0..size).map(|i| i as i64).collect();

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
    fn test_optimized_sequencer_basic() {
        let config = SequencerConfig {
            batch_size: 100,
            max_lateness_ms: 1000,
            ordering: Ordering::ByTimestamp,
            late_data_policy: LateDataPolicy::Drop,
            flush_interval_ms: 1000,
        };

        let mut sequencer = OptimizedSequencer::new(config);

        // Ingest some batches
        let batch1 = create_test_batch(1000, 50);
        let batch2 = create_test_batch(2000, 50);

        sequencer.ingest(batch1).unwrap();
        sequencer.ingest(batch2).unwrap();

        // Process batches
        let mut total_processed = 0;
        while let Some(result_batch) = sequencer.next_batch() {
            total_processed += result_batch.num_rows();
        }

        assert_eq!(total_processed, 100);
    }
}
