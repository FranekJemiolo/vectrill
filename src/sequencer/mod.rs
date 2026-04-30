//! Event sequencer for ordering events from multiple sources

use crate::{error::Result, RecordBatch};
use arrow::array::{Int64Array, StringArray};
use arrow::datatypes::DataType;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::sync::Arc;

pub mod config;
pub mod heap;

pub use config::{LateDataPolicy, Ordering, SequencerConfig};
pub use heap::{Cursor, HeapItem};

/// Event sequencer that orders events from multiple sources
pub struct Sequencer {
    config: SequencerConfig,
    cursors: Vec<Cursor>,
    heap: BinaryHeap<Reverse<HeapItem>>,
    output_buffer: Vec<(Arc<RecordBatch>, usize)>,
    watermark: i64,
    max_timestamps: Vec<i64>,
}

impl Sequencer {
    /// Create a new sequencer with the given configuration
    pub fn new(config: SequencerConfig) -> Self {
        Self {
            config,
            cursors: Vec::new(),
            heap: BinaryHeap::new(),
            output_buffer: Vec::new(),
            watermark: i64::MIN,
            max_timestamps: Vec::new(),
        }
    }

    /// Ingest a batch from a connector
    pub fn ingest(&mut self, batch: RecordBatch) -> Result<()> {
        let batch = Arc::new(batch);
        let cursor_id = self.cursors.len();

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

                // Add to output buffer
                let cursor = &self.cursors[item.cursor_id];
                self.output_buffer
                    .push((cursor.batch.clone(), item.row_idx));

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
        self.output_buffer.sort_by_key(|(batch, row_idx)| {
            Self::get_timestamp_static(batch, *row_idx).unwrap_or(i64::MIN)
        });

        // Build output batch using Arrow take kernel
        let first_batch = &self.output_buffer[0].0;
        let schema = first_batch.schema();
        let _num_rows = self.output_buffer.len();

        // Collect indices per batch
        let mut batch_indices: std::collections::HashMap<usize, Vec<usize>> =
            std::collections::HashMap::new();
        for (batch, row_idx) in &self.output_buffer {
            // Use batch address as key (simplified)
            let key = batch as *const _ as usize;
            batch_indices.entry(key).or_default().push(*row_idx);
        }

        // TODO: Implement proper batch construction with Arrow take kernel
        // For now, create a simple batch
        let timestamps: Vec<i64> = self
            .output_buffer
            .iter()
            .map(|(batch, row_idx)| self.get_timestamp(batch, *row_idx).unwrap_or(0))
            .collect();

        let keys: Vec<String> = self
            .output_buffer
            .iter()
            .map(|(batch, row_idx)| {
                self.get_key(batch, *row_idx)
                    .unwrap_or_else(|| "unknown".to_string())
            })
            .collect();

        let values: Vec<i64> = self
            .output_buffer
            .iter()
            .map(|(batch, row_idx)| self.get_value(batch, *row_idx).unwrap_or(0))
            .collect();

        let timestamp_array = Int64Array::from(timestamps);
        let key_array = StringArray::from(keys);
        let value_array = Int64Array::from(values);

        let result = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(timestamp_array),
                Arc::new(key_array),
                Arc::new(value_array),
            ],
        )
        .ok();

        // Clear buffer
        self.output_buffer.clear();

        // Update watermark
        self.update_watermark();

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
        let batch = self.cursors[cursor_id].batch.clone();
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

    /// Update the watermark based on max timestamps from all connectors
    fn update_watermark(&mut self) {
        if self.max_timestamps.is_empty() {
            return;
        }

        let min_max = *self.max_timestamps.iter().min().unwrap_or(&i64::MAX);
        self.watermark = min_max - self.config.max_lateness_ms;
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

    /// Get key from a batch at a specific row
    fn get_key(&self, batch: &RecordBatch, row_idx: usize) -> Option<String> {
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

    /// Get value from a batch at a specific row
    fn get_value(&self, batch: &RecordBatch, row_idx: usize) -> Option<i64> {
        if batch.num_columns() > 2 {
            let column = batch.column(2);
            if column.data_type() == &DataType::Int64 {
                let array = column.as_any().downcast_ref::<Int64Array>()?;
                array.value(row_idx).into()
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
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};

    fn create_test_batch(start_ts: i64, count: usize) -> RecordBatch {
        let schema = Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]);

        let timestamps: Vec<i64> = (0..count).map(|i| start_ts + i as i64 * 1000).collect();
        let keys: Vec<String> = (0..count).map(|i| format!("key_{}", i % 3)).collect();
        let values: Vec<i64> = (0..count).map(|i| i as i64).collect();

        RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int64Array::from(timestamps)),
                Arc::new(StringArray::from(keys)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_sequencer_creation() {
        let config = SequencerConfig::default();
        let sequencer = Sequencer::new(config);
        assert_eq!(sequencer.config.batch_size, 1000);
    }

    #[test]
    fn test_sequencer_ingest() {
        let config = SequencerConfig::default();
        let mut sequencer = Sequencer::new(config);

        let batch = create_test_batch(1000, 5);
        assert!(sequencer.ingest(batch).is_ok());
        assert_eq!(sequencer.cursors.len(), 1);
    }

    #[test]
    fn test_sequencer_ordering() {
        let config = SequencerConfig {
            batch_size: 10,
            ..Default::default()
        };
        let mut sequencer = Sequencer::new(config);

        // Ingest batches with out-of-order timestamps
        let batch1 = create_test_batch(5000, 3); // 5000, 6000, 7000
        let batch2 = create_test_batch(1000, 3); // 1000, 2000, 3000

        sequencer.ingest(batch1).unwrap();
        sequencer.ingest(batch2).unwrap();

        let result = sequencer.next_batch();
        assert!(result.is_some());

        let output = result.unwrap();
        let timestamps = output
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        // Check that output is ordered
        for i in 1..timestamps.len() {
            assert!(timestamps.value(i) >= timestamps.value(i - 1));
        }
    }
}
