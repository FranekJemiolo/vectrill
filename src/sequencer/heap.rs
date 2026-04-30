//! Heap-based k-way merge for event ordering

use crate::RecordBatch;
use std::sync::Arc;

/// Cursor for tracking position within a batch
#[derive(Debug)]
pub struct Cursor {
    /// The batch being processed
    pub batch: Arc<RecordBatch>,
    /// Current row index
    pub index: usize,
    /// Total number of rows in the batch
    pub len: usize,
}

impl Cursor {
    /// Create a new cursor
    pub fn new(batch: Arc<RecordBatch>) -> Self {
        let len = batch.num_rows();
        Self {
            batch,
            index: 0,
            len,
        }
    }

    /// Check if the cursor has more rows
    pub fn has_more(&self) -> bool {
        self.index < self.len
    }

    /// Get the current row index
    pub fn current(&self) -> usize {
        self.index
    }

    /// Advance to the next row
    pub fn advance(&mut self) {
        self.index += 1;
    }
}

/// Item in the merge heap
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct HeapItem {
    /// Timestamp for ordering
    pub timestamp: i64,
    /// Which cursor this item came from
    pub cursor_id: usize,
    /// Row index within the cursor's batch
    pub row_idx: usize,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse comparison for min-heap behavior
        self.timestamp.cmp(&other.timestamp).reverse()
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    fn create_test_batch() -> Arc<RecordBatch> {
        let schema = Schema::new(vec![
            Field::new("timestamp", DataType::Int64, false),
            Field::new("key", DataType::Utf8, false),
        ]);

        let timestamps = Int64Array::from(vec![1000, 2000, 3000]);
        let keys = StringArray::from(vec!["a", "b", "c"]);

        Arc::new(
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(timestamps), Arc::new(keys)])
                .unwrap(),
        )
    }

    #[test]
    fn test_cursor_creation() {
        let batch = create_test_batch();
        let cursor = Cursor::new(batch);
        assert_eq!(cursor.len, 3);
        assert_eq!(cursor.index, 0);
        assert!(cursor.has_more());
    }

    #[test]
    fn test_cursor_advance() {
        let batch = create_test_batch();
        let mut cursor = Cursor::new(batch);

        cursor.advance();
        assert_eq!(cursor.index, 1);
        assert!(cursor.has_more());

        cursor.advance();
        cursor.advance();
        assert_eq!(cursor.index, 3);
        assert!(!cursor.has_more());
    }

    #[test]
    fn test_heap_item_ordering() {
        let item1 = HeapItem {
            timestamp: 1000,
            cursor_id: 0,
            row_idx: 0,
        };

        let item2 = HeapItem {
            timestamp: 2000,
            cursor_id: 1,
            row_idx: 0,
        };

        // For min-heap, smaller timestamp should be "greater"
        assert_eq!(item1.cmp(&item2), std::cmp::Ordering::Greater);
    }
}
