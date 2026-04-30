//! Memory optimization - buffer pooling for Arrow arrays

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Buffer pool for reusing Arrow arrays
pub struct BufferPool {
    /// Pools keyed by data type
    pools: Mutex<HashMap<DataType, Vec<ArrayRef>>>,
    /// Maximum size per pool
    max_size_per_pool: usize,
}

impl BufferPool {
    /// Create a new buffer pool
    pub fn new(max_size_per_pool: usize) -> Self {
        Self {
            pools: Mutex::new(HashMap::new()),
            max_size_per_pool,
        }
    }

    /// Get an array from the pool or create a new one
    pub fn get_array(&self, dtype: &DataType, capacity: usize) -> ArrayRef {
        let mut pools = self.pools.lock().unwrap();
        let pool = pools.entry(dtype.clone()).or_insert_with(Vec::new);
        
        // Try to find a suitable buffer
        if let Some(idx) = pool.iter().position(|arr| arr.len() >= capacity) {
            pool.swap_remove(idx)
        } else {
            // Create a new array
            self.create_array(dtype, capacity)
        }
    }

    /// Return an array to the pool
    pub fn return_array(&self, array: ArrayRef) {
        let mut pools = self.pools.lock().unwrap();
        let dtype = array.data_type().clone();
        let pool = pools.entry(dtype).or_insert_with(Vec::new);
        
        if pool.len() < self.max_size_per_pool {
            pool.push(array);
        }
    }

    /// Create a new array of the given type and capacity
    fn create_array(&self, dtype: &DataType, capacity: usize) -> ArrayRef {
        use arrow::array::*;
        
        match dtype {
            DataType::Boolean => Arc::new(BooleanArray::new_null(capacity)),
            DataType::Int8 => Arc::new(Int8Array::new_null(capacity)),
            DataType::Int16 => Arc::new(Int16Array::new_null(capacity)),
            DataType::Int32 => Arc::new(Int32Array::new_null(capacity)),
            DataType::Int64 => Arc::new(Int64Array::new_null(capacity)),
            DataType::UInt8 => Arc::new(UInt8Array::new_null(capacity)),
            DataType::UInt16 => Arc::new(UInt16Array::new_null(capacity)),
            DataType::UInt32 => Arc::new(UInt32Array::new_null(capacity)),
            DataType::UInt64 => Arc::new(UInt64Array::new_null(capacity)),
            DataType::Float32 => Arc::new(Float32Array::new_null(capacity)),
            DataType::Float64 => Arc::new(Float64Array::new_null(capacity)),
            DataType::Utf8 => Arc::new(StringArray::new_null(capacity)),
            DataType::LargeUtf8 => Arc::new(LargeStringArray::new_null(capacity)),
            DataType::Timestamp(_, _) => Arc::new(TimestampMicrosecondArray::new_null(capacity)),
            DataType::Date32 => Arc::new(Date32Array::new_null(capacity)),
            DataType::Date64 => Arc::new(Date64Array::new_null(capacity)),
            _ => Arc::new(new_null_array(dtype, capacity)),
        }
    }

    /// Clear all pools
    pub fn clear(&self) {
        let mut pools = self.pools.lock().unwrap();
        pools.clear();
    }

    /// Get statistics about the pool
    pub fn stats(&self) -> PoolStats {
        let pools = self.pools.lock().unwrap();
        let total_arrays: usize = pools.values().map(|v| v.len()).sum();
        let total_bytes: usize = pools
            .values()
            .flat_map(|v| v.iter())
            .map(|arr| arr.get_buffer_memory_size())
            .sum();
        
        PoolStats {
            total_arrays,
            total_bytes,
            pools_count: pools.len(),
        }
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new(10)
    }
}

/// Statistics about the buffer pool
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub total_arrays: usize,
    pub total_bytes: usize,
    pub pools_count: usize,
}

/// Global buffer pool instance
static GLOBAL_BUFFER_POOL: once_cell::sync::Lazy<Arc<BufferPool>> =
    once_cell::sync::Lazy::new(|| Arc::new(BufferPool::default()));

/// Get the global buffer pool
pub fn global_buffer_pool() -> Arc<BufferPool> {
    GLOBAL_BUFFER_POOL.clone()
}
