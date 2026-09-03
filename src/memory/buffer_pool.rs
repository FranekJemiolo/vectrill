//! Memory optimization - enhanced buffer pooling for Arrow arrays

use super::config::{AutoTuningEngine, MemoryPoolConfig, TuningDecision};
use super::zero_copy::ZeroCopyOps;
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Enhanced buffer pool for reusing Arrow arrays with LRU eviction
pub struct BufferPool {
    /// Pools keyed by data type using LRU
    pools: Mutex<HashMap<DataType, VecDeque<ArrayRef>>>,
    /// Configuration
    config: MemoryPoolConfig,
    /// Auto-tuning engine
    auto_tuning: Mutex<AutoTuningEngine>,
    /// Total memory allocated across all pools
    total_memory_bytes: std::sync::atomic::AtomicUsize,
    /// Last cleanup time
    last_cleanup: std::sync::Mutex<Instant>,
}

impl BufferPool {
    /// Create a new buffer pool with default configuration
    pub fn new(max_size_per_pool: usize) -> Self {
        let config = MemoryPoolConfig::new(max_size_per_pool);
        Self::with_config(config)
    }

    /// Create a buffer pool with custom configuration
    pub fn with_config(config: MemoryPoolConfig) -> Self {
        let auto_tuning = AutoTuningEngine::new(config.clone());
        Self {
            pools: Mutex::new(HashMap::new()),
            config,
            auto_tuning: Mutex::new(auto_tuning),
            total_memory_bytes: std::sync::atomic::AtomicUsize::new(0),
            last_cleanup: std::sync::Mutex::new(Instant::now()),
        }
    }

    /// Get an array from the pool with LRU eviction and memory pressure detection
    pub fn get_array(&self, dtype: &DataType, capacity: usize) -> ArrayRef {
        let mut pools = self.pools.lock().unwrap();

        // Check memory pressure and cleanup if needed
        self.check_memory_pressure(&mut pools);

        // Perform auto-tuning if needed
        self.perform_auto_tuning(&mut pools);

        let pool = pools.entry(dtype.clone()).or_default();

        // Try to find a suitable buffer using LRU
        if let Some(idx) = pool.iter().position(|arr| arr.len() >= capacity) {
            let array = pool.remove(idx).unwrap();
            // Move to back (most recently used)
            pool.push_back(array.clone());
            array
        } else {
            // Create a new array and track memory
            let array = self.create_array(dtype, capacity);
            self.track_memory_usage(&array);
            pool.push_back(array.clone());

            // Enforce pool size limit
            if pool.len() > self.config.max_size_per_pool {
                pool.pop_front(); // Remove oldest
            }

            array
        }
    }

    /// Perform auto-tuning if needed
    fn perform_auto_tuning(&self, pools: &mut HashMap<DataType, VecDeque<ArrayRef>>) {
        let mut auto_tuning = self.auto_tuning.lock().unwrap();
        if auto_tuning.should_tune() {
            let current_pool_size = pools.values().map(|pool| pool.len()).sum();
            let decision = auto_tuning.tune(current_pool_size);

            match decision {
                TuningDecision::Grow(new_size, _reason) => {
                    // Update pool sizes if needed
                    for pool in pools.values_mut() {
                        while pool.len() < new_size {
                            if let Some(array) = pool.back() {
                                pool.push_back(array.clone());
                            } else {
                                break;
                            }
                        }
                    }
                }
                TuningDecision::Shrink(new_size, _reason) => {
                    // Reduce pool sizes if needed
                    for pool in pools.values_mut() {
                        while pool.len() > new_size {
                            pool.pop_front();
                        }
                    }
                }
                TuningDecision::NoAction(_) => {}
            }
        }
    }

    /// Check memory pressure and cleanup if needed
    fn check_memory_pressure(&self, pools: &mut HashMap<DataType, VecDeque<ArrayRef>>) {
        let current_memory = self
            .total_memory_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        let last_cleanup = *self.last_cleanup.lock().unwrap();

        // Cleanup if memory pressure threshold exceeded or cleanup interval passed
        if current_memory > self.config.memory_pressure_threshold
            || last_cleanup.elapsed() > Duration::from_secs(60)
        {
            self.cleanup_old_buffers(pools);
            *self.last_cleanup.lock().unwrap() = Instant::now();
        }
    }

    /// Track memory usage for allocated arrays
    fn track_memory_usage(&self, array: &ArrayRef) {
        let memory_bytes = array.get_buffer_memory_size();
        self.total_memory_bytes
            .fetch_add(memory_bytes, std::sync::atomic::Ordering::Relaxed);
    }

    /// Cleanup old buffers to free memory
    fn cleanup_old_buffers(&self, pools: &mut HashMap<DataType, VecDeque<ArrayRef>>) {
        for pool in pools.values_mut() {
            // Remove buffers that haven't been used recently (keep half)
            let keep_count = (pool.len() / 2).max(1);
            while pool.len() > keep_count {
                pool.pop_front();
            }
        }
    }

    /// Return an array to the pool with LRU management
    pub fn return_array(&self, array: ArrayRef) {
        let mut pools = self.pools.lock().unwrap();
        let dtype = array.data_type().clone();
        let pool = pools.entry(dtype).or_default();

        // Add to back (most recently used)
        pool.push_back(array.clone());

        // Enforce pool size limit
        if pool.len() > self.config.max_size_per_pool {
            pool.pop_front(); // Remove oldest
        }
    }

    /// Get a zero-copy view of an array from the pool
    pub fn get_zero_copy_view(&self, array: &ArrayRef) -> ArrayRef {
        ZeroCopyOps::share_array(array)
    }

    /// Create a zero-copy slice of an array
    pub fn get_slice(
        &self,
        array: &ArrayRef,
        offset: usize,
        length: usize,
    ) -> Result<ArrayRef, crate::error::VectrillError> {
        ZeroCopyOps::slice(array, offset, length)
    }

    /// Create a zero-copy cast view of an array
    pub fn get_cast_view(&self, array: &ArrayRef) -> Result<ArrayRef, crate::error::VectrillError> {
        ZeroCopyOps::cast_view(array)
    }

    /// Get array with zero-copy optimization - try pool first, then create new
    pub fn get_array_zero_copy(&self, dtype: &DataType, capacity: usize) -> ArrayRef {
        // First try to get from pool
        let pooled_array = self.get_array(dtype, capacity);

        // If pooled array is larger than needed, create zero-copy slice
        if pooled_array.len() > capacity {
            if let Ok(sliced) = ZeroCopyOps::slice(&pooled_array, 0, capacity) {
                return sliced;
            }
        }

        pooled_array
    }

    /// Share an array between multiple operations without copying
    pub fn share_for_concurrent_use(
        &self,
        array: &ArrayRef,
        num_consumers: usize,
    ) -> Vec<ArrayRef> {
        (0..num_consumers)
            .map(|_| ZeroCopyOps::share_array(array))
            .collect()
    }

    /// Create a zero-copy view with selected columns for struct arrays
    pub fn select_columns(
        &self,
        array: &ArrayRef,
        column_indices: &[usize],
    ) -> Result<ArrayRef, crate::error::VectrillError> {
        ZeroCopyOps::select_columns(array, column_indices)
    }

    /// Create a zero-copy view with reordered columns
    pub fn reorder_columns(
        &self,
        array: &ArrayRef,
        new_order: &[usize],
    ) -> Result<ArrayRef, crate::error::VectrillError> {
        ZeroCopyOps::reorder_columns(array, new_order)
    }

    /// Get current configuration
    pub fn config(&self) -> &MemoryPoolConfig {
        &self.config
    }

    /// Update configuration
    pub fn update_config(&self, config: MemoryPoolConfig) {
        let mut auto_tuning = self.auto_tuning.lock().unwrap();
        auto_tuning.update_config(config);
    }

    /// Get auto-tuning statistics
    pub fn auto_tuning_stats(&self) -> crate::memory::config::AutoTuningStats {
        let auto_tuning = self.auto_tuning.lock().unwrap();
        auto_tuning.get_stats().clone()
    }

    /// Force auto-tuning to run now
    pub fn force_auto_tuning(&self) -> TuningDecision {
        let pools = self.pools.lock().unwrap();
        let mut auto_tuning = self.auto_tuning.lock().unwrap();
        let current_pool_size = pools.values().map(|pool| pool.len()).sum();
        auto_tuning.tune(current_pool_size)
    }

    /// Get memory usage statistics
    pub fn memory_stats(&self) -> crate::memory::config::AutoTuningStats {
        let mut auto_tuning = self.auto_tuning.lock().unwrap();
        auto_tuning.tracker().get_stats().clone()
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

    /// Get enhanced statistics about the pool
    pub fn stats(&self) -> EnhancedPoolStats {
        let pools = self.pools.lock().unwrap();
        let total_arrays: usize = pools.values().map(|v| v.len()).sum();
        let total_bytes: usize = pools
            .values()
            .flat_map(|v| v.iter())
            .map(|arr| arr.get_buffer_memory_size())
            .sum();

        let current_memory = self
            .total_memory_bytes
            .load(std::sync::atomic::Ordering::Relaxed);
        let memory_pressure = current_memory > self.config.memory_pressure_threshold;
        let last_cleanup = *self.last_cleanup.lock().unwrap();
        let cleanup_elapsed = last_cleanup.elapsed();

        EnhancedPoolStats {
            total_arrays,
            total_bytes,
            pools_count: pools.len(),
            current_memory_bytes: current_memory,
            memory_pressure,
            memory_pressure_threshold: self.config.memory_pressure_threshold,
            last_cleanup_time: last_cleanup,
            cleanup_interval_elapsed: cleanup_elapsed,
        }
    }

    /// Get basic statistics for backward compatibility
    pub fn basic_stats(&self) -> PoolStats {
        let enhanced = self.stats();
        PoolStats {
            total_arrays: enhanced.total_arrays,
            total_bytes: enhanced.total_bytes,
            pools_count: enhanced.pools_count,
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

/// Enhanced statistics about the buffer pool with memory monitoring
#[derive(Debug, Clone)]
pub struct EnhancedPoolStats {
    pub total_arrays: usize,
    pub total_bytes: usize,
    pub pools_count: usize,
    pub current_memory_bytes: usize,
    pub memory_pressure: bool,
    pub memory_pressure_threshold: usize,
    pub last_cleanup_time: std::time::Instant,
    pub cleanup_interval_elapsed: std::time::Duration,
}

/// Global buffer pool instance
static GLOBAL_BUFFER_POOL: once_cell::sync::Lazy<Arc<BufferPool>> =
    once_cell::sync::Lazy::new(|| Arc::new(BufferPool::default()));

/// Get the global buffer pool
pub fn global_buffer_pool() -> Arc<BufferPool> {
    GLOBAL_BUFFER_POOL.clone()
}
