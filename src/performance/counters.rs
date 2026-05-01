//! Performance counters for monitoring execution metrics

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Performance counter types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CounterType {
    /// Total rows processed
    RowsProcessed,
    /// Total batches processed
    BatchesProcessed,
    /// Total time spent in microseconds
    TotalTimeUs,
    /// Number of allocations
    Allocations,
    /// Memory allocated in bytes
    MemoryAllocated,
    /// Cache hits
    CacheHits,
    /// Cache misses
    CacheMisses,
}

/// Performance counter
#[derive(Debug, Clone)]
pub struct Counter {
    counter_type: CounterType,
    value: Arc<AtomicU64>,
}

impl Counter {
    /// Create a new counter
    pub fn new(counter_type: CounterType) -> Self {
        Self {
            counter_type,
            value: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Increment the counter by 1
    pub fn increment(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    /// Add a value to the counter
    pub fn add(&self, value: u64) {
        self.value.fetch_add(value, Ordering::Relaxed);
    }

    /// Get the current value
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    /// Reset the counter to 0
    pub fn reset(&self) {
        self.value.store(0, Ordering::Relaxed);
    }

    /// Get the counter type
    pub fn counter_type(&self) -> CounterType {
        self.counter_type
    }
}

/// Performance counter registry
pub struct CounterRegistry {
    counters: HashMap<String, Arc<Counter>>,
}

impl CounterRegistry {
    /// Create a new counter registry
    pub fn new() -> Self {
        Self {
            counters: HashMap::new(),
        }
    }

    /// Register a new counter
    pub fn register(&mut self, name: String, counter_type: CounterType) -> Arc<Counter> {
        let counter = Arc::new(Counter::new(counter_type));
        self.counters.insert(name, counter.clone());
        counter
    }

    /// Get a counter by name
    pub fn get(&self, name: &str) -> Option<Arc<Counter>> {
        self.counters.get(name).cloned()
    }

    /// Get all counter values
    pub fn snapshot(&self) -> HashMap<String, u64> {
        self.counters
            .iter()
            .map(|(name, counter)| (name.clone(), counter.get()))
            .collect()
    }

    /// Reset all counters
    pub fn reset_all(&self) {
        for counter in self.counters.values() {
            counter.reset();
        }
    }

    /// Get the number of registered counters
    pub fn len(&self) -> usize {
        self.counters.len()
    }

    pub fn is_empty(&self) -> bool {
        self.counters.is_empty()
    }
}

impl Default for CounterRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Performance timer for measuring execution time
pub struct Timer {
    counter: Arc<Counter>,
    start: Option<std::time::Instant>,
}

impl Timer {
    /// Create a new timer
    pub fn new(counter: Arc<Counter>) -> Self {
        Self {
            counter,
            start: None,
        }
    }

    /// Start the timer
    pub fn start(&mut self) {
        self.start = Some(std::time::Instant::now());
    }

    /// Stop the timer and record the elapsed time in microseconds
    pub fn stop(&mut self) {
        if let Some(start) = self.start.take() {
            let elapsed = start.elapsed().as_micros() as u64;
            self.counter.add(elapsed);
        }
    }

    /// Record the elapsed time without stopping (for cumulative timing)
    pub fn record(&mut self) {
        if let Some(start) = self.start {
            let elapsed = start.elapsed().as_micros() as u64;
            self.counter.add(elapsed);
            self.start = Some(std::time::Instant::now());
        }
    }
}

impl Drop for Timer {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Global counter registry instance
static GLOBAL_COUNTER_REGISTRY: once_cell::sync::Lazy<Arc<std::sync::Mutex<CounterRegistry>>> =
    once_cell::sync::Lazy::new(|| Arc::new(std::sync::Mutex::new(CounterRegistry::new())));

/// Get the global counter registry
pub fn global_counter_registry() -> Arc<std::sync::Mutex<CounterRegistry>> {
    GLOBAL_COUNTER_REGISTRY.clone()
}
