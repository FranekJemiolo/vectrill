//! Memory pool configuration and auto-tuning

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// Memory pool configuration with auto-tuning capabilities
#[derive(Debug, Clone)]
pub struct MemoryPoolConfig {
    /// Maximum size per pool
    pub max_size_per_pool: usize,
    /// Memory pressure threshold in bytes
    pub memory_pressure_threshold: usize,
    /// Auto-tuning enabled
    pub auto_tuning_enabled: bool,
    /// Target memory utilization percentage (0-100)
    pub target_utilization: f32,
    /// Minimum pool size
    pub min_pool_size: usize,
    /// Maximum pool size
    pub max_pool_size: usize,
    /// Auto-tuning interval
    pub tuning_interval: Duration,
    /// Growth factor for pool expansion
    pub growth_factor: f32,
    /// Shrink factor for pool contraction
    pub shrink_factor: f32,
    /// Memory usage history for auto-tuning
    pub history_size: usize,
}

impl Default for MemoryPoolConfig {
    fn default() -> Self {
        Self {
            max_size_per_pool: 1000,
            memory_pressure_threshold: 100 * 1024 * 1024, // 100MB
            auto_tuning_enabled: true,
            target_utilization: 75.0, // 75% target utilization
            min_pool_size: 100,
            max_pool_size: 10000,
            tuning_interval: Duration::from_secs(30),
            growth_factor: 1.5,
            shrink_factor: 0.75,
            history_size: 100,
        }
    }
}

impl MemoryPoolConfig {
    /// Create a new configuration with custom parameters
    pub fn new(max_size_per_pool: usize) -> Self {
        Self {
            max_size_per_pool,
            ..Default::default()
        }
    }

    /// Create a configuration with memory limit
    pub fn with_memory_limit(max_size_per_pool: usize, memory_limit_mb: usize) -> Self {
        Self {
            max_size_per_pool,
            memory_pressure_threshold: memory_limit_mb * 1024 * 1024,
            ..Default::default()
        }
    }

    /// Enable auto-tuning with custom parameters
    pub fn with_auto_tuning(mut self, target_utilization: f32, tuning_interval_secs: u64) -> Self {
        self.auto_tuning_enabled = true;
        self.target_utilization = target_utilization.clamp(50.0, 95.0);
        self.tuning_interval = Duration::from_secs(tuning_interval_secs);
        self
    }

    /// Set pool size bounds
    pub fn with_pool_bounds(mut self, min_size: usize, max_size: usize) -> Self {
        self.min_pool_size = min_size;
        self.max_pool_size = max_size;
        self
    }

    /// Set growth and shrink factors
    pub fn with_adaptive_factors(mut self, growth_factor: f32, shrink_factor: f32) -> Self {
        self.growth_factor = growth_factor.clamp(1.1, 3.0);
        self.shrink_factor = shrink_factor.clamp(0.5, 0.9);
        self
    }

    /// Disable auto-tuning
    pub fn without_auto_tuning(mut self) -> Self {
        self.auto_tuning_enabled = false;
        self
    }
}

/// Auto-tuning statistics for memory pool
#[derive(Debug, Clone)]
pub struct AutoTuningStats {
    /// Current pool size
    pub current_pool_size: usize,
    /// Average utilization over time window
    pub avg_utilization: f32,
    /// Peak utilization
    pub peak_utilization: f32,
    /// Number of tuning operations performed
    pub tuning_operations: usize,
    /// Last tuning time
    pub last_tuning_time: Option<Instant>,
    /// Memory pressure events
    pub pressure_events: usize,
    /// Growth operations
    pub growth_operations: usize,
    /// Shrink operations
    pub shrink_operations: usize,
}

impl Default for AutoTuningStats {
    fn default() -> Self {
        Self {
            current_pool_size: 0,
            avg_utilization: 0.0,
            peak_utilization: 0.0,
            tuning_operations: 0,
            last_tuning_time: None,
            pressure_events: 0,
            growth_operations: 0,
            shrink_operations: 0,
        }
    }
}

/// Memory usage tracker for auto-tuning
pub struct MemoryUsageTracker {
    /// Memory usage history
    usage_history: Vec<f32>,
    /// Timestamp history
    timestamp_history: Vec<Instant>,
    /// Maximum history size
    max_history_size: usize,
    /// Current memory usage in bytes
    current_usage: AtomicUsize,
    /// Peak memory usage
    peak_usage: AtomicUsize,
    /// Total allocations
    total_allocations: AtomicUsize,
    /// Total deallocations
    total_deallocations: AtomicUsize,
}

impl MemoryUsageTracker {
    /// Create a new memory usage tracker
    pub fn new(max_history_size: usize) -> Self {
        Self {
            usage_history: Vec::with_capacity(max_history_size),
            timestamp_history: Vec::with_capacity(max_history_size),
            max_history_size,
            current_usage: AtomicUsize::new(0),
            peak_usage: AtomicUsize::new(0),
            total_allocations: AtomicUsize::new(0),
            total_deallocations: AtomicUsize::new(0),
        }
    }

    /// Record memory allocation
    pub fn record_allocation(&self, size: usize) {
        self.current_usage.fetch_add(size, Ordering::Relaxed);
        self.total_allocations.fetch_add(size, Ordering::Relaxed);

        // Update peak usage
        let current = self.current_usage.load(Ordering::Relaxed);
        let peak = self.peak_usage.load(Ordering::Relaxed);
        if current > peak {
            self.peak_usage.store(current, Ordering::Relaxed);
        }
    }

    /// Record memory deallocation
    pub fn record_deallocation(&self, size: usize) {
        self.current_usage.fetch_sub(size, Ordering::Relaxed);
        self.total_deallocations.fetch_add(size, Ordering::Relaxed);
    }

    /// Get current memory usage
    pub fn current_usage(&self) -> usize {
        self.current_usage.load(Ordering::Relaxed)
    }

    /// Get peak memory usage
    pub fn peak_usage(&self) -> usize {
        self.peak_usage.load(Ordering::Relaxed)
    }

    /// Get utilization percentage
    pub fn utilization(&self, limit: usize) -> f32 {
        let current = self.current_usage.load(Ordering::Relaxed);
        if limit == 0 {
            0.0
        } else {
            (current as f32 / limit as f32) * 100.0
        }
    }

    /// Add usage sample to history
    pub fn add_sample(&mut self, utilization: f32) {
        let now = Instant::now();

        self.usage_history.push(utilization);
        self.timestamp_history.push(now);

        // Maintain history size limit
        if self.usage_history.len() > self.max_history_size {
            self.usage_history.remove(0);
            self.timestamp_history.remove(0);
        }
    }

    /// Get average utilization over history
    pub fn avg_utilization(&self) -> f32 {
        if self.usage_history.is_empty() {
            0.0
        } else {
            self.usage_history.iter().sum::<f32>() / self.usage_history.len() as f32
        }
    }

    /// Get peak utilization over history
    pub fn peak_utilization(&self) -> f32 {
        self.usage_history
            .iter()
            .fold(0.0, |max, &val| max.max(val))
    }

    /// Get utilization trend (positive = increasing, negative = decreasing)
    pub fn utilization_trend(&self) -> f32 {
        if self.usage_history.len() < 2 {
            0.0
        } else {
            let recent = &self.usage_history[self.usage_history.len() - 2..];
            let slope = (recent[1] - recent[0]) / recent[0].max(1.0);
            slope
        }
    }

    /// Get statistics
    pub fn get_stats(&self) -> AutoTuningStats {
        AutoTuningStats {
            current_pool_size: self.current_usage(),
            avg_utilization: self.avg_utilization(),
            peak_utilization: self.peak_utilization(),
            tuning_operations: 0,   // Tracked separately
            last_tuning_time: None, // Tracked separately
            pressure_events: 0,     // Tracked separately
            growth_operations: 0,   // Tracked separately
            shrink_operations: 0,   // Tracked separately
        }
    }

    /// Reset statistics
    pub fn reset(&mut self) {
        self.usage_history.clear();
        self.timestamp_history.clear();
        self.current_usage.store(0, Ordering::Relaxed);
        self.peak_usage.store(0, Ordering::Relaxed);
        self.total_allocations.store(0, Ordering::Relaxed);
        self.total_deallocations.store(0, Ordering::Relaxed);
    }
}

/// Auto-tuning engine for memory pools
pub struct AutoTuningEngine {
    /// Configuration
    config: MemoryPoolConfig,
    /// Usage tracker
    tracker: MemoryUsageTracker,
    /// Last tuning time
    last_tuning: Option<Instant>,
    /// Statistics
    stats: AutoTuningStats,
}

impl AutoTuningEngine {
    /// Create a new auto-tuning engine
    pub fn new(config: MemoryPoolConfig) -> Self {
        Self {
            tracker: MemoryUsageTracker::new(config.history_size),
            config,
            last_tuning: None,
            stats: AutoTuningStats::default(),
        }
    }

    /// Check if tuning should be performed
    pub fn should_tune(&self) -> bool {
        if !self.config.auto_tuning_enabled {
            return false;
        }

        if let Some(last) = self.last_tuning {
            last.elapsed() >= self.config.tuning_interval
        } else {
            true
        }
    }

    /// Perform auto-tuning based on current usage patterns
    pub fn tune(&mut self, current_pool_size: usize) -> TuningDecision {
        let utilization = self
            .tracker
            .utilization(self.config.memory_pressure_threshold);
        let _avg_utilization = self.tracker.avg_utilization();
        let trend = self.tracker.utilization_trend();

        self.tracker.add_sample(utilization);

        let decision = if utilization > self.config.target_utilization + 10.0 {
            // High utilization - consider growing pool
            if current_pool_size < self.config.max_pool_size {
                let new_size = ((current_pool_size as f32 * self.config.growth_factor) as usize)
                    .min(self.config.max_pool_size);
                TuningDecision::Grow(new_size, format!("High utilization: {:.1}%", utilization))
            } else {
                TuningDecision::NoAction(format!(
                    "Pool at max size, high utilization: {:.1}%",
                    utilization
                ))
            }
        } else if utilization < self.config.target_utilization - 20.0 && trend < -0.1 {
            // Low utilization with decreasing trend - consider shrinking
            if current_pool_size > self.config.min_pool_size {
                let new_size = ((current_pool_size as f32 * self.config.shrink_factor) as usize)
                    .max(self.config.min_pool_size);
                TuningDecision::Shrink(
                    new_size,
                    format!("Low utilization: {:.1}%, trend: {:.2}", utilization, trend),
                )
            } else {
                TuningDecision::NoAction(format!(
                    "Pool at min size, low utilization: {:.1}%",
                    utilization
                ))
            }
        } else {
            TuningDecision::NoAction(format!("Utilization within target: {:.1}%", utilization))
        };

        // Update statistics
        match &decision {
            TuningDecision::Grow(_, _) => {
                self.stats.growth_operations += 1;
                self.stats.tuning_operations += 1;
            }
            TuningDecision::Shrink(_, _) => {
                self.stats.shrink_operations += 1;
                self.stats.tuning_operations += 1;
            }
            TuningDecision::NoAction(_) => {}
        }

        self.last_tuning = Some(Instant::now());
        decision
    }

    /// Record memory pressure event
    pub fn record_pressure_event(&mut self) {
        self.stats.pressure_events += 1;
    }

    /// Get current statistics
    pub fn get_stats(&self) -> &AutoTuningStats {
        &self.stats
    }

    /// Get mutable tracker
    pub fn tracker(&mut self) -> &mut MemoryUsageTracker {
        &mut self.tracker
    }

    /// Update configuration
    pub fn update_config(&mut self, config: MemoryPoolConfig) {
        self.tracker.max_history_size = config.history_size;
        self.config = config;
    }
}

/// Auto-tuning decision
#[derive(Debug, Clone)]
pub enum TuningDecision {
    /// Grow pool to new size
    Grow(usize, String),
    /// Shrink pool to new size
    Shrink(usize, String),
    /// No action needed
    NoAction(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_pool_config_default() {
        let config = MemoryPoolConfig::default();
        assert_eq!(config.max_size_per_pool, 1000);
        assert_eq!(config.memory_pressure_threshold, 100 * 1024 * 1024);
        assert!(config.auto_tuning_enabled);
        assert_eq!(config.target_utilization, 75.0);
    }

    #[test]
    fn test_memory_pool_config_builder() {
        let config = MemoryPoolConfig::with_memory_limit(500, 50)
            .with_auto_tuning(80.0, 60)
            .with_pool_bounds(50, 5000)
            .with_adaptive_factors(2.0, 0.8);

        assert_eq!(config.max_size_per_pool, 500);
        assert_eq!(config.memory_pressure_threshold, 50 * 1024 * 1024);
        assert_eq!(config.target_utilization, 80.0);
        assert_eq!(config.min_pool_size, 50);
        assert_eq!(config.max_pool_size, 5000);
        assert_eq!(config.growth_factor, 2.0);
        assert_eq!(config.shrink_factor, 0.8);
    }

    #[test]
    fn test_memory_usage_tracker() {
        let mut tracker = MemoryUsageTracker::new(10);

        tracker.record_allocation(1000);
        assert_eq!(tracker.current_usage(), 1000);
        assert_eq!(tracker.peak_usage(), 1000);

        tracker.record_deallocation(500);
        assert_eq!(tracker.current_usage(), 500);
        assert_eq!(tracker.peak_usage(), 1000);

        tracker.add_sample(75.0);
        tracker.add_sample(80.0);
        assert_eq!(tracker.avg_utilization(), 77.5);
        assert_eq!(tracker.peak_utilization(), 80.0);
    }

    #[test]
    fn test_auto_tuning_engine() {
        let config = MemoryPoolConfig::new(1000)
            .with_auto_tuning(75.0, 30)
            .with_pool_bounds(100, 5000);

        let mut engine = AutoTuningEngine::new(config);

        // Test tuning decision
        let decision = engine.tune(1000);
        match decision {
            TuningDecision::NoAction(reason) => {
                assert!(reason.contains("Utilization within target"));
            }
            _ => panic!("Expected NoAction decision"),
        }
    }
}
