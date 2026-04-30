//! Watermark system for streaming semantics

use std::collections::HashMap;

/// Represents a watermark from a specific source
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Watermark {
    /// The timestamp of the watermark
    pub timestamp: i64,
    /// The source that generated this watermark
    pub source: String,
}

impl Watermark {
    /// Create a new watermark
    pub fn new(timestamp: i64, source: String) -> Self {
        Self { timestamp, source }
    }
}

/// Tracks watermarks from multiple sources and computes the global watermark
#[derive(Debug, Clone)]
pub struct WatermarkTracker {
    /// Per-source watermarks
    pub per_source: HashMap<String, i64>,
    /// Global watermark (minimum of all source watermarks minus max lateness)
    pub global: i64,
    /// Maximum allowed lateness
    pub max_lateness: i64,
}

impl WatermarkTracker {
    /// Create a new watermark tracker
    pub fn new(max_lateness: i64) -> Self {
        Self {
            per_source: HashMap::new(),
            global: i64::MIN,
            max_lateness,
        }
    }
    
    /// Update the watermark for a specific source
    pub fn update(&mut self, source: &str, timestamp: i64) {
        self.per_source.insert(source.to_string(), timestamp);
        self.recompute_global();
    }
    
    /// Recompute the global watermark from all source watermarks
    fn recompute_global(&mut self) {
        if self.per_source.is_empty() {
            self.global = i64::MIN;
        } else {
            // Global watermark is the minimum of all source watermarks
            let min_timestamp = *self.per_source.values().min().unwrap_or(&i64::MAX);
            self.global = min_timestamp.saturating_sub(self.max_lateness);
        }
    }
    
    /// Get the current global watermark
    pub fn global_watermark(&self) -> i64 {
        self.global
    }
    
    /// Get the watermark for a specific source
    pub fn source_watermark(&self, source: &str) -> Option<i64> {
        self.per_source.get(source).copied()
    }
    
    /// Check if an event is late based on the global watermark
    pub fn is_late(&self, event_timestamp: i64) -> bool {
        event_timestamp < self.global
    }
}

impl Default for WatermarkTracker {
    fn default() -> Self {
        Self::new(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_watermark_creation() {
        let watermark = Watermark::new(100, "source1".to_string());
        assert_eq!(watermark.timestamp, 100);
        assert_eq!(watermark.source, "source1");
    }
    
    #[test]
    fn test_watermark_tracker_creation() {
        let tracker = WatermarkTracker::new(10);
        assert_eq!(tracker.global, i64::MIN);
        assert_eq!(tracker.max_lateness, 10);
        assert!(tracker.per_source.is_empty());
    }
    
    #[test]
    fn test_watermark_update() {
        let mut tracker = WatermarkTracker::new(5);
        tracker.update("source1", 100);
        
        assert_eq!(tracker.source_watermark("source1"), Some(100));
        assert_eq!(tracker.global_watermark(), 95); // 100 - 5
    }
    
    #[test]
    fn test_global_watermark_aggregation() {
        let mut tracker = WatermarkTracker::new(10);
        tracker.update("source1", 100);
        tracker.update("source2", 150);
        
        // Global should be min(100, 150) - 10 = 90
        assert_eq!(tracker.global_watermark(), 90);
    }
    
    #[test]
    fn test_late_detection() {
        let mut tracker = WatermarkTracker::new(5);
        tracker.update("source1", 100);
        
        // Event at timestamp 90 is late (90 < 95)
        assert!(tracker.is_late(90));
        
        // Event at timestamp 100 is not late (100 >= 95)
        assert!(!tracker.is_late(100));
    }
}
