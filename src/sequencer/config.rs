//! Configuration for the sequencer

/// Ordering mode for events
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Ordering {
    /// Order by timestamp only
    #[default]
    ByTimestamp,
    /// Order by key, then by timestamp
    ByKeyThenTimestamp,
}

/// Policy for handling late data
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LateDataPolicy {
    /// Drop late data
    #[default]
    Drop,
    /// Allow late data
    Allow,
    /// Send late data to side output
    SideOutput,
}

/// Configuration for the sequencer
#[derive(Debug, Clone)]
pub struct SequencerConfig {
    /// Ordering mode
    pub ordering: Ordering,
    /// Maximum allowed lateness in milliseconds
    pub max_lateness_ms: i64,
    /// Target batch size
    pub batch_size: usize,
    /// Flush interval in milliseconds
    pub flush_interval_ms: u64,
    /// Policy for handling late data
    pub late_data_policy: LateDataPolicy,
}

impl Default for SequencerConfig {
    fn default() -> Self {
        Self {
            ordering: Ordering::default(),
            max_lateness_ms: 5000, // 5 seconds
            batch_size: 1000,
            flush_interval_ms: 1000, // 1 second
            late_data_policy: LateDataPolicy::default(),
        }
    }
}

impl SequencerConfig {
    /// Create a new sequencer configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the ordering mode
    pub fn with_ordering(mut self, ordering: Ordering) -> Self {
        self.ordering = ordering;
        self
    }

    /// Set the maximum lateness
    pub fn with_max_lateness_ms(mut self, max_lateness_ms: i64) -> Self {
        self.max_lateness_ms = max_lateness_ms;
        self
    }

    /// Set the batch size
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the flush interval
    pub fn with_flush_interval_ms(mut self, flush_interval_ms: u64) -> Self {
        self.flush_interval_ms = flush_interval_ms;
        self
    }

    /// Set the late data policy
    pub fn with_late_data_policy(mut self, late_data_policy: LateDataPolicy) -> Self {
        self.late_data_policy = late_data_policy;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = SequencerConfig::default();
        assert_eq!(config.ordering, Ordering::ByTimestamp);
        assert_eq!(config.max_lateness_ms, 5000);
        assert_eq!(config.batch_size, 1000);
        assert_eq!(config.flush_interval_ms, 1000);
        assert_eq!(config.late_data_policy, LateDataPolicy::Drop);
    }

    #[test]
    fn test_config_builder() {
        let config = SequencerConfig::new()
            .with_ordering(Ordering::ByKeyThenTimestamp)
            .with_max_lateness_ms(10000)
            .with_batch_size(500)
            .with_flush_interval_ms(2000)
            .with_late_data_policy(LateDataPolicy::Allow);

        assert_eq!(config.ordering, Ordering::ByKeyThenTimestamp);
        assert_eq!(config.max_lateness_ms, 10000);
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.flush_interval_ms, 2000);
        assert_eq!(config.late_data_policy, LateDataPolicy::Allow);
    }
}
