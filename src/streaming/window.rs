//! Window operators for streaming semantics

use std::time::Duration;

/// Window specification
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowSpec {
    /// Tumbling window - non-overlapping windows
    Tumbling {
        size: Duration,
    },
    /// Sliding window - overlapping windows
    Sliding {
        size: Duration,
        slide: Duration,
    },
    /// Session window - dynamic windows based on gaps
    Session {
        gap: Duration,
    },
}

impl WindowSpec {
    /// Get the window size
    pub fn size(&self) -> Duration {
        match self {
            WindowSpec::Tumbling { size } => *size,
            WindowSpec::Sliding { size, .. } => *size,
            WindowSpec::Session { gap } => *gap,
        }
    }
}

/// Window key - identifies a specific window
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct WindowKey {
    /// Start timestamp of the window
    pub start: i64,
    /// End timestamp of the window
    pub end: i64,
}

impl WindowKey {
    /// Create a new window key
    pub fn new(start: i64, end: i64) -> Self {
        Self { start, end }
    }
    
    /// Check if a timestamp falls within this window
    pub fn contains(&self, timestamp: i64) -> bool {
        timestamp >= self.start && timestamp < self.end
    }
    
    /// Calculate the window duration
    pub fn duration(&self) -> i64 {
        self.end - self.start
    }
}

/// Window type
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowType {
    Tumbling,
    Sliding,
    Session,
}

impl WindowType {
    /// Convert from WindowSpec
    pub fn from_spec(spec: &WindowSpec) -> Self {
        match spec {
            WindowSpec::Tumbling { .. } => WindowType::Tumbling,
            WindowSpec::Sliding { .. } => WindowType::Sliding,
            WindowSpec::Session { .. } => WindowType::Session,
        }
    }
}

/// Assign a timestamp to a window based on the window specification
pub fn assign_to_window(timestamp: i64, spec: &WindowSpec) -> WindowKey {
    match spec {
        WindowSpec::Tumbling { size } => {
            let size_ms = size.as_millis() as i64;
            let window_start = (timestamp / size_ms) * size_ms;
            WindowKey::new(window_start, window_start + size_ms)
        }
        WindowSpec::Sliding { size, slide } => {
            let slide_ms = slide.as_millis() as i64;
            let window_start = (timestamp / slide_ms) * slide_ms;
            let size_ms = size.as_millis() as i64;
            WindowKey::new(window_start, window_start + size_ms)
        }
        WindowSpec::Session { gap } => {
            // Session windows are dynamic, so this is a simplified version
            // In a real implementation, we'd need to track session state
            let gap_ms = gap.as_millis() as i64;
            WindowKey::new(timestamp, timestamp + gap_ms)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_window_spec_tumbling() {
        let spec = WindowSpec::Tumbling {
            size: Duration::from_secs(10),
        };
        assert_eq!(spec.size(), Duration::from_secs(10));
    }
    
    #[test]
    fn test_window_spec_sliding() {
        let spec = WindowSpec::Sliding {
            size: Duration::from_secs(10),
            slide: Duration::from_secs(5),
        };
        assert_eq!(spec.size(), Duration::from_secs(10));
    }
    
    #[test]
    fn test_window_key_creation() {
        let key = WindowKey::new(0, 1000);
        assert_eq!(key.start, 0);
        assert_eq!(key.end, 1000);
    }
    
    #[test]
    fn test_window_key_contains() {
        let key = WindowKey::new(0, 1000);
        assert!(key.contains(500));
        assert!(!key.contains(1500));
        assert!(key.contains(0));
        assert!(!key.contains(1000)); // end is exclusive
    }
    
    #[test]
    fn test_assign_to_tumbling_window() {
        let spec = WindowSpec::Tumbling {
            size: Duration::from_secs(10),
        };
        let key = assign_to_window(15000, &spec);
        assert_eq!(key.start, 10000);
        assert_eq!(key.end, 20000);
    }
    
    #[test]
    fn test_assign_to_sliding_window() {
        let spec = WindowSpec::Sliding {
            size: Duration::from_secs(10),
            slide: Duration::from_secs(5),
        };
        let key = assign_to_window(15000, &spec);
        assert_eq!(key.start, 15000);
        assert_eq!(key.end, 25000);
    }
}
