//! Window state management for streaming semantics

use std::collections::HashMap;
use crate::streaming::window::WindowKey;

/// Aggregate state for a window
#[derive(Debug, Clone)]
pub struct AggregateState {
    /// Count of events in the window
    pub count: usize,
    /// Sum for numeric aggregations
    pub sum: f64,
    /// Minimum value
    pub min: f64,
    /// Maximum value
    pub max: f64,
}

impl AggregateState {
    /// Create a new aggregate state
    pub fn new() -> Self {
        Self {
            count: 0,
            sum: 0.0,
            min: f64::MAX,
            max: f64::MIN,
        }
    }
    
    /// Update the state with a new value
    pub fn update(&mut self, value: f64) {
        self.count += 1;
        self.sum += value;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
    }
    
    /// Calculate the average
    pub fn avg(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }
}

impl Default for AggregateState {
    fn default() -> Self {
        Self::new()
    }
}

/// Window state - stores aggregates for a specific window
#[derive(Debug, Clone)]
pub struct WindowState {
    /// Window key
    pub window_key: WindowKey,
    /// Optional group key (for keyed windows)
    pub group_key: Option<String>,
    /// Aggregates per column
    pub aggregates: HashMap<String, AggregateState>,
    /// Total count of events in this window
    pub count: usize,
}

impl WindowState {
    /// Create a new window state
    pub fn new(window_key: WindowKey, group_key: Option<String>) -> Self {
        Self {
            window_key,
            group_key,
            aggregates: HashMap::new(),
            count: 0,
        }
    }
    
    /// Update an aggregate for a specific column
    pub fn update_aggregate(&mut self, column: &str, value: f64) {
        let agg = self.aggregates.entry(column.to_string()).or_default();
        agg.update(value);
        self.count += 1;
    }
    
    /// Get the aggregate state for a column
    pub fn get_aggregate(&self, column: &str) -> Option<&AggregateState> {
        self.aggregates.get(column)
    }
    
    /// Check if this window is expired based on a watermark
    pub fn is_expired(&self, watermark: i64) -> bool {
        self.window_key.end <= watermark
    }
}

/// In-memory state store for window states
#[derive(Debug, Clone)]
pub struct WindowStateStore {
    /// States keyed by (window_key, group_key)
    states: HashMap<(WindowKey, Option<String>), WindowState>,
    /// Maximum number of states to keep
    max_states: usize,
}

impl WindowStateStore {
    /// Create a new window state store
    pub fn new(max_states: usize) -> Self {
        Self {
            states: HashMap::new(),
            max_states,
        }
    }
    
    /// Get or create a window state
    pub fn get_or_create(&mut self, window_key: WindowKey, group_key: Option<String>) -> &mut WindowState {
        let key = (window_key, group_key);
        if !self.states.contains_key(&key) {
            // Evict old states if we're at capacity
            if self.states.len() >= self.max_states {
                self.evict_oldest();
            }
            self.states.insert(
                key.clone(),
                WindowState::new(key.0.clone(), key.1.clone()),
            );
        }
        self.states.get_mut(&key).unwrap()
    }
    
    /// Get a window state
    pub fn get(&self, window_key: &WindowKey, group_key: &Option<String>) -> Option<&WindowState> {
        self.states.get(&(window_key.clone(), group_key.clone()))
    }
    
    /// Remove expired states based on watermark
    pub fn remove_expired(&mut self, watermark: i64) -> Vec<WindowState> {
        let mut expired = Vec::new();
        self.states.retain(|(window_key, _), state| {
            if state.is_expired(watermark) {
                expired.push(state.clone());
                false
            } else {
                true
            }
        });
        expired
    }
    
    /// Evict the oldest state (simple LRU approximation)
    fn evict_oldest(&mut self) {
        if let Some(key) = self.states.keys().cloned().next() {
            self.states.remove(&key);
        }
    }
    
    /// Get the number of states in the store
    pub fn len(&self) -> usize {
        self.states.len()
    }
    
    /// Check if the store is empty
    pub fn is_empty(&self) -> bool {
        self.states.is_empty()
    }
}

impl Default for WindowStateStore {
    fn default() -> Self {
        Self::new(1000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_aggregate_state() {
        let mut state = AggregateState::new();
        state.update(10.0);
        state.update(20.0);
        state.update(30.0);
        
        assert_eq!(state.count, 3);
        assert_eq!(state.sum, 60.0);
        assert_eq!(state.avg(), 20.0);
        assert_eq!(state.min, 10.0);
        assert_eq!(state.max, 30.0);
    }
    
    #[test]
    fn test_window_state() {
        let window_key = WindowKey::new(0, 1000);
        let mut state = WindowState::new(window_key.clone(), Some("group1".to_string()));
        
        state.update_aggregate("value", 10.0);
        state.update_aggregate("value", 20.0);
        
        assert_eq!(state.count, 2);
        assert_eq!(state.window_key, window_key);
        assert_eq!(state.group_key, Some("group1".to_string()));
    }
    
    #[test]
    fn test_window_state_store() {
        let mut store = WindowStateStore::new(10);
        let window_key = WindowKey::new(0, 1000);
        
        let state = store.get_or_create(window_key.clone(), Some("group1".to_string()));
        state.update_aggregate("value", 10.0);
        
        assert_eq!(store.len(), 1);
        
        let retrieved = store.get(&window_key, &Some("group1".to_string()));
        assert!(retrieved.is_some());
    }
    
    #[test]
    fn test_remove_expired() {
        let mut store = WindowStateStore::new(10);
        let window_key1 = WindowKey::new(0, 1000);
        let window_key2 = WindowKey::new(2000, 3000);
        
        store.get_or_create(window_key1, None).update_aggregate("value", 10.0);
        store.get_or_create(window_key2, None).update_aggregate("value", 20.0);
        
        assert_eq!(store.len(), 2);
        
        let expired = store.remove_expired(1500);
        assert_eq!(expired.len(), 1);
        assert_eq!(store.len(), 1);
    }
}
