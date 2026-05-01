//! Metrics collection and export system

mod registry;
mod types;

pub use registry::{get_global_metrics, MetricsRegistry};
pub use types::{Metric, MetricType, MetricValue};

use std::sync::Arc;

/// Global metrics registry instance
static GLOBAL_REGISTRY: once_cell::sync::Lazy<Arc<MetricsRegistry>> =
    once_cell::sync::Lazy::new(|| Arc::new(MetricsRegistry::new()));

/// Get the global metrics registry
pub fn global_registry() -> Arc<MetricsRegistry> {
    GLOBAL_REGISTRY.clone()
}

/// Initialize metrics collection
pub fn init() {
    let _ = global_registry();
}
