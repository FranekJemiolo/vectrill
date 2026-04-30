//! Metric types and values

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Metric type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
}

/// Metric value
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MetricValue {
    Counter(u64),
    Gauge(f64),
    Histogram(HistogramData),
}

/// Histogram data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistogramData {
    pub buckets: Vec<f64>,
    pub counts: Vec<u64>,
    pub sum: f64,
    pub count: u64,
}

/// A metric with labels
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metric {
    pub name: String,
    pub metric_type: MetricType,
    pub value: MetricValue,
    pub labels: HashMap<String, String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl Metric {
    pub fn new_counter(name: String, value: u64, labels: HashMap<String, String>) -> Self {
        Self {
            name,
            metric_type: MetricType::Counter,
            value: MetricValue::Counter(value),
            labels,
            timestamp: chrono::Utc::now(),
        }
    }

    pub fn new_gauge(name: String, value: f64, labels: HashMap<String, String>) -> Self {
        Self {
            name,
            metric_type: MetricType::Gauge,
            value: MetricValue::Gauge(value),
            labels,
            timestamp: chrono::Utc::now(),
        }
    }

    pub fn new_histogram(
        name: String,
        buckets: Vec<f64>,
        counts: Vec<u64>,
        sum: f64,
        count: u64,
        labels: HashMap<String, String>,
    ) -> Self {
        Self {
            name,
            metric_type: MetricType::Histogram,
            value: MetricValue::Histogram(HistogramData {
                buckets,
                counts,
                sum,
                count,
            }),
            labels,
            timestamp: chrono::Utc::now(),
        }
    }
}
