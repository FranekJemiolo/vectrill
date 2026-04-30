//! Metrics registry for collecting and storing metrics

use super::types::Metric;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::RwLock as TokioRwLock;

/// Metrics registry for collecting metrics
pub struct MetricsRegistry {
    counters: RwLock<HashMap<String, Arc<AtomicU64>>>,
    gauges: RwLock<HashMap<String, Arc<AtomicU64>>>,
    histograms: RwLock<HashMap<String, Histogram>>,
    metrics: TokioRwLock<Vec<Metric>>,
}

impl MetricsRegistry {
    /// Create a new metrics registry
    pub fn new() -> Self {
        Self {
            counters: RwLock::new(HashMap::new()),
            gauges: RwLock::new(HashMap::new()),
            histograms: RwLock::new(HashMap::new()),
            metrics: TokioRwLock::new(Vec::new()),
        }
    }

    /// Increment a counter
    pub fn increment_counter(&self, name: &str, value: u64, labels: HashMap<String, String>) {
        let counters = self.counters.read().unwrap();
        let counter = counters.get(name).cloned().unwrap_or_else(|| {
            drop(counters);
            let mut counters = self.counters.write().unwrap();
            counters
                .entry(name.to_string())
                .or_insert_with(|| Arc::new(AtomicU64::new(0)))
                .clone()
        });

        counter.fetch_add(value, Ordering::Relaxed);

        // Record metric
        let metric = Metric::new_counter(name.to_string(), counter.load(Ordering::Relaxed), labels);
        tokio::spawn(async move {
            let mut metrics = GLOBAL_METRICS.write().await;
            metrics.push(metric);
        });
    }

    /// Set a gauge value
    pub fn set_gauge(&self, name: &str, value: f64, labels: HashMap<String, String>) {
        let int_value = (value * 1000.0) as u64;
        let gauges = self.gauges.read().unwrap();
        let gauge = gauges.get(name).cloned().unwrap_or_else(|| {
            drop(gauges);
            let mut gauges = self.gauges.write().unwrap();
            gauges
                .entry(name.to_string())
                .or_insert_with(|| Arc::new(AtomicU64::new(0)))
                .clone()
        });

        gauge.store(int_value, Ordering::Relaxed);

        // Record metric
        let metric = Metric::new_gauge(name.to_string(), value, labels);
        tokio::spawn(async move {
            let mut metrics = GLOBAL_METRICS.write().await;
            metrics.push(metric);
        });
    }

    /// Record a histogram value
    pub fn record_histogram(&self, name: &str, value: f64, labels: HashMap<String, String>) {
        let mut histograms = self.histograms.write().unwrap();
        let histogram = histograms
            .entry(name.to_string())
            .or_insert_with(|| Histogram::new(vec![0.5, 1.0, 5.0, 10.0, 50.0, 100.0]));

        histogram.record(value);

        // Record metric
        let metric = Metric::new_histogram(
            name.to_string(),
            histogram.buckets.clone(),
            histogram.counts.clone(),
            histogram.sum,
            histogram.count,
            labels,
        );
        tokio::spawn(async move {
            let mut metrics = GLOBAL_METRICS.write().await;
            metrics.push(metric);
        });
    }

    /// Get all current metrics
    pub async fn get_metrics(&self) -> Vec<Metric> {
        self.metrics.read().await.clone()
    }

    /// Export metrics in Prometheus format
    pub fn export_prometheus(&self) -> String {
        let mut output = String::new();

        // Export counters
        let counters = self.counters.read().unwrap();
        for (name, counter) in counters.iter() {
            output.push_str(&format!("# TYPE {} counter\n", name));
            output.push_str(&format!("{} {}\n", name, counter.load(Ordering::Relaxed)));
        }

        // Export gauges
        let gauges = self.gauges.read().unwrap();
        for (name, gauge) in gauges.iter() {
            output.push_str(&format!("# TYPE {} gauge\n", name));
            let value = gauge.load(Ordering::Relaxed) as f64 / 1000.0;
            output.push_str(&format!("{} {}\n", name, value));
        }

        output
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Histogram implementation
#[derive(Debug, Clone)]
struct Histogram {
    buckets: Vec<f64>,
    counts: Vec<u64>,
    sum: f64,
    count: u64,
}

impl Histogram {
    fn new(buckets: Vec<f64>) -> Self {
        let len = buckets.len();
        Self {
            buckets,
            counts: vec![0; len + 1],
            sum: 0.0,
            count: 0,
        }
    }

    fn record(&mut self, value: f64) {
        self.sum += value;
        self.count += 1;

        let len = self.buckets.len();
        for i in 0..len {
            if value <= self.buckets[i] {
                self.counts[i] += 1;
            }
        }
        // Count values above all buckets
        self.counts[len] += 1;
    }
}

/// Global metrics storage
static GLOBAL_METRICS: once_cell::sync::Lazy<TokioRwLock<Vec<Metric>>> =
    once_cell::sync::Lazy::new(|| TokioRwLock::new(Vec::new()));

/// Get global metrics
pub async fn get_global_metrics() -> Vec<Metric> {
    GLOBAL_METRICS.read().await.clone()
}
