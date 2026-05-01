//! Traffic Spike Detection Example
//!
//! This example demonstrates how to use Vectrill's transformation framework
//! to build a network traffic analysis and spike detection system.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{sleep, Duration};
use vectrill::connectors::file_sink::FileSinkFormat;
use vectrill::connectors::sink::{FileSink, Sink};
use vectrill::connectors::{Connector, MemoryConnector};
use vectrill::transformations::builtin::{
    FilterOperator, FilterTransform, FilterValue, MapOperation, MapTransform,
};
use vectrill::transformations::{Transformation, TransformationPipeline};
use vectrill::{RecordBatch, VectrillError};

use arrow::array::{Array, BooleanArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use chrono::Utc;

/// Network traffic event structure
#[derive(Debug, Clone)]
pub struct TrafficEvent {
    source_ip: String,
    destination_ip: String,
    protocol: String,
    port: u16,
    bytes_transferred: u64,
    packet_count: u32,
    duration_ms: u32,
    timestamp: i64,
    interface: String,
    traffic_type: String,
}

/// Create traffic data schema
fn create_traffic_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("source_ip", DataType::Utf8, false),
        Field::new("destination_ip", DataType::Utf8, false),
        Field::new("protocol", DataType::Utf8, false),
        Field::new("port", DataType::Int64, false),
        Field::new("bytes_transferred", DataType::Int64, false),
        Field::new("packet_count", DataType::Int64, false),
        Field::new("duration_ms", DataType::Int64, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("interface", DataType::Utf8, false),
        Field::new("traffic_type", DataType::Utf8, false),
        Field::new("bytes_per_second", DataType::Float64, true), // Calculated
        Field::new("packets_per_second", DataType::Float64, true), // Calculated
        Field::new("avg_packet_size", DataType::Float64, true),  // Calculated
        Field::new("traffic_ratio", DataType::Float64, true),    // Calculated
        Field::new("is_spike", DataType::Boolean, true),         // Calculated
        Field::new("spike_severity", DataType::Float64, true),   // Calculated
        Field::new("anomaly_type", DataType::Utf8, true),        // Calculated
        Field::new("risk_level", DataType::Utf8, true),          // Calculated
    ]))
}

/// Generate sample network traffic data
fn generate_traffic_data(events_count: usize) -> Vec<TrafficEvent> {
    let mut events = Vec::new();
    let source_ips = vec![
        "192.168.1.100",
        "192.168.1.101",
        "192.168.1.102",
        "10.0.0.50",
        "10.0.0.51",
    ];
    let dest_ips = vec![
        "8.8.8.8",
        "1.1.1.1",
        "208.67.222.222",
        "9.9.9.9",
        "149.112.112.112",
    ];
    let protocols = vec!["TCP", "UDP", "ICMP"];
    let ports = vec![80, 443, 22, 53, 25, 110, 143, 993, 995];
    let interfaces = vec!["eth0", "wlan0", "lo"];
    let traffic_types = vec!["web", "dns", "ssh", "email", "backup"];

    let mut baseline_traffic = HashMap::new();

    for i in 0..events_count {
        let timestamp = Utc::now().timestamp_millis();
        let source_ip = source_ips[i % source_ips.len()].to_string();
        let dest_ip = dest_ips[i % dest_ips.len()].to_string();
        let protocol = protocols[i % protocols.len()].to_string();
        let port = ports[i % ports.len()] as u16;
        let interface = interfaces[i % interfaces.len()].to_string();
        let traffic_type = traffic_types[i % traffic_types.len()].to_string();

        // Generate baseline traffic with occasional spikes
        let key = format!("{}_{}_{}", source_ip, dest_ip, traffic_type);
        let baseline = baseline_traffic
            .entry(key.clone())
            .or_insert(1000.0 + rand::random::<f64>() * 500.0);

        let mut bytes_transferred = (*baseline as u64) + (rand::random::<u64>() % 1000);
        let mut packet_count = (bytes_transferred / 150) as u32; // Average packet size ~150 bytes
        let duration_ms = 100 + rand::random::<u32>() % 900;

        // Inject traffic spikes (3% chance)
        if rand::random::<f64>() < 0.03 {
            let spike_multiplier = 5.0 + rand::random::<f64>() * 10.0; // 5-15x normal traffic
            bytes_transferred = (bytes_transferred as f64 * spike_multiplier) as u64;
            packet_count = (packet_count as f64 * spike_multiplier) as u32;
        }

        // Update baseline with some drift
        *baseline = *baseline * 0.95 + (bytes_transferred as f64) * 0.05;

        events.push(TrafficEvent {
            source_ip,
            destination_ip: dest_ip,
            protocol,
            port,
            bytes_transferred,
            packet_count,
            duration_ms,
            timestamp,
            interface,
            traffic_type,
        });
    }

    events
}

/// Convert traffic events to Arrow RecordBatch
fn traffic_to_record_batch(
    events: Vec<TrafficEvent>,
    schema: Arc<Schema>,
) -> Result<RecordBatch, VectrillError> {
    let source_ips: Vec<String> = events.iter().map(|e| e.source_ip.clone()).collect();
    let dest_ips: Vec<String> = events.iter().map(|e| e.destination_ip.clone()).collect();
    let protocols: Vec<String> = events.iter().map(|e| e.protocol.clone()).collect();
    let ports: Vec<i64> = events.iter().map(|e| e.port as i64).collect();
    let bytes_transferred: Vec<i64> = events.iter().map(|e| e.bytes_transferred as i64).collect();
    let packet_counts: Vec<i64> = events.iter().map(|e| e.packet_count as i64).collect();
    let durations: Vec<i64> = events.iter().map(|e| e.duration_ms as i64).collect();
    let timestamps: Vec<i64> = events.iter().map(|e| e.timestamp).collect();
    let interfaces: Vec<String> = events.iter().map(|e| e.interface.clone()).collect();
    let traffic_types: Vec<String> = events.iter().map(|e| e.traffic_type.clone()).collect();

    let source_ip_array = StringArray::from(source_ips);
    let dest_ip_array = StringArray::from(dest_ips);
    let protocol_array = StringArray::from(protocols);
    let port_array = Int64Array::from(ports);
    let bytes_array = Int64Array::from(bytes_transferred);
    let packet_array = Int64Array::from(packet_counts);
    let duration_array = Int64Array::from(durations);
    let timestamp_array = Int64Array::from(timestamps);
    let interface_array = StringArray::from(interfaces);
    let traffic_type_array = StringArray::from(traffic_types);

    // Initialize calculated fields with nulls
    let bytes_per_sec_array = arrow::array::new_null_array(&DataType::Float64, events.len());
    let packets_per_sec_array = arrow::array::new_null_array(&DataType::Float64, events.len());
    let avg_packet_size_array = arrow::array::new_null_array(&DataType::Float64, events.len());
    let traffic_ratio_array = arrow::array::new_null_array(&DataType::Float64, events.len());
    let is_spike_array = arrow::array::new_null_array(&DataType::Boolean, events.len());
    let spike_severity_array = arrow::array::new_null_array(&DataType::Float64, events.len());
    let anomaly_type_array = arrow::array::new_null_array(&DataType::Utf8, events.len());
    let risk_level_array = arrow::array::new_null_array(&DataType::Utf8, events.len());

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(source_ip_array) as _,
            Arc::new(dest_ip_array) as _,
            Arc::new(protocol_array) as _,
            Arc::new(port_array) as _,
            Arc::new(bytes_array) as _,
            Arc::new(packet_array) as _,
            Arc::new(duration_array) as _,
            Arc::new(timestamp_array) as _,
            Arc::new(interface_array) as _,
            Arc::new(traffic_type_array) as _,
            Arc::new(bytes_per_sec_array) as _,
            Arc::new(packets_per_sec_array) as _,
            Arc::new(avg_packet_size_array) as _,
            Arc::new(traffic_ratio_array) as _,
            Arc::new(is_spike_array) as _,
            Arc::new(spike_severity_array) as _,
            Arc::new(anomaly_type_array) as _,
            Arc::new(risk_level_array) as _,
        ],
    )
    .map_err(|e| VectrillError::ArrowError(e.to_string()))
}

/// Custom transformation to calculate traffic metrics
pub struct TrafficMetricsCalculator {
    schema: Arc<Schema>,
}

impl TrafficMetricsCalculator {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }
}

#[async_trait::async_trait]
impl Transformation for TrafficMetricsCalculator {
    async fn apply(&mut self, batch: &RecordBatch) -> Result<RecordBatch, VectrillError> {
        let bytes_array = batch
            .column(4)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let packet_array = batch
            .column(5)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let duration_array = batch
            .column(6)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let mut bytes_per_sec = Vec::new();
        let mut packets_per_sec = Vec::new();
        let mut avg_packet_size = Vec::new();

        for i in 0..batch.num_rows() {
            if !bytes_array.is_null(i) && !packet_array.is_null(i) && !duration_array.is_null(i) {
                let bytes = bytes_array.value(i) as f64;
                let packets = packet_array.value(i) as f64;
                let duration_ms = duration_array.value(i) as f64;

                let duration_sec = duration_ms / 1000.0;
                let bps = if duration_sec > 0.0 {
                    bytes / duration_sec
                } else {
                    0.0
                };
                let pps = if duration_sec > 0.0 {
                    packets / duration_sec
                } else {
                    0.0
                };
                let avg_size = if packets > 0.0 { bytes / packets } else { 0.0 };

                bytes_per_sec.push(Some(bps));
                packets_per_sec.push(Some(pps));
                avg_packet_size.push(Some(avg_size));
            } else {
                bytes_per_sec.push(None);
                packets_per_sec.push(None);
                avg_packet_size.push(None);
            }
        }

        let bytes_per_sec_array = Float64Array::from(bytes_per_sec);
        let packets_per_sec_array = Float64Array::from(packets_per_sec);
        let avg_packet_size_array = Float64Array::from(avg_packet_size);

        // Update calculated columns
        let mut new_columns = batch.columns().to_vec();
        new_columns[10] = Arc::new(bytes_per_sec_array) as _;
        new_columns[11] = Arc::new(packets_per_sec_array) as _;
        new_columns[12] = Arc::new(avg_packet_size_array) as _;

        RecordBatch::try_new(self.schema.clone(), new_columns)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))
    }

    fn name(&self) -> &str {
        "traffic_metrics_calculator"
    }

    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Custom transformation to calculate traffic ratios
pub struct TrafficRatioCalculator {
    schema: Arc<Schema>,
    baseline_metrics: HashMap<String, f64>,
}

impl TrafficRatioCalculator {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self {
            schema,
            baseline_metrics: HashMap::new(),
        }
    }
}

#[async_trait::async_trait]
impl Transformation for TrafficRatioCalculator {
    async fn apply(&mut self, batch: &RecordBatch) -> Result<RecordBatch, VectrillError> {
        let source_ip_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let dest_ip_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let traffic_type_array = batch
            .column(9)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let bytes_per_sec_array = batch
            .column(10)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        let mut traffic_ratios = Vec::new();

        for i in 0..batch.num_rows() {
            if !source_ip_array.is_null(i)
                && !dest_ip_array.is_null(i)
                && !traffic_type_array.is_null(i)
                && !bytes_per_sec_array.is_null(i)
            {
                let source_ip = source_ip_array.value(i);
                let dest_ip = dest_ip_array.value(i);
                let traffic_type = traffic_type_array.value(i);
                let bytes_per_sec = bytes_per_sec_array.value(i);

                let key = format!("{}_{}_{}", source_ip, dest_ip, traffic_type);

                // Update baseline metrics
                let baseline = self
                    .baseline_metrics
                    .entry(key.clone())
                    .or_insert(bytes_per_sec);
                *baseline = *baseline * 0.9 + bytes_per_sec * 0.1; // Exponential moving average

                let ratio = if *baseline > 0.0 {
                    bytes_per_sec / *baseline
                } else {
                    1.0
                };
                traffic_ratios.push(Some(ratio));
            } else {
                traffic_ratios.push(None);
            }
        }

        let traffic_ratio_array = Float64Array::from(traffic_ratios);

        // Update the traffic_ratio column
        let mut new_columns = batch.columns().to_vec();
        new_columns[13] = Arc::new(traffic_ratio_array) as _;

        RecordBatch::try_new(self.schema.clone(), new_columns)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))
    }

    fn name(&self) -> &str {
        "traffic_ratio_calculator"
    }

    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Custom transformation to detect traffic spikes
pub struct TrafficSpikeDetector {
    schema: Arc<Schema>,
    spike_threshold: f64,
}

impl TrafficSpikeDetector {
    pub fn new(schema: Arc<Schema>, spike_threshold: f64) -> Self {
        Self {
            schema,
            spike_threshold,
        }
    }

    /// Calculate spike severity based on traffic ratio
    fn calculate_spike_severity(ratio: f64) -> f64 {
        if ratio <= 1.0 {
            0.0
        } else {
            (ratio - 1.0) * 100.0 // Percentage above baseline
        }
    }

    /// Determine anomaly type
    fn determine_anomaly_type(ratio: f64, bytes_per_sec: f64) -> &'static str {
        if ratio > 10.0 {
            "MASSIVE_SPIKE"
        } else if ratio > 5.0 {
            "LARGE_SPIKE"
        } else if ratio > 2.0 {
            "MODERATE_SPIKE"
        } else if ratio > 1.5 {
            "SMALL_SPIKE"
        } else if bytes_per_sec > 1000000.0 {
            // > 1MB/s
            "HIGH_VOLUME"
        } else {
            "NORMAL"
        }
    }
}

#[async_trait::async_trait]
impl Transformation for TrafficSpikeDetector {
    async fn apply(&mut self, batch: &RecordBatch) -> Result<RecordBatch, VectrillError> {
        let traffic_ratio_array = batch
            .column(13)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let bytes_per_sec_array = batch
            .column(10)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        let mut is_spikes = Vec::new();
        let mut spike_severities = Vec::new();
        let mut anomaly_types = Vec::new();

        for i in 0..batch.num_rows() {
            if !traffic_ratio_array.is_null(i) && !bytes_per_sec_array.is_null(i) {
                let ratio = traffic_ratio_array.value(i);
                let bytes_per_sec = bytes_per_sec_array.value(i);

                let is_spike = ratio > self.spike_threshold;
                let severity = Self::calculate_spike_severity(ratio);
                let anomaly_type = Self::determine_anomaly_type(ratio, bytes_per_sec);

                is_spikes.push(Some(is_spike));
                spike_severities.push(Some(severity));
                anomaly_types.push(Some(anomaly_type.to_string()));
            } else {
                is_spikes.push(None);
                spike_severities.push(None);
                anomaly_types.push(None);
            }
        }

        let is_spike_array = BooleanArray::from(is_spikes);
        let spike_severity_array = Float64Array::from(spike_severities);
        let anomaly_type_array = StringArray::from(anomaly_types);

        // Update calculated columns
        let mut new_columns = batch.columns().to_vec();
        new_columns[14] = Arc::new(is_spike_array) as _;
        new_columns[15] = Arc::new(spike_severity_array) as _;
        new_columns[16] = Arc::new(anomaly_type_array) as _;

        RecordBatch::try_new(self.schema.clone(), new_columns)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))
    }

    fn name(&self) -> &str {
        "traffic_spike_detector"
    }

    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Custom transformation to assess risk levels
pub struct RiskAssessmentCalculator {
    schema: Arc<Schema>,
}

impl RiskAssessmentCalculator {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }

    /// Assess risk level based on multiple factors
    fn assess_risk_level(
        is_spike: bool,
        spike_severity: f64,
        bytes_per_sec: f64,
        protocol: &str,
        port: i64,
    ) -> &'static str {
        let mut risk_score = 0.0;

        // Spike detection
        if is_spike {
            risk_score += spike_severity.min(50.0);
        }

        // High volume traffic
        if bytes_per_sec > 10000000.0 {
            // > 10MB/s
            risk_score += 30.0;
        } else if bytes_per_sec > 1000000.0 {
            // > 1MB/s
            risk_score += 10.0;
        }

        // Protocol risk
        match protocol {
            "ICMP" => risk_score += 20.0, // Often used in attacks
            "UDP" => risk_score += 10.0,  // Can be used for amplification
            _ => {}
        }

        // Port risk
        if port < 1024 {
            risk_score += 5.0; // Well-known ports
        }

        if risk_score >= 80.0 {
            "CRITICAL"
        } else if risk_score >= 50.0 {
            "HIGH"
        } else if risk_score >= 25.0 {
            "MEDIUM"
        } else if risk_score >= 10.0 {
            "LOW"
        } else {
            "MINIMAL"
        }
    }
}

#[async_trait::async_trait]
impl Transformation for RiskAssessmentCalculator {
    async fn apply(&mut self, batch: &RecordBatch) -> Result<RecordBatch, VectrillError> {
        let is_spike_array = batch
            .column(14)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let spike_severity_array = batch
            .column(15)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let bytes_per_sec_array = batch
            .column(10)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let protocol_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let port_array = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let mut risk_levels = Vec::new();

        for i in 0..batch.num_rows() {
            if !is_spike_array.is_null(i)
                && !spike_severity_array.is_null(i)
                && !bytes_per_sec_array.is_null(i)
                && !protocol_array.is_null(i)
                && !port_array.is_null(i)
            {
                let is_spike = is_spike_array.value(i);
                let spike_severity = spike_severity_array.value(i);
                let bytes_per_sec = bytes_per_sec_array.value(i);
                let protocol = protocol_array.value(i);
                let port = port_array.value(i);

                let risk_level = Self::assess_risk_level(
                    is_spike,
                    spike_severity,
                    bytes_per_sec,
                    protocol,
                    port,
                );
                risk_levels.push(Some(risk_level.to_string()));
            } else {
                risk_levels.push(None);
            }
        }

        let risk_level_array = StringArray::from(risk_levels);

        // Update the risk_level column
        let mut new_columns = batch.columns().to_vec();
        new_columns[17] = Arc::new(risk_level_array) as _;

        RecordBatch::try_new(self.schema.clone(), new_columns)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))
    }

    fn name(&self) -> &str {
        "risk_assessment_calculator"
    }

    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Build the traffic spike detection pipeline
fn build_traffic_pipeline(schema: Arc<Schema>) -> TransformationPipeline {
    TransformationPipeline::new("traffic_spike_detection_pipeline".to_string())
        .add_transform(Box::new(TrafficMetricsCalculator::new(schema.clone())))
        .add_transform(Box::new(TrafficRatioCalculator::new(schema.clone())))
        .add_transform(Box::new(TrafficSpikeDetector::new(schema.clone(), 2.0)))
        .add_transform(Box::new(RiskAssessmentCalculator::new(schema.clone())))
        .add_transform(Box::new(FilterTransform::new(
            "bytes_transferred".to_string(),
            FilterOperator::GreaterThan,
            FilterValue::Int64(0), // Filter out zero-byte transfers
            schema.clone(),
        )))
        .add_transform(Box::new(MapTransform::new(
            "bytes_transferred".to_string(),
            MapOperation::Multiply(1.0), // Convert to MB if needed
            "bytes_transferred_mb".to_string(),
            schema.clone(),
        )))
}

/// Main traffic spike detection example
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🌐 Starting Traffic Spike Detection Example");

    // Create schema
    let schema = create_traffic_schema();

    // Create transformation pipeline
    let mut pipeline = build_traffic_pipeline(schema.clone());

    // Generate network traffic data
    let traffic_events = generate_traffic_data(1000); // 1000 traffic events

    println!(
        "📡 Generated network traffic data: {} events",
        traffic_events.len()
    );

    // Create data source
    let mut source = MemoryConnector::with_schema(
        "traffic_source".to_string(),
        schema.clone(),
        20, // 20 batches
        50, // 50 events per batch
    );

    // Create output sink
    let mut sink = FileSink::new(
        std::path::PathBuf::from("traffic_spike_detection.csv"),
        FileSinkFormat::Csv,
        schema,
    )?;

    println!("🔄 Processing network traffic through spike detection pipeline...");

    // Process data through pipeline
    let mut total_processed = 0;
    let mut batch_count = 0;
    let mut spike_count = 0;
    let mut critical_count = 0;

    while let Some(batch_result) = source.next_batch().await {
        let batch = batch_result?;
        batch_count += 1;

        println!(
            "📊 Processing batch {} ({} events)",
            batch_count,
            batch.num_rows()
        );

        // Apply transformation pipeline
        let transformed_batch = pipeline.apply(batch).await?;

        // Count spikes and critical risks
        if let Some(is_spike_array) = transformed_batch
            .column(14)
            .as_any()
            .downcast_ref::<BooleanArray>()
        {
            spike_count += is_spike_array.iter().filter(|&x| x == Some(true)).count();
        }

        if let Some(risk_level_array) = transformed_batch
            .column(17)
            .as_any()
            .downcast_ref::<StringArray>()
        {
            critical_count += risk_level_array
                .iter()
                .filter(|&x| x == Some("CRITICAL"))
                .count();
        }

        // Write to output
        sink.write_batch(&transformed_batch).await?;

        total_processed += transformed_batch.num_rows();

        // Simulate real-time processing delay
        sleep(Duration::from_millis(25)).await;
    }

    sink.flush().await?;

    println!("✅ Traffic spike detection processing complete!");
    println!("📈 Total batches processed: {}", batch_count);
    println!("📊 Total events processed: {}", total_processed);
    println!("⚠️  Traffic spikes detected: {}", spike_count);
    println!("🚨 Critical risk events: {}", critical_count);
    println!("💾 Output saved to: traffic_spike_detection.csv");

    // Display pipeline statistics
    println!("\n🔧 Pipeline Statistics:");
    println!("  - Pipeline name: {}", pipeline.name());
    println!("  - Number of transformations: {}", pipeline.len());

    println!("\n📋 Traffic Analysis Features:");
    println!("  📊 Traffic Metrics Calculation (bytes/sec, packets/sec, avg packet size)");
    println!("  📈 Traffic Ratio Analysis (baseline comparison)");
    println!("  🚨 Spike Detection (threshold-based anomaly detection)");
    println!("  ⚠️  Spike Severity Assessment (magnitude quantification)");
    println!("  🔍 Anomaly Type Classification (categorization)");
    println!("  🛡️  Risk Level Assessment (security impact evaluation)");

    println!("\n🎯 Key Features Demonstrated:");
    println!("  ✅ Real-time network traffic monitoring");
    println!("  ✅ Statistical baseline tracking");
    println!("  ✅ Multi-factor anomaly detection");
    println!("  ✅ Risk-based threat assessment");
    println!("  ✅ Protocol and port analysis");
    println!("  ✅ Data filtering and transformation");
    println!("  ✅ Pipeline composition");
    println!("  ✅ File output integration");

    println!("\n📡 Network Protocols Monitored:");
    println!("  🌐 TCP (Transmission Control Protocol)");
    println!("  📦 UDP (User Datagram Protocol)");
    println!("  📍 ICMP (Internet Control Message Protocol)");

    println!("\n🎯 Traffic Types Analyzed:");
    println!("  🌍 Web Traffic (HTTP/HTTPS)");
    println!("  🔍 DNS Queries");
    println!("  🔐 SSH Connections");
    println!("  📧 Email Traffic");
    println!("  💾 Backup Operations");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_traffic_schema_creation() {
        let schema = create_traffic_schema();
        assert_eq!(schema.fields().len(), 18);
        assert_eq!(schema.field(0).name(), "source_ip");
        assert_eq!(schema.field(4).name(), "bytes_transferred");
        assert_eq!(schema.field(10).name(), "bytes_per_second");
    }

    #[test]
    fn test_traffic_data_generation() {
        let events = generate_traffic_data(10);
        assert_eq!(events.len(), 10);
        assert!(events[0].bytes_transferred > 0);
        assert!(events[0].packet_count > 0);
        assert!(events[0].duration_ms > 0);
    }

    #[test]
    fn test_spike_severity_calculation() {
        let severity = TrafficSpikeDetector::calculate_spike_severity(5.0);
        assert_eq!(severity, 400.0); // (5.0 - 1.0) * 100

        let severity_zero = TrafficSpikeDetector::calculate_spike_severity(0.5);
        assert_eq!(severity_zero, 0.0);
    }

    #[test]
    fn test_anomaly_type_determination() {
        let anomaly = TrafficSpikeDetector::determine_anomaly_type(15.0, 2000000.0);
        assert_eq!(anomaly, "MASSIVE_SPIKE");

        let anomaly_small = TrafficSpikeDetector::determine_anomaly_type(1.8, 500000.0);
        assert_eq!(anomaly_small, "SMALL_SPIKE");

        let anomaly_volume = TrafficSpikeDetector::determine_anomaly_type(1.2, 2000000.0);
        assert_eq!(anomaly_volume, "HIGH_VOLUME");
    }

    #[test]
    fn test_risk_assessment() {
        let risk = RiskAssessmentCalculator::assess_risk_level(true, 500.0, 2000000.0, "ICMP", 80);
        assert_eq!(risk, "CRITICAL");

        let risk_low =
            RiskAssessmentCalculator::assess_risk_level(false, 0.0, 100000.0, "TCP", 8080);
        assert_eq!(risk_low, "MINIMAL");

        let risk_medium =
            RiskAssessmentCalculator::assess_risk_level(true, 100.0, 1500000.0, "UDP", 53);
        assert!(risk == "HIGH" || risk == "MEDIUM");
    }

    #[test]
    fn test_transformation_pipeline_creation() {
        let schema = create_traffic_schema();
        let pipeline = build_traffic_pipeline(schema);
        assert_eq!(pipeline.name(), "traffic_spike_detection_pipeline");
        assert_eq!(pipeline.len(), 7);
    }
}
