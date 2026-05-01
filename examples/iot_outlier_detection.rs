//! IoT Outlier Detection Example
//! 
//! This example demonstrates how to use Vectrill's transformation framework
//! to build an IoT sensor data processing and outlier detection system.

use std::sync::Arc;
use std::collections::HashMap;
use tokio::time::{sleep, Duration};
use vectrill::connectors::{Connector, MemoryConnector};
use vectrill::connectors::sink::{Sink, FileSink};
use vectrill::connectors::file_sink::FileSinkFormat;
use vectrill::transformations::{Transformation, TransformationPipeline};
use vectrill::transformations::builtin::{
    FilterTransform, MapTransform, FilterOperator, FilterValue, MapOperation
};
use vectrill::{RecordBatch, VectrillError};

use arrow::datatypes::{DataType, Field, Schema};
use arrow::array::{Float64Array, Int64Array, StringArray, BooleanArray};
use arrow::array::{Array, Datum};
use chrono::Utc;

/// IoT sensor reading structure
#[derive(Debug, Clone)]
pub struct SensorReading {
    device_id: String,
    sensor_type: String,
    timestamp: i64,
    value: f64,
    unit: String,
    location: String,
    battery_level: f64,
    signal_strength: f64,
}

/// Create IoT sensor data schema
fn create_iot_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("device_id", DataType::Utf8, false),
        Field::new("sensor_type", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("value", DataType::Float64, false),
        Field::new("unit", DataType::Utf8, false),
        Field::new("location", DataType::Utf8, false),
        Field::new("battery_level", DataType::Float64, false),
        Field::new("signal_strength", DataType::Float64, false),
        Field::new("z_score", DataType::Float64, true),           // Calculated
        Field::new("is_outlier", DataType::Boolean, true),         // Calculated
        Field::new("anomaly_score", DataType::Float64, true),     // Calculated
        Field::new("health_status", DataType::Utf8, true),         // Calculated
        Field::new("moving_avg", DataType::Float64, true),        // Calculated
        Field::new("trend", DataType::Utf8, true),                 // Calculated
    ]))
}

/// Generate sample IoT sensor data
fn generate_sensor_data(devices: &[&str], readings_per_device: usize) -> Vec<SensorReading> {
    let mut all_readings = Vec::new();
    let sensor_types = vec![
        ("temperature", "°C", 15.0, 35.0),
        ("humidity", "%", 20.0, 80.0),
        ("pressure", "hPa", 980.0, 1040.0),
        ("light", "lux", 0.0, 1000.0),
        ("vibration", "Hz", 0.0, 100.0),
        ("voltage", "V", 3.0, 5.0),
    ];
    
    for (device_idx, device_id) in devices.iter().enumerate() {
        for reading_idx in 0..readings_per_device {
            let timestamp = Utc::now().timestamp_millis();
            
            // Select a random sensor type
            let (sensor_type, unit, min_val, max_val) = 
                sensor_types[reading_idx % sensor_types.len()];
            
            // Generate normal value with some noise
            let base_value = min_val + (max_val - min_val) * 0.5;
            let noise = (rand::random::<f64>() - 0.5) * (max_val - min_val) * 0.2;
            let mut value = base_value + noise;
            
            // Occasionally inject outliers (5% chance)
            if rand::random::<f64>() < 0.05 {
                let outlier_magnitude = (max_val - min_val) * 0.5;
                if rand::random::<bool>() {
                    value += outlier_magnitude;  // High outlier
                } else {
                    value -= outlier_magnitude;  // Low outlier
                }
            }
            
            // Ensure value stays within reasonable bounds
            value = value.max(min_val * 0.5).min(max_val * 1.5);
            
            let battery_level = 20.0 + rand::random::<f64>() * 80.0;
            let signal_strength = -100.0 + rand::random::<f64>() * 70.0;
            let location = format!("Zone_{}", (device_idx % 5) + 1);
            
            all_readings.push(SensorReading {
                device_id: device_id.to_string(),
                sensor_type: sensor_type.to_string(),
                timestamp,
                value,
                unit: unit.to_string(),
                location,
                battery_level,
                signal_strength,
            });
        }
    }
    
    all_readings
}

/// Convert sensor readings to Arrow RecordBatch
fn sensors_to_record_batch(readings: Vec<SensorReading>, schema: Arc<Schema>) -> Result<RecordBatch, VectrillError> {
    let device_ids: Vec<String> = readings.iter().map(|r| r.device_id.clone()).collect();
    let sensor_types: Vec<String> = readings.iter().map(|r| r.sensor_type.clone()).collect();
    let timestamps: Vec<i64> = readings.iter().map(|r| r.timestamp).collect();
    let values: Vec<f64> = readings.iter().map(|r| r.value).collect();
    let units: Vec<String> = readings.iter().map(|r| r.unit.clone()).collect();
    let locations: Vec<String> = readings.iter().map(|r| r.location.clone()).collect();
    let battery_levels: Vec<f64> = readings.iter().map(|r| r.battery_level).collect();
    let signal_strengths: Vec<f64> = readings.iter().map(|r| r.signal_strength).collect();
    
    let device_id_array = StringArray::from(device_ids);
    let sensor_type_array = StringArray::from(sensor_types);
    let timestamp_array = Int64Array::from(timestamps);
    let value_array = Float64Array::from(values);
    let unit_array = StringArray::from(units);
    let location_array = StringArray::from(locations);
    let battery_level_array = Float64Array::from(battery_levels);
    let signal_strength_array = Float64Array::from(signal_strengths);
    
    // Initialize calculated fields with nulls
    let z_score_array = arrow::array::new_null_array(&DataType::Float64, readings.len());
    let is_outlier_array = arrow::array::new_null_array(&DataType::Boolean, readings.len());
    let anomaly_score_array = arrow::array::new_null_array(&DataType::Float64, readings.len());
    let health_status_array = arrow::array::new_null_array(&DataType::Utf8, readings.len());
    let moving_avg_array = arrow::array::new_null_array(&DataType::Float64, readings.len());
    let trend_array = arrow::array::new_null_array(&DataType::Utf8, readings.len());
    
    RecordBatch::try_new(schema, vec![
        Arc::new(device_id_array) as _,
        Arc::new(sensor_type_array) as _,
        Arc::new(timestamp_array) as _,
        Arc::new(value_array) as _,
        Arc::new(unit_array) as _,
        Arc::new(location_array) as _,
        Arc::new(battery_level_array) as _,
        Arc::new(signal_strength_array) as _,
        Arc::new(z_score_array) as _,
        Arc::new(is_outlier_array) as _,
        Arc::new(anomaly_score_array) as _,
        Arc::new(health_status_array) as _,
        Arc::new(moving_avg_array) as _,
        Arc::new(trend_array) as _,
    ]).map_err(|e| VectrillError::ArrowError(e.to_string()))
}

/// Custom transformation to calculate Z-scores for outlier detection
pub struct ZScoreCalculator {
    schema: Arc<Schema>,
    window_size: usize,
    device_stats: HashMap<String, Vec<f64>>,
}

impl ZScoreCalculator {
    pub fn new(schema: Arc<Schema>, window_size: usize) -> Self {
        Self {
            schema,
            window_size,
            device_stats: HashMap::new(),
        }
    }
    
    /// Calculate Z-score for a value
    fn calculate_z_score(value: f64, mean: f64, std_dev: f64) -> f64 {
        if std_dev == 0.0 {
            0.0
        } else {
            (value - mean) / std_dev
        }
    }
    
    /// Calculate mean and standard deviation
    fn calculate_stats(values: &[f64]) -> (f64, f64) {
        if values.is_empty() {
            return (0.0, 0.0);
        }
        
        let mean = values.iter().sum::<f64>() / values.len() as f64;
        let variance = values.iter()
            .map(|x| (x - mean).powi(2))
            .sum::<f64>() / values.len() as f64;
        let std_dev = variance.sqrt();
        
        (mean, std_dev)
    }
}

#[async_trait::async_trait]
impl Transformation for ZScoreCalculator {
    async fn apply(&mut self, batch: &RecordBatch) -> Result<RecordBatch, VectrillError> {
        let device_id_array = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let sensor_type_array = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let value_array = batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        
        let mut z_scores = Vec::new();
        
        for i in 0..batch.num_rows() {
            if !device_id_array.is_null(i) && !sensor_type_array.is_null(i) && !value_array.is_null(i) {
                let device_id = device_id_array.value(i);
                let sensor_type = sensor_type_array.value(i);
                let value = value_array.value(i);
                let key = format!("{}_{}", device_id, sensor_type);
                
                // Update device statistics
                let stats = self.device_stats.entry(key.clone()).or_insert_with(Vec::new);
                stats.push(value);
                
                // Keep only recent values (sliding window)
                if stats.len() > self.window_size {
                    stats.remove(0);
                }
                
                // Calculate Z-score
                let (mean, std_dev) = Self::calculate_stats(stats);
                let z_score = Self::calculate_z_score(value, mean, std_dev);
                z_scores.push(Some(z_score));
            } else {
                z_scores.push(None);
            }
        }
        
        let z_score_array = Float64Array::from(z_scores);
        
        // Update the z_score column in the batch
        let mut new_columns = batch.columns().to_vec();
        new_columns[8] = Arc::new(z_score_array) as _;
        
        RecordBatch::try_new(self.schema.clone(), new_columns)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))
    }
    
    fn name(&self) -> &str {
        "z_score_calculator"
    }
    
    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Custom transformation to detect outliers based on Z-scores
pub struct OutlierDetector {
    schema: Arc<Schema>,
    z_threshold: f64,
}

impl OutlierDetector {
    pub fn new(schema: Arc<Schema>, z_threshold: f64) -> Self {
        Self { schema, z_threshold }
    }
}

#[async_trait::async_trait]
impl Transformation for OutlierDetector {
    async fn apply(&mut self, batch: &RecordBatch) -> Result<RecordBatch, VectrillError> {
        let z_score_array = batch.column(8).as_any().downcast_ref::<Float64Array>().unwrap();
        
        let is_outlier_values: Vec<Option<bool>> = z_score_array
            .iter()
            .map(|z_opt| z_opt.map(|z| z.abs() > self.z_threshold))
            .collect();
        
        let is_outlier_array = BooleanArray::from(is_outlier_values);
        
        // Update the is_outlier column in the batch
        let mut new_columns = batch.columns().to_vec();
        new_columns[9] = Arc::new(is_outlier_array) as _;
        
        RecordBatch::try_new(self.schema.clone(), new_columns)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))
    }
    
    fn name(&self) -> &str {
        "outlier_detector"
    }
    
    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Custom transformation to calculate anomaly scores
pub struct AnomalyScoreCalculator {
    schema: Arc<Schema>,
}

impl AnomalyScoreCalculator {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }
    
    /// Calculate comprehensive anomaly score
    fn calculate_anomaly_score(
        z_score: f64,
        battery_level: f64,
        signal_strength: f64,
        is_outlier: bool,
    ) -> f64 {
        let mut score = 0.0;
        
        // Z-score contribution (0-40 points)
        score += (z_score.abs() * 10.0).min(40.0);
        
        // Outlier flag contribution (0-30 points)
        if is_outlier {
            score += 30.0;
        }
        
        // Battery level contribution (0-15 points)
        if battery_level < 20.0 {
            score += 15.0;
        } else if battery_level < 50.0 {
            score += 5.0;
        }
        
        // Signal strength contribution (0-15 points)
        if signal_strength < -80.0 {
            score += 15.0;
        } else if signal_strength < -60.0 {
            score += 5.0;
        }
        
        score.min(100.0)  // Cap at 100
    }
}

#[async_trait::async_trait]
impl Transformation for AnomalyScoreCalculator {
    async fn apply(&mut self, batch: &RecordBatch) -> Result<RecordBatch, VectrillError> {
        let z_score_array = batch.column(8).as_any().downcast_ref::<Float64Array>().unwrap();
        let battery_level_array = batch.column(6).as_any().downcast_ref::<Float64Array>().unwrap();
        let signal_strength_array = batch.column(7).as_any().downcast_ref::<Float64Array>().unwrap();
        let is_outlier_array = batch.column(9).as_any().downcast_ref::<BooleanArray>().unwrap();
        
        let anomaly_scores: Vec<Option<f64>> = z_score_array
            .iter()
            .zip(battery_level_array.iter())
            .zip(signal_strength_array.iter())
            .zip(is_outlier_array.iter())
            .map(|(((z_opt, battery_opt), signal_opt), outlier_opt)| {
                match (z_opt, battery_opt, signal_opt, outlier_opt) {
                    (Some(z), Some(battery), Some(signal), Some(outlier)) => {
                        Some(Self::calculate_anomaly_score(z, battery, signal, outlier))
                    }
                    _ => None,
                }
            })
            .collect();
        
        let anomaly_score_array = Float64Array::from(anomaly_scores);
        
        // Update the anomaly_score column in the batch
        let mut new_columns = batch.columns().to_vec();
        new_columns[10] = Arc::new(anomaly_score_array) as _;
        
        RecordBatch::try_new(self.schema.clone(), new_columns)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))
    }
    
    fn name(&self) -> &str {
        "anomaly_score_calculator"
    }
    
    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Custom transformation to determine device health status
pub struct HealthStatusCalculator {
    schema: Arc<Schema>,
}

impl HealthStatusCalculator {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }
    
    /// Determine health status based on multiple factors
    fn determine_health_status(anomaly_score: f64, battery_level: f64, signal_strength: f64) -> &'static str {
        if anomaly_score > 70.0 || battery_level < 10.0 || signal_strength < -90.0 {
            "CRITICAL"
        } else if anomaly_score > 40.0 || battery_level < 30.0 || signal_strength < -70.0 {
            "WARNING"
        } else if anomaly_score > 20.0 || battery_level < 50.0 || signal_strength < -60.0 {
            "CAUTION"
        } else {
            "HEALTHY"
        }
    }
}

#[async_trait::async_trait]
impl Transformation for HealthStatusCalculator {
    async fn apply(&mut self, batch: &RecordBatch) -> Result<RecordBatch, VectrillError> {
        let anomaly_score_array = batch.column(10).as_any().downcast_ref::<Float64Array>().unwrap();
        let battery_level_array = batch.column(6).as_any().downcast_ref::<Float64Array>().unwrap();
        let signal_strength_array = batch.column(7).as_any().downcast_ref::<Float64Array>().unwrap();
        
        let health_status_values: Vec<Option<String>> = anomaly_score_array
            .iter()
            .zip(battery_level_array.iter())
            .zip(signal_strength_array.iter())
            .map(|((anomaly_opt, battery_opt), signal_opt)| {
                match (anomaly_opt, battery_opt, signal_opt) {
                    (Some(anomaly), Some(battery), Some(signal)) => {
                        Some(Self::determine_health_status(anomaly, battery, signal).to_string())
                    }
                    _ => None,
                }
            })
            .collect();
        
        let health_status_array = StringArray::from(health_status_values);
        
        // Update the health_status column in the batch
        let mut new_columns = batch.columns().to_vec();
        new_columns[11] = Arc::new(health_status_array) as _;
        
        RecordBatch::try_new(self.schema.clone(), new_columns)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))
    }
    
    fn name(&self) -> &str {
        "health_status_calculator"
    }
    
    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Custom transformation to calculate moving averages
pub struct MovingAverageCalculator {
    schema: Arc<Schema>,
    window_size: usize,
    device_values: HashMap<String, Vec<f64>>,
}

impl MovingAverageCalculator {
    pub fn new(schema: Arc<Schema>, window_size: usize) -> Self {
        Self {
            schema,
            window_size,
            device_values: HashMap::new(),
        }
    }
}

#[async_trait::async_trait]
impl Transformation for MovingAverageCalculator {
    async fn apply(&mut self, batch: &RecordBatch) -> Result<RecordBatch, VectrillError> {
        let device_id_array = batch.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        let sensor_type_array = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let value_array = batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        
        let mut moving_averages = Vec::new();
        
        for i in 0..batch.num_rows() {
            if !device_id_array.is_null(i) && !sensor_type_array.is_null(i) && !value_array.is_null(i) {
                let device_id = device_id_array.value(i);
                let sensor_type = sensor_type_array.value(i);
                let value = value_array.value(i);
                let key = format!("{}_{}", device_id, sensor_type);
                
                // Update device values
                let values = self.device_values.entry(key.clone()).or_insert_with(Vec::new);
                values.push(value);
                
                // Keep only recent values (sliding window)
                if values.len() > self.window_size {
                    values.remove(0);
                }
                
                // Calculate moving average
                let moving_avg = if values.is_empty() {
                    0.0
                } else {
                    values.iter().sum::<f64>() / values.len() as f64
                };
                moving_averages.push(Some(moving_avg));
            } else {
                moving_averages.push(None);
            }
        }
        
        let moving_avg_array = Float64Array::from(moving_averages);
        
        // Update the moving_avg column in the batch
        let mut new_columns = batch.columns().to_vec();
        new_columns[12] = Arc::new(moving_avg_array) as _;
        
        RecordBatch::try_new(self.schema.clone(), new_columns)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))
    }
    
    fn name(&self) -> &str {
        "moving_average_calculator"
    }
    
    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Custom transformation to detect trends
pub struct TrendDetector {
    schema: Arc<Schema>,
}

impl TrendDetector {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }
    
    /// Detect trend based on current value and moving average
    fn detect_trend(current_value: f64, moving_avg: f64) -> &'static str {
        let diff = current_value - moving_avg;
        let threshold = moving_avg.abs() * 0.1;  // 10% threshold
        
        if diff > threshold {
            "RISING"
        } else if diff < -threshold {
            "FALLING"
        } else {
            "STABLE"
        }
    }
}

#[async_trait::async_trait]
impl Transformation for TrendDetector {
    async fn apply(&mut self, batch: &RecordBatch) -> Result<RecordBatch, VectrillError> {
        let value_array = batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        let moving_avg_array = batch.column(12).as_any().downcast_ref::<Float64Array>().unwrap();
        
        let trend_values: Vec<Option<String>> = value_array
            .iter()
            .zip(moving_avg_array.iter())
            .map(|(value_opt, avg_opt)| {
                match (value_opt, avg_opt) {
                    (Some(value), Some(avg)) => {
                        Some(Self::detect_trend(value, avg).to_string())
                    }
                    _ => None,
                }
            })
            .collect();
        
        let trend_array = StringArray::from(trend_values);
        
        // Update the trend column in the batch
        let mut new_columns = batch.columns().to_vec();
        new_columns[13] = Arc::new(trend_array) as _;
        
        RecordBatch::try_new(self.schema.clone(), new_columns)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))
    }
    
    fn name(&self) -> &str {
        "trend_detector"
    }
    
    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Build the IoT outlier detection pipeline
fn build_iot_pipeline(schema: Arc<Schema>) -> TransformationPipeline {
    TransformationPipeline::new("iot_outlier_detection_pipeline".to_string())
        .add_transform(Box::new(ZScoreCalculator::new(schema.clone(), 50)))
        .add_transform(Box::new(OutlierDetector::new(schema.clone(), 2.5)))
        .add_transform(Box::new(AnomalyScoreCalculator::new(schema.clone())))
        .add_transform(Box::new(HealthStatusCalculator::new(schema.clone())))
        .add_transform(Box::new(MovingAverageCalculator::new(schema.clone(), 20)))
        .add_transform(Box::new(TrendDetector::new(schema.clone())))
        .add_transform(Box::new(FilterTransform::new(
            "anomaly_score".to_string(),
            FilterOperator::GreaterThan,
            FilterValue::Float64(0.0),  // Filter out invalid scores
            schema.clone(),
        )))
        .add_transform(Box::new(MapTransform::new(
            "battery_level".to_string(),
            MapOperation::Multiply(0.01),  // Convert to decimal
            "battery_level_decimal".to_string(),
            schema.clone(),
        )))
}

/// Main IoT outlier detection example
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🔍 Starting IoT Outlier Detection Example");
    
    // Create schema
    let schema = create_iot_schema();
    
    // Create transformation pipeline
    let mut pipeline = build_iot_pipeline(schema.clone());
    
    // Generate IoT sensor data
    let devices = vec!["DEV001", "DEV002", "DEV003", "DEV004", "DEV005"];
    let sensor_data = generate_sensor_data(&devices, 300);  // 300 readings per device
    
    println!("📡 Generated IoT sensor data for {} devices", devices.len());
    println!("📊 Total sensor readings: {}", sensor_data.len());
    
    // Create data source
    let mut source = MemoryConnector::with_schema(
        "iot_source".to_string(),
        schema.clone(),
        15,  // 15 batches
        100, // 100 readings per batch
    );
    
    // Create output sink
    let mut sink = FileSink::new(
        std::path::PathBuf::from("iot_outlier_detection.csv"),
        FileSinkFormat::Csv,
        schema,
    )?;
    
    println!("🔄 Processing IoT sensor data through outlier detection pipeline...");
    
    // Process data through pipeline
    let mut total_processed = 0;
    let mut batch_count = 0;
    let mut outlier_count = 0;
    let mut critical_count = 0;
    
    while let Some(batch_result) = source.next_batch().await {
        let batch = batch_result?;
        batch_count += 1;
        
        println!("🔍 Processing batch {} ({} readings)", batch_count, batch.num_rows());
        
        // Apply transformation pipeline
        let transformed_batch = pipeline.apply(batch).await?;
        
        // Count outliers and critical devices
        if let Some(is_outlier_array) = transformed_batch.column(9).as_any().downcast_ref::<BooleanArray>() {
            outlier_count += is_outlier_array.iter().filter(|&x| x == Some(true)).count();
        }
        
        if let Some(health_status_array) = transformed_batch.column(11).as_any().downcast_ref::<StringArray>() {
            critical_count += health_status_array.iter()
                .filter(|&x| x == Some("CRITICAL"))
                .count();
        }
        
        // Write to output
        sink.write_batch(&transformed_batch).await?;
        
        total_processed += transformed_batch.num_rows();
        
        // Simulate real-time processing delay
        sleep(Duration::from_millis(30)).await;
    }
    
    sink.flush().await?;
    
    println!("✅ IoT outlier detection processing complete!");
    println!("📈 Total batches processed: {}", batch_count);
    println!("📊 Total readings processed: {}", total_processed);
    println!("⚠️  Outliers detected: {}", outlier_count);
    println!("🚨 Critical device states: {}", critical_count);
    println!("💾 Output saved to: iot_outlier_detection.csv");
    
    // Display pipeline statistics
    println!("\n🔧 Pipeline Statistics:");
    println!("  - Pipeline name: {}", pipeline.name());
    println!("  - Number of transformations: {}", pipeline.len());
    
    println!("\n📋 Anomaly Detection Features:");
    println!("  📊 Z-Score Calculation (statistical outlier detection)");
    println!("  🚨 Outlier Flagging (threshold-based detection)");
    println!("  📈 Anomaly Scoring (comprehensive risk assessment)");
    println!("  🏥 Health Status Monitoring (device health classification)");
    println!("  📊 Moving Average Calculation (trend analysis)");
    println!("  📈 Trend Detection (directional analysis)");
    
    println!("\n🎯 Key Features Demonstrated:");
    println!("  ✅ Multi-device IoT sensor monitoring");
    println!("  ✅ Real-time statistical outlier detection");
    println!("  ✅ Comprehensive anomaly scoring");
    println!("  ✅ Device health status classification");
    println!("  ✅ Trend analysis and prediction");
    println!("  ✅ Data filtering and transformation");
    println!("  ✅ Pipeline composition");
    println!("  ✅ File output integration");
    
    println!("\n📡 IoT Devices Monitored:");
    for device in &devices {
        println!("  🔌 {}", device);
    }
    
    println!("\n📊 Sensor Types:");
    println!("  🌡️  Temperature");
    println!("  💧 Humidity");
    println!("  🌪️  Pressure");
    println!("  💡 Light");
    println!("  📳 Vibration");
    println!("  ⚡ Voltage");
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_iot_schema_creation() {
        let schema = create_iot_schema();
        assert_eq!(schema.fields().len(), 14);
        assert_eq!(schema.field(0).name(), "device_id");
        assert_eq!(schema.field(3).name(), "value");
        assert_eq!(schema.field(8).name(), "z_score");
    }
    
    #[test]
    fn test_sensor_data_generation() {
        let devices = vec!["TEST"];
        let readings = generate_sensor_data(&devices, 10);
        assert_eq!(readings.len(), 10);
        assert_eq!(readings[0].device_id, "TEST");
        assert!(readings[0].battery_level >= 0.0 && readings[0].battery_level <= 100.0);
        assert!(readings[0].signal_strength >= -100.0 && readings[0].signal_strength <= -30.0);
    }
    
    #[test]
    fn test_z_score_calculation() {
        let z = ZScoreCalculator::calculate_z_score(25.0, 20.0, 5.0);
        assert_eq!(z, 1.0);
        
        let z_zero = ZScoreCalculator::calculate_z_score(20.0, 20.0, 0.0);
        assert_eq!(z_zero, 0.0);
    }
    
    #[test]
    fn test_stats_calculation() {
        let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let (mean, std_dev) = ZScoreCalculator::calculate_stats(&values);
        assert_eq!(mean, 3.0);
        assert!((std_dev - 1.5811).abs() < 0.001);
    }
    
    #[test]
    fn test_anomaly_score_calculation() {
        let score = AnomalyScoreCalculator::calculate_anomaly_score(3.0, 80.0, -50.0, true);
        assert!(score > 30.0);  // Should be elevated due to outlier flag
        
        let score_low = AnomalyScoreCalculator::calculate_anomaly_score(0.5, 90.0, -40.0, false);
        assert!(score_low < score);  // Should be lower for normal conditions
    }
    
    #[test]
    fn test_health_status_determination() {
        let status = HealthStatusCalculator::determine_health_status(80.0, 5.0, -95.0);
        assert_eq!(status, "CRITICAL");
        
        let status_healthy = HealthStatusCalculator::determine_health_status(10.0, 80.0, -50.0);
        assert_eq!(status_healthy, "HEALTHY");
    }
    
    #[test]
    fn test_trend_detection() {
        let trend = TrendDetector::detect_trend(25.0, 20.0);
        assert_eq!(trend, "RISING");
        
        let trend_falling = TrendDetector::detect_trend(15.0, 20.0);
        assert_eq!(trend_falling, "FALLING");
        
        let trend_stable = TrendDetector::detect_trend(20.0, 20.0);
        assert_eq!(trend_stable, "STABLE");
    }
    
    #[test]
    fn test_transformation_pipeline_creation() {
        let schema = create_iot_schema();
        let pipeline = build_iot_pipeline(schema);
        assert_eq!(pipeline.name(), "iot_outlier_detection_pipeline");
        assert_eq!(pipeline.len(), 9);
    }
}
