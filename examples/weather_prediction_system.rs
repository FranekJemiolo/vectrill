//! Weather Prediction System Example
//! 
//! This example demonstrates how to use Vectrill's transformation framework
//! to build a weather data processing and prediction system.

use std::sync::Arc;
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
use chrono::Utc;

/// Weather station data structure
#[derive(Debug, Clone)]
pub struct WeatherReading {
    station_id: String,
    timestamp: i64,
    temperature: f64,      // Celsius
    humidity: f64,         // Percentage
    pressure: f64,         // hPa
    wind_speed: f64,       // km/h
    wind_direction: f64,   // Degrees
    precipitation: f64,    // mm
    visibility: f64,       // km
    cloud_cover: f64,     // Percentage
}

/// Create weather data schema
fn create_weather_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("station_id", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
        Field::new("temperature", DataType::Float64, false),
        Field::new("humidity", DataType::Float64, false),
        Field::new("pressure", DataType::Float64, false),
        Field::new("wind_speed", DataType::Float64, false),
        Field::new("wind_direction", DataType::Float64, false),
        Field::new("precipitation", DataType::Float64, false),
        Field::new("visibility", DataType::Float64, false),
        Field::new("cloud_cover", DataType::Float64, false),
        Field::new("heat_index", DataType::Float64, true),        // Calculated
        Field::new("wind_chill", DataType::Float64, true),        // Calculated
        Field::new("dew_point", DataType::Float64, true),         // Calculated
        Field::new("comfort_index", DataType::Float64, true),     // Calculated
        Field::new("storm_risk", DataType::Float64, true),         // Calculated
        Field::new("is_extreme", DataType::Boolean, true),         // Calculated
    ]))
}

/// Generate sample weather data for multiple stations
fn generate_weather_data(stations: &[&str], readings_per_station: usize) -> Vec<WeatherReading> {
    let mut all_readings = Vec::new();
    
    for (station_idx, station_id) in stations.iter().enumerate() {
        let mut base_temp = 20.0 + (station_idx as f64 * 5.0);  // Different base temps
        
        for _i in 0..readings_per_station {
            let timestamp = Utc::now().timestamp_millis();
            
            // Simulate realistic weather variations
            let temp_variation = (rand::random::<f64>() - 0.5) * 10.0;
            base_temp += temp_variation * 0.1;  // Slow drift
            
            let temperature = base_temp + (rand::random::<f64>() - 0.5) * 5.0;
            let humidity = 30.0 + rand::random::<f64>() * 60.0;
            let pressure = 980.0 + rand::random::<f64>() * 40.0;
            let wind_speed = rand::random::<f64>() * 50.0;
            let wind_direction = rand::random::<f64>() * 360.0;
            let precipitation = if rand::random::<f64>() < 0.3 {
                rand::random::<f64>() * 10.0
            } else {
                0.0
            };
            let visibility = 1.0 + rand::random::<f64>() * 19.0;
            let cloud_cover = rand::random::<f64>() * 100.0;
            
            all_readings.push(WeatherReading {
                station_id: station_id.to_string(),
                timestamp,
                temperature,
                humidity,
                pressure,
                wind_speed,
                wind_direction,
                precipitation,
                visibility,
                cloud_cover,
            });
        }
    }
    
    all_readings
}

/// Convert weather readings to Arrow RecordBatch
fn weather_to_record_batch(readings: Vec<WeatherReading>, schema: Arc<Schema>) -> Result<RecordBatch, VectrillError> {
    let station_ids: Vec<String> = readings.iter().map(|r| r.station_id.clone()).collect();
    let timestamps: Vec<i64> = readings.iter().map(|r| r.timestamp).collect();
    let temperatures: Vec<f64> = readings.iter().map(|r| r.temperature).collect();
    let humidities: Vec<f64> = readings.iter().map(|r| r.humidity).collect();
    let pressures: Vec<f64> = readings.iter().map(|r| r.pressure).collect();
    let wind_speeds: Vec<f64> = readings.iter().map(|r| r.wind_speed).collect();
    let wind_directions: Vec<f64> = readings.iter().map(|r| r.wind_direction).collect();
    let precipitations: Vec<f64> = readings.iter().map(|r| r.precipitation).collect();
    let visibilities: Vec<f64> = readings.iter().map(|r| r.visibility).collect();
    let cloud_covers: Vec<f64> = readings.iter().map(|r| r.cloud_cover).collect();
    
    let station_id_array = StringArray::from(station_ids);
    let timestamp_array = Int64Array::from(timestamps);
    let temperature_array = Float64Array::from(temperatures);
    let humidity_array = Float64Array::from(humidities);
    let pressure_array = Float64Array::from(pressures);
    let wind_speed_array = Float64Array::from(wind_speeds);
    let wind_direction_array = Float64Array::from(wind_directions);
    let precipitation_array = Float64Array::from(precipitations);
    let visibility_array = Float64Array::from(visibilities);
    let cloud_cover_array = Float64Array::from(cloud_covers);
    
    // Initialize calculated fields with nulls
    let heat_index_array = arrow::array::new_null_array(&DataType::Float64, readings.len());
    let wind_chill_array = arrow::array::new_null_array(&DataType::Float64, readings.len());
    let dew_point_array = arrow::array::new_null_array(&DataType::Float64, readings.len());
    let comfort_index_array = arrow::array::new_null_array(&DataType::Float64, readings.len());
    let storm_risk_array = arrow::array::new_null_array(&DataType::Float64, readings.len());
    let is_extreme_array = arrow::array::new_null_array(&DataType::Boolean, readings.len());
    
    RecordBatch::try_new(schema, vec![
        Arc::new(station_id_array) as _,
        Arc::new(timestamp_array) as _,
        Arc::new(temperature_array) as _,
        Arc::new(humidity_array) as _,
        Arc::new(pressure_array) as _,
        Arc::new(wind_speed_array) as _,
        Arc::new(wind_direction_array) as _,
        Arc::new(precipitation_array) as _,
        Arc::new(visibility_array) as _,
        Arc::new(cloud_cover_array) as _,
        Arc::new(heat_index_array) as _,
        Arc::new(wind_chill_array) as _,
        Arc::new(dew_point_array) as _,
        Arc::new(comfort_index_array) as _,
        Arc::new(storm_risk_array) as _,
        Arc::new(is_extreme_array) as _,
    ]).map_err(|e| VectrillError::ArrowError(e.to_string()))
}

/// Custom transformation to calculate heat index
pub struct HeatIndexCalculator {
    schema: Arc<Schema>,
}

impl HeatIndexCalculator {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }
    
    /// Calculate heat index using the Steadman formula
    fn calculate_heat_index(temperature: f64, humidity: f64) -> f64 {
        if temperature < 27.0 {
            return temperature;  // Heat index only meaningful above 27°C
        }
        
        let t = temperature;
        let h = humidity;
        
        // Simplified heat index formula
        let hi = -8.78469475556 +
            1.61139411 * t +
            2.33854883889 * h +
            -0.14611605 * t * h +
            -0.012308094 * t * t +
            -0.0164248277778 * h * h +
            0.002211732 * t * t * h +
            0.00072546 * t * h * h +
            -0.000003582 * t * t * h * h;
        
        hi.max(temperature)  // Heat index can't be lower than actual temperature
    }
}

#[async_trait::async_trait]
impl Transformation for HeatIndexCalculator {
    async fn apply(&mut self, batch: &RecordBatch) -> Result<RecordBatch, VectrillError> {
        let temp_array = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        let humidity_array = batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        
        let heat_index_values: Vec<Option<f64>> = temp_array
            .iter()
            .zip(humidity_array.iter())
            .map(|(temp_opt, humidity_opt)| match (temp_opt, humidity_opt) {
                (Some(temp), Some(humidity)) => Some(Self::calculate_heat_index(temp, humidity)),
                _ => None,
            })
            .collect();
        
        let heat_index_array = Float64Array::from(heat_index_values);
        
        // Update the heat_index column in the batch
        let mut new_columns = batch.columns().to_vec();
        new_columns[10] = Arc::new(heat_index_array) as _;
        
        RecordBatch::try_new(self.schema.clone(), new_columns)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))
    }
    
    fn name(&self) -> &str {
        "heat_index_calculator"
    }
    
    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Custom transformation to calculate wind chill
pub struct WindChillCalculator {
    schema: Arc<Schema>,
}

impl WindChillCalculator {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }
    
    /// Calculate wind chill using the Canadian formula
    fn calculate_wind_chill(temperature: f64, wind_speed: f64) -> f64 {
        if temperature > 10.0 || wind_speed < 4.8 {
            return temperature;  // Wind chill only meaningful below 10°C and above 4.8 km/h
        }
        
        let t = temperature;
        let v = wind_speed;
        
        // Wind chill formula
        13.12 + 0.6215 * t - 11.37 * (v * 0.16).powf(0.16) + 0.3965 * t * (v * 0.16).powf(0.16)
    }
}

#[async_trait::async_trait]
impl Transformation for WindChillCalculator {
    async fn apply(&mut self, batch: &RecordBatch) -> Result<RecordBatch, VectrillError> {
        let temp_array = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        let wind_speed_array = batch.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
        
        let wind_chill_values: Vec<Option<f64>> = temp_array
            .iter()
            .zip(wind_speed_array.iter())
            .map(|(temp_opt, wind_speed_opt)| match (temp_opt, wind_speed_opt) {
                (Some(temp), Some(wind_speed)) => Some(Self::calculate_wind_chill(temp, wind_speed)),
                _ => None,
            })
            .collect();
        
        let wind_chill_array = Float64Array::from(wind_chill_values);
        
        // Update the wind_chill column in the batch
        let mut new_columns = batch.columns().to_vec();
        new_columns[11] = Arc::new(wind_chill_array) as _;
        
        RecordBatch::try_new(self.schema.clone(), new_columns)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))
    }
    
    fn name(&self) -> &str {
        "wind_chill_calculator"
    }
    
    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Custom transformation to calculate dew point
pub struct DewPointCalculator {
    schema: Arc<Schema>,
}

impl DewPointCalculator {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }
    
    /// Calculate dew point using Magnus formula
    fn calculate_dew_point(temperature: f64, humidity: f64) -> f64 {
        let a = 17.27;
        let b = 237.7;
        let alpha = ((a * temperature) / (b + temperature)) + humidity.ln();
        (b * alpha) / (a - alpha)
    }
}

#[async_trait::async_trait]
impl Transformation for DewPointCalculator {
    async fn apply(&mut self, batch: &RecordBatch) -> Result<RecordBatch, VectrillError> {
        let temp_array = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        let humidity_array = batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        
        let dew_point_values: Vec<Option<f64>> = temp_array
            .iter()
            .zip(humidity_array.iter())
            .map(|(temp_opt, humidity_opt)| match (temp_opt, humidity_opt) {
                (Some(temp), Some(humidity)) => Some(Self::calculate_dew_point(temp, humidity)),
                _ => None,
            })
            .collect();
        
        let dew_point_array = Float64Array::from(dew_point_values);
        
        // Update the dew_point column in the batch
        let mut new_columns = batch.columns().to_vec();
        new_columns[12] = Arc::new(dew_point_array) as _;
        
        RecordBatch::try_new(self.schema.clone(), new_columns)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))
    }
    
    fn name(&self) -> &str {
        "dew_point_calculator"
    }
    
    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Custom transformation to calculate comfort index
pub struct ComfortIndexCalculator {
    schema: Arc<Schema>,
}

impl ComfortIndexCalculator {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }
    
    /// Calculate comfort index based on temperature and humidity
    fn calculate_comfort_index(temperature: f64, humidity: f64) -> f64 {
        // Comfort index: 0-100, where 50 is most comfortable
        let temp_factor = 1.0 - ((temperature - 22.0).abs() / 20.0).min(1.0);
        let humidity_factor = 1.0 - ((humidity - 45.0).abs() / 40.0).min(1.0);
        (temp_factor * 0.6 + humidity_factor * 0.4) * 100.0
    }
}

#[async_trait::async_trait]
impl Transformation for ComfortIndexCalculator {
    async fn apply(&mut self, batch: &RecordBatch) -> Result<RecordBatch, VectrillError> {
        let temp_array = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        let humidity_array = batch.column(3).as_any().downcast_ref::<Float64Array>().unwrap();
        
        let comfort_index_values: Vec<Option<f64>> = temp_array
            .iter()
            .zip(humidity_array.iter())
            .map(|(temp_opt, humidity_opt)| match (temp_opt, humidity_opt) {
                (Some(temp), Some(humidity)) => Some(Self::calculate_comfort_index(temp, humidity)),
                _ => None,
            })
            .collect();
        
        let comfort_index_array = Float64Array::from(comfort_index_values);
        
        // Update the comfort_index column in the batch
        let mut new_columns = batch.columns().to_vec();
        new_columns[13] = Arc::new(comfort_index_array) as _;
        
        RecordBatch::try_new(self.schema.clone(), new_columns)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))
    }
    
    fn name(&self) -> &str {
        "comfort_index_calculator"
    }
    
    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Custom transformation to calculate storm risk
pub struct StormRiskCalculator {
    schema: Arc<Schema>,
}

impl StormRiskCalculator {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }
    
    /// Calculate storm risk based on multiple factors
    fn calculate_storm_risk(pressure: f64, precipitation: f64, wind_speed: f64, cloud_cover: f64) -> f64 {
        let pressure_risk = if pressure < 1000.0 {
            (1000.0 - pressure) / 20.0  // Low pressure increases risk
        } else {
            0.0
        };
        
        let precipitation_risk = (precipitation / 10.0).min(1.0);
        let wind_risk = (wind_speed / 50.0).min(1.0);
        let cloud_risk = cloud_cover / 100.0;
        
        // Weighted combination of risk factors
        (pressure_risk * 0.3 + precipitation_risk * 0.3 + wind_risk * 0.2 + cloud_risk * 0.2) * 100.0
    }
}

#[async_trait::async_trait]
impl Transformation for StormRiskCalculator {
    async fn apply(&mut self, batch: &RecordBatch) -> Result<RecordBatch, VectrillError> {
        let pressure_array = batch.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        let precipitation_array = batch.column(7).as_any().downcast_ref::<Float64Array>().unwrap();
        let wind_speed_array = batch.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
        let cloud_cover_array = batch.column(9).as_any().downcast_ref::<Float64Array>().unwrap();
        
        let storm_risk_values: Vec<Option<f64>> = pressure_array
            .iter()
            .zip(precipitation_array.iter())
            .zip(wind_speed_array.iter())
            .zip(cloud_cover_array.iter())
            .map(|(((pressure_opt, precip_opt), wind_opt), cloud_opt)| {
                match (pressure_opt, precip_opt, wind_opt, cloud_opt) {
                    (Some(pressure), Some(precip), Some(wind), Some(cloud)) => {
                        Some(Self::calculate_storm_risk(pressure, precip, wind, cloud))
                    }
                    _ => None,
                }
            })
            .collect();
        
        let storm_risk_array = Float64Array::from(storm_risk_values);
        
        // Update the storm_risk column in the batch
        let mut new_columns = batch.columns().to_vec();
        new_columns[14] = Arc::new(storm_risk_array) as _;
        
        RecordBatch::try_new(self.schema.clone(), new_columns)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))
    }
    
    fn name(&self) -> &str {
        "storm_risk_calculator"
    }
    
    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Custom transformation to identify extreme weather conditions
pub struct ExtremeWeatherDetector {
    schema: Arc<Schema>,
}

impl ExtremeWeatherDetector {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }
    
    /// Determine if weather conditions are extreme
    fn is_extreme(temperature: f64, wind_speed: f64, precipitation: f64, storm_risk: f64) -> bool {
        temperature < -20.0 || temperature > 40.0 ||  // Extreme temperature
        wind_speed > 80.0 ||                          // Extreme wind
        precipitation > 50.0 ||                        // Heavy precipitation
        storm_risk > 75.0                            // High storm risk
    }
}

#[async_trait::async_trait]
impl Transformation for ExtremeWeatherDetector {
    async fn apply(&mut self, batch: &RecordBatch) -> Result<RecordBatch, VectrillError> {
        let temp_array = batch.column(2).as_any().downcast_ref::<Float64Array>().unwrap();
        let wind_speed_array = batch.column(5).as_any().downcast_ref::<Float64Array>().unwrap();
        let precipitation_array = batch.column(7).as_any().downcast_ref::<Float64Array>().unwrap();
        let storm_risk_array = batch.column(14).as_any().downcast_ref::<Float64Array>().unwrap();
        
        let is_extreme_values: Vec<Option<bool>> = temp_array
            .iter()
            .zip(wind_speed_array.iter())
            .zip(precipitation_array.iter())
            .zip(storm_risk_array.iter())
            .map(|(((temp_opt, wind_opt), precip_opt), storm_opt)| {
                match (temp_opt, wind_opt, precip_opt, storm_opt) {
                    (Some(temp), Some(wind), Some(precip), Some(storm)) => {
                        Some(Self::is_extreme(temp, wind, precip, storm))
                    }
                    _ => None,
                }
            })
            .collect();
        
        let is_extreme_array = BooleanArray::from(is_extreme_values);
        
        // Update the is_extreme column in the batch
        let mut new_columns = batch.columns().to_vec();
        new_columns[15] = Arc::new(is_extreme_array) as _;
        
        RecordBatch::try_new(self.schema.clone(), new_columns)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))
    }
    
    fn name(&self) -> &str {
        "extreme_weather_detector"
    }
    
    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Build the weather prediction pipeline
fn build_weather_pipeline(schema: Arc<Schema>) -> TransformationPipeline {
    TransformationPipeline::new("weather_prediction_pipeline".to_string())
        .add_transform(Box::new(HeatIndexCalculator::new(schema.clone())))
        .add_transform(Box::new(WindChillCalculator::new(schema.clone())))
        .add_transform(Box::new(DewPointCalculator::new(schema.clone())))
        .add_transform(Box::new(ComfortIndexCalculator::new(schema.clone())))
        .add_transform(Box::new(StormRiskCalculator::new(schema.clone())))
        .add_transform(Box::new(ExtremeWeatherDetector::new(schema.clone())))
        .add_transform(Box::new(FilterTransform::new(
            "temperature".to_string(),
            FilterOperator::GreaterThan,
            FilterValue::Float64(-50.0),  // Filter out impossible temperatures
            schema.clone(),
        )))
        .add_transform(Box::new(MapTransform::new(
            "wind_speed".to_string(),
            MapOperation::Multiply(1.0),  // Convert to m/s if needed
            "wind_speed_ms".to_string(),
            schema.clone(),
        )))
}

/// Main weather prediction system example
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🌤️  Starting Weather Prediction System Example");
    
    // Create schema
    let schema = create_weather_schema();
    
    // Create transformation pipeline
    let mut pipeline = build_weather_pipeline(schema.clone());
    
    // Generate weather data for multiple stations
    let stations = vec!["NYC", "LAX", "CHI", "HOU", "PHX"];
    let weather_data = generate_weather_data(&stations, 200);  // 200 readings per station
    
    println!("📡 Generated weather data for {} stations", stations.len());
    println!("📊 Total readings: {}", weather_data.len());
    
    // Create data source
    let mut source = MemoryConnector::with_schema(
        "weather_source".to_string(),
        schema.clone(),
        10,  // 10 batches
        100, // 100 readings per batch
    );
    
    // Create output sink
    let mut sink = FileSink::new(
        std::path::PathBuf::from("weather_predictions.csv"),
        FileSinkFormat::Csv,
        schema,
    )?;
    
    println!("🔄 Processing weather data through prediction pipeline...");
    
    // Process data through pipeline
    let mut total_processed = 0;
    let mut batch_count = 0;
    let mut extreme_count = 0;
    
    while let Some(batch_result) = source.next_batch().await {
        let batch = batch_result?;
        batch_count += 1;
        
        println!("🌡️  Processing batch {} ({} readings)", batch_count, batch.num_rows());
        
        // Apply transformation pipeline
        let transformed_batch = pipeline.apply(batch).await?;
        
        // Count extreme weather conditions
        if let Some(is_extreme_array) = transformed_batch.column(15).as_any().downcast_ref::<BooleanArray>() {
            extreme_count += is_extreme_array.iter().filter(|&x| x == Some(true)).count();
        }
        
        // Write to output
        sink.write_batch(&transformed_batch).await?;
        
        total_processed += transformed_batch.num_rows();
        
        // Simulate real-time processing delay
        sleep(Duration::from_millis(50)).await;
    }
    
    sink.flush().await?;
    
    println!("✅ Weather prediction processing complete!");
    println!("📈 Total batches processed: {}", batch_count);
    println!("📊 Total readings processed: {}", total_processed);
    println!("⚠️  Extreme weather conditions detected: {}", extreme_count);
    println!("💾 Output saved to: weather_predictions.csv");
    
    // Display pipeline statistics
    println!("\n🔧 Pipeline Statistics:");
    println!("  - Pipeline name: {}", pipeline.name());
    println!("  - Number of transformations: {}", pipeline.len());
    
    println!("\n📋 Weather Calculations:");
    println!("  🌡️  Heat Index (feels like temperature)");
    println!("  ❄️  Wind Chill (feels like temperature in wind)");
    println!("  💧 Dew Point (condensation temperature)");
    println!("  😌 Comfort Index (0-100 comfort level)");
    println!("  ⛈️  Storm Risk (0-100 probability)");
    println!("  🚨 Extreme Weather Detection");
    
    println!("\n🎯 Key Features Demonstrated:");
    println!("  ✅ Multi-station weather data processing");
    println!("  ✅ Complex meteorological calculations");
    println!("  ✅ Real-time weather prediction");
    println!("  ✅ Extreme weather detection");
    println!("  ✅ Data filtering and transformation");
    println!("  ✅ Pipeline composition");
    println!("  ✅ File output integration");
    
    println!("\n📈 Weather Stations Monitored:");
    for station in &stations {
        println!("  📍 {}", station);
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_weather_schema_creation() {
        let schema = create_weather_schema();
        assert_eq!(schema.fields().len(), 16);
        assert_eq!(schema.field(0).name(), "station_id");
        assert_eq!(schema.field(2).name(), "temperature");
        assert_eq!(schema.field(10).name(), "heat_index");
    }
    
    #[test]
    fn test_weather_data_generation() {
        let stations = vec!["TEST"];
        let readings = generate_weather_data(&stations, 10);
        assert_eq!(readings.len(), 10);
        assert_eq!(readings[0].station_id, "TEST");
        assert!(readings[0].temperature > -50.0 && readings[0].temperature < 60.0);
        assert!(readings[0].humidity >= 0.0 && readings[0].humidity <= 100.0);
    }
    
    #[test]
    fn test_heat_index_calculation() {
        let hi = HeatIndexCalculator::calculate_heat_index(30.0, 80.0);
        assert!(hi > 30.0);  // Heat index should be higher than temperature
        
        let hi_cold = HeatIndexCalculator::calculate_heat_index(20.0, 60.0);
        assert_eq!(hi_cold, 20.0);  // Should return temperature for low temps
    }
    
    #[test]
    fn test_wind_chill_calculation() {
        let wc = WindChillCalculator::calculate_wind_chill(5.0, 20.0);
        assert!(wc < 5.0);  // Wind chill should be lower than temperature
        
        let wc_warm = WindChillCalculator::calculate_wind_chill(15.0, 20.0);
        assert_eq!(wc_warm, 15.0);  // Should return temperature for warm temps
    }
    
    #[test]
    fn test_storm_risk_calculation() {
        let risk = StormRiskCalculator::calculate_storm_risk(980.0, 10.0, 30.0, 90.0);
        assert!(risk > 0.0 && risk <= 100.0);
        
        let risk_low = StormRiskCalculator::calculate_storm_risk(1020.0, 0.0, 5.0, 10.0);
        assert!(risk_low < risk);  // Low risk conditions should have lower risk
    }
    
    #[test]
    fn test_transformation_pipeline_creation() {
        let schema = create_weather_schema();
        let pipeline = build_weather_pipeline(schema);
        assert_eq!(pipeline.name(), "weather_prediction_pipeline");
        assert_eq!(pipeline.len(), 9);
    }
}
