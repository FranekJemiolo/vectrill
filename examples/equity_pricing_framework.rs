//! Equity Pricing Framework Example
//!
//! This example demonstrates how to use Vectrill's transformation framework
//! to build a real-time equity pricing and analysis system.

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

use arrow::array::{Float64Array, Int64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema};
use chrono::Utc;

/// Equity pricing data structure
#[derive(Debug, Clone)]
pub struct EquityTick {
    symbol: String,
    timestamp: i64,
    price: f64,
    volume: i64,
    bid: f64,
    ask: f64,
}

/// Create equity pricing schema
fn create_equity_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new(
            "timestamp",
            DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("price", DataType::Float64, false),
        Field::new("volume", DataType::Int64, false),
        Field::new("bid", DataType::Float64, false),
        Field::new("ask", DataType::Float64, false),
        Field::new("spread", DataType::Float64, true), // Calculated field
        Field::new("mid_price", DataType::Float64, true), // Calculated field
        Field::new("price_change", DataType::Float64, true), // Calculated field
        Field::new("volume_weighted_price", DataType::Float64, true), // Calculated field
    ]))
}

/// Generate sample equity tick data
fn generate_equity_ticks(symbol: &str, count: usize) -> Vec<EquityTick> {
    let mut ticks = Vec::new();
    let mut base_price = 100.0;

    for _i in 0..count {
        let timestamp = Utc::now().timestamp_millis();

        // Simulate price movement with random walk
        let price_change = (rand::random::<f64>() - 0.5) * 2.0; // +/- 1.0
        base_price += price_change;
        base_price = base_price.max(1.0); // Ensure price doesn't go negative

        let bid = base_price - 0.01;
        let ask = base_price + 0.01;
        let volume = (rand::random::<f64>() * 10000.0) as i64;

        ticks.push(EquityTick {
            symbol: symbol.to_string(),
            timestamp,
            price: base_price,
            volume,
            bid,
            ask,
        });
    }

    ticks
}

/// Convert equity ticks to Arrow RecordBatch
fn ticks_to_record_batch(
    ticks: Vec<EquityTick>,
    schema: Arc<Schema>,
) -> Result<RecordBatch, VectrillError> {
    let symbols: Vec<String> = ticks.iter().map(|t| t.symbol.clone()).collect();
    let timestamps: Vec<i64> = ticks.iter().map(|t| t.timestamp).collect();
    let prices: Vec<f64> = ticks.iter().map(|t| t.price).collect();
    let volumes: Vec<i64> = ticks.iter().map(|t| t.volume).collect();
    let bids: Vec<f64> = ticks.iter().map(|t| t.bid).collect();
    let asks: Vec<f64> = ticks.iter().map(|t| t.ask).collect();

    let symbol_array = StringArray::from(symbols);
    let timestamp_array = TimestampMillisecondArray::from(timestamps);
    let price_array = Float64Array::from(prices);
    let volume_array = Int64Array::from(volumes);
    let bid_array = Float64Array::from(bids);
    let ask_array = Float64Array::from(asks);

    // Initialize calculated fields with nulls
    let spread_array =
        arrow::array::new_null_array(&arrow::datatypes::DataType::Float64, ticks.len());
    let mid_price_array =
        arrow::array::new_null_array(&arrow::datatypes::DataType::Float64, ticks.len());
    let price_change_array =
        arrow::array::new_null_array(&arrow::datatypes::DataType::Float64, ticks.len());
    let vwap_array =
        arrow::array::new_null_array(&arrow::datatypes::DataType::Float64, ticks.len());

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(symbol_array) as _,
            Arc::new(timestamp_array) as _,
            Arc::new(price_array) as _,
            Arc::new(volume_array) as _,
            Arc::new(bid_array) as _,
            Arc::new(ask_array) as _,
            Arc::new(spread_array) as _,
            Arc::new(mid_price_array) as _,
            Arc::new(price_change_array) as _,
            Arc::new(vwap_array) as _,
        ],
    )
    .map_err(|e| VectrillError::ArrowError(e.to_string()))
}

/// Custom transformation to calculate bid-ask spread
pub struct SpreadCalculator {
    schema: Arc<Schema>,
}

impl SpreadCalculator {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }
}

#[async_trait::async_trait]
impl Transformation for SpreadCalculator {
    async fn apply(&mut self, batch: &RecordBatch) -> Result<RecordBatch, VectrillError> {
        let bid_array = batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let ask_array = batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        let spread_values: Vec<Option<f64>> = bid_array
            .iter()
            .zip(ask_array.iter())
            .map(|(bid, ask)| match (bid, ask) {
                (Some(b), Some(a)) => Some(a - b),
                _ => None,
            })
            .collect();

        let spread_array = Float64Array::from(spread_values);

        // Update the spread column in the batch
        let mut new_columns = batch.columns().to_vec();
        new_columns[6] = Arc::new(spread_array) as _;

        RecordBatch::try_new(self.schema.clone(), new_columns)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))
    }

    fn name(&self) -> &str {
        "spread_calculator"
    }

    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Custom transformation to calculate mid price
pub struct MidPriceCalculator {
    schema: Arc<Schema>,
}

impl MidPriceCalculator {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self { schema }
    }
}

#[async_trait::async_trait]
impl Transformation for MidPriceCalculator {
    async fn apply(&mut self, batch: &RecordBatch) -> Result<RecordBatch, VectrillError> {
        let bid_array = batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let ask_array = batch
            .column(5)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        let mid_price_values: Vec<Option<f64>> = bid_array
            .iter()
            .zip(ask_array.iter())
            .map(|(bid, ask)| match (bid, ask) {
                (Some(b), Some(a)) => Some((b + a) / 2.0),
                _ => None,
            })
            .collect();

        let mid_price_array = Float64Array::from(mid_price_values);

        // Update the mid_price column in the batch
        let mut new_columns = batch.columns().to_vec();
        new_columns[7] = Arc::new(mid_price_array) as _;

        RecordBatch::try_new(self.schema.clone(), new_columns)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))
    }

    fn name(&self) -> &str {
        "mid_price_calculator"
    }

    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Custom transformation to calculate price changes
pub struct PriceChangeCalculator {
    schema: Arc<Schema>,
    previous_prices: std::collections::HashMap<String, f64>,
}

impl PriceChangeCalculator {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self {
            schema,
            previous_prices: std::collections::HashMap::new(),
        }
    }
}

#[async_trait::async_trait]
impl Transformation for PriceChangeCalculator {
    async fn apply(&mut self, batch: &RecordBatch) -> Result<RecordBatch, VectrillError> {
        let symbol_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let price_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        let price_change_values: Vec<Option<f64>> = symbol_array
            .iter()
            .zip(price_array.iter())
            .map(|(symbol_opt, price_opt)| match (symbol_opt, price_opt) {
                (Some(symbol), Some(price)) => {
                    let change = if let Some(prev_price) = self.previous_prices.get(symbol) {
                        price - prev_price
                    } else {
                        0.0
                    };
                    self.previous_prices.insert(symbol.to_string(), price);
                    Some(change)
                }
                _ => None,
            })
            .collect();

        let price_change_array = Float64Array::from(price_change_values);

        // Update the price_change column in the batch
        let mut new_columns = batch.columns().to_vec();
        new_columns[8] = Arc::new(price_change_array) as _;

        RecordBatch::try_new(self.schema.clone(), new_columns)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))
    }

    fn name(&self) -> &str {
        "price_change_calculator"
    }

    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Custom transformation to calculate volume weighted average price (VWAP)
pub struct VwapCalculator {
    schema: Arc<Schema>,
    cumulative_volume: std::collections::HashMap<String, f64>,
    cumulative_value: std::collections::HashMap<String, f64>,
}

impl VwapCalculator {
    pub fn new(schema: Arc<Schema>) -> Self {
        Self {
            schema,
            cumulative_volume: std::collections::HashMap::new(),
            cumulative_value: std::collections::HashMap::new(),
        }
    }
}

#[async_trait::async_trait]
impl Transformation for VwapCalculator {
    async fn apply(&mut self, batch: &RecordBatch) -> Result<RecordBatch, VectrillError> {
        let symbol_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let price_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let volume_array = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let vwap_values: Vec<Option<f64>> = symbol_array
            .iter()
            .zip(price_array.iter())
            .zip(volume_array.iter())
            .map(|((symbol_opt, price_opt), volume_opt)| {
                match (symbol_opt, price_opt, volume_opt) {
                    (Some(symbol), Some(price), Some(volume)) => {
                        let volume_f = volume as f64;
                        let cum_volume = self
                            .cumulative_volume
                            .entry(symbol.to_string())
                            .or_insert(0.0);
                        let cum_value = self
                            .cumulative_value
                            .entry(symbol.to_string())
                            .or_insert(0.0);

                        *cum_volume += volume_f;
                        *cum_value += price * volume_f;

                        let vwap = *cum_value / *cum_volume;
                        Some(vwap)
                    }
                    _ => None,
                }
            })
            .collect();

        let vwap_array = Float64Array::from(vwap_values);

        // Update the vwap column in the batch
        let mut new_columns = batch.columns().to_vec();
        new_columns[9] = Arc::new(vwap_array) as _;

        RecordBatch::try_new(self.schema.clone(), new_columns)
            .map_err(|e| VectrillError::ArrowError(e.to_string()))
    }

    fn name(&self) -> &str {
        "vwap_calculator"
    }

    fn output_schema(&self) -> arrow::datatypes::SchemaRef {
        self.schema.clone()
    }
}

/// Build the equity pricing transformation pipeline
fn build_equity_pricing_pipeline(schema: Arc<Schema>) -> TransformationPipeline {
    TransformationPipeline::new("equity_pricing_pipeline".to_string())
        .add_transform(Box::new(SpreadCalculator::new(schema.clone())))
        .add_transform(Box::new(MidPriceCalculator::new(schema.clone())))
        .add_transform(Box::new(PriceChangeCalculator::new(schema.clone())))
        .add_transform(Box::new(VwapCalculator::new(schema.clone())))
        .add_transform(Box::new(FilterTransform::new(
            "price".to_string(),
            FilterOperator::GreaterThan,
            FilterValue::Float64(0.0),
            schema.clone(),
        )))
        .add_transform(Box::new(MapTransform::new(
            "volume".to_string(),
            MapOperation::Multiply(0.001), // Convert to thousands
            "volume_thousands".to_string(),
            schema.clone(),
        )))
}

/// Main equity pricing framework example
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Starting Equity Pricing Framework Example");

    // Create schema
    let schema = create_equity_schema();

    // Create transformation pipeline
    let mut pipeline = build_equity_pricing_pipeline(schema.clone());

    // Create data source
    let mut source = MemoryConnector::with_schema(
        "equity_source".to_string(),
        schema.clone(),
        5,   // 5 batches
        100, // 100 ticks per batch
    );

    // Create output sink
    let mut sink = FileSink::new(
        std::path::PathBuf::from("equity_pricing_output.csv"),
        FileSinkFormat::Csv,
        schema,
    )?;

    println!("📊 Processing equity pricing data...");

    // Process data through pipeline
    let mut total_processed = 0;
    let mut batch_count = 0;

    while let Some(batch_result) = source.next_batch().await {
        let batch = batch_result?;
        batch_count += 1;

        println!(
            "🔄 Processing batch {} ({} rows)",
            batch_count,
            batch.num_rows()
        );

        // Apply transformation pipeline
        let transformed_batch = pipeline.apply(batch).await?;

        // Write to output
        sink.write_batch(&transformed_batch).await?;

        total_processed += transformed_batch.num_rows();

        // Simulate real-time processing delay
        sleep(Duration::from_millis(100)).await;
    }

    sink.flush().await?;

    println!("✅ Equity pricing processing complete!");
    println!("📈 Total batches processed: {}", batch_count);
    println!("📊 Total records processed: {}", total_processed);
    println!("💾 Output saved to: equity_pricing_output.csv");

    // Display pipeline statistics
    println!("\n🔧 Pipeline Statistics:");
    println!("  - Pipeline name: {}", pipeline.name());
    println!("  - Number of transformations: {}", pipeline.len());

    println!("\n📋 Transformation Steps:");
    println!("  1. Calculate bid-ask spread");
    println!("  2. Calculate mid price");
    println!("  3. Calculate price changes");
    println!("  4. Calculate volume weighted average price (VWAP)");
    println!("  5. Filter out zero or negative prices");
    println!("  6. Convert volume to thousands");

    println!("\n🎯 Key Features Demonstrated:");
    println!("  ✅ Real-time data processing");
    println!("  ✅ Custom transformation framework");
    println!("  ✅ Complex calculations (spread, VWAP, price changes)");
    println!("  ✅ Data filtering and mapping");
    println!("  ✅ Pipeline composition");
    println!("  ✅ File output integration");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_equity_schema_creation() {
        let schema = create_equity_schema();
        assert_eq!(schema.fields().len(), 10);
        assert_eq!(schema.field(0).name(), "symbol");
        assert_eq!(schema.field(1).name(), "timestamp");
        assert_eq!(schema.field(2).name(), "price");
    }

    #[test]
    fn test_equity_tick_generation() {
        let ticks = generate_equity_ticks("AAPL", 10);
        assert_eq!(ticks.len(), 10);
        assert_eq!(ticks[0].symbol, "AAPL");
        assert!(ticks[0].price > 0.0);
        assert!(ticks[0].volume > 0);
        assert!(ticks[0].ask > ticks[0].bid);
    }

    #[test]
    fn test_transformation_pipeline_creation() {
        let schema = create_equity_schema();
        let pipeline = build_equity_pricing_pipeline(schema);
        assert_eq!(pipeline.name(), "equity_pricing_pipeline");
        assert_eq!(pipeline.len(), 6);
    }
}
