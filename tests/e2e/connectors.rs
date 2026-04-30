//! End-to-end tests for connectors

use vectrill::connectors::{Connector, FileConnector};
use vectrill::error::Result;
use std::path::PathBuf;

#[test]
fn test_csv_connector_basic() {
    let mut connector = FileConnector::new(PathBuf::from("tests/fixtures/data.csv"));
    
    // Test connector creation
    assert_eq!(connector.name(), "file");
    
    // Test schema inference (if CSV exists)
    if PathBuf::from("tests/fixtures/data.csv").exists() {
        let schema = connector.schema();
        assert!(schema.is_ok());
    }
}

#[test]
fn test_csv_connector_streaming() {
    let connector = FileConnector::new(PathBuf::from("tests/fixtures/data.csv"));
    
    // Test streaming batches
    if PathBuf::from("tests/fixtures/data.csv").exists() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let mut stream = connector.stream().await.unwrap();
            let batch_count = stream.by_ref().count().await;
            assert!(batch_count > 0);
        });
    }
}

#[cfg(feature = "kafka")]
#[test]
#[ignore] // Requires Kafka to be running
fn test_kafka_connector_basic() {
    // Kafka connector tests would go here
    // These require Kafka to be available via docker-compose
}

#[cfg(feature = "json")]
#[test]
fn test_json_connector_basic() {
    // JSON connector tests would go here
}

#[cfg(feature = "parquet")]
#[test]
fn test_parquet_connector_basic() {
    // Parquet connector tests would go here
}
