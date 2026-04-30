//! End-to-end tests for connectors

use vectrill::connectors::{Connector, FileConnector};
use vectrill::connectors::file::FileFormat;
use arrow::datatypes::{Schema, Field, DataType};
use std::path::PathBuf;
use std::sync::Arc;

#[test]
fn test_csv_connector_basic() {
    let path = PathBuf::from("tests/fixtures/data.csv");
    let format = FileFormat::Csv;
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
        Field::new("active", DataType::Boolean, false),
    ]));
    
    let connector = FileConnector::new(path, format, schema).expect("Failed to create connector");
    
    // Test connector creation - name returns the path
    assert!(connector.name().contains("data.csv"));
    
    // Test schema
    let schema = connector.schema();
    assert!(schema.fields().len() > 0);
}
