//! End-to-end tests for connectors

use arrow::datatypes::{DataType, Field, Schema};
use std::path::PathBuf;
use std::sync::Arc;
use vectrill::connectors::file::FileFormat;
use vectrill::connectors::{Connector, FileConnector};

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
