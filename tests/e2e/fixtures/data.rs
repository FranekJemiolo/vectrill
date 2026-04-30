//! Test data fixtures

/// Sample CSV data for testing
pub const CSV_SAMPLE: &str = r#"id,name,value,active
1,Alice,100,true
2,Bob,200,false
3,Charlie,300,true
4,Diana,400,false
5,Eve,500,true
"#;

/// Sample JSON data for testing
pub const JSON_SAMPLE: &str = r#"[
  {"id": 1, "name": "Alice", "value": 100, "active": true},
  {"id": 2, "name": "Bob", "value": 200, "active": false},
  {"id": 3, "name": "Charlie", "value": 300, "active": true}
]"#;

/// Sample SQL for PostgreSQL testing
pub const SQL_CREATE_TABLE: &str = r#"
CREATE TABLE IF NOT EXISTS test_data (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    value INTEGER,
    active BOOLEAN
);
"#;

/// Sample SQL insert
pub const SQL_INSERT_DATA: &str = r#"
INSERT INTO test_data (name, value, active) VALUES
    ('Alice', 100, true),
    ('Bob', 200, false),
    ('Charlie', 300, true);
"#;

/// Kafka topic for testing
pub const KAFKA_TEST_TOPIC: &str = "vectrill-test-topic";
