# End-to-End Tests

This directory contains end-to-end tests for vectrill using docker-compose to set up external dependencies.

## Prerequisites

- Docker
- Docker Compose

## Running Tests

### Start the test environment

```bash
docker-compose -f docker-compose.test.yml up -d
```

This will start:
- Kafka (port 9092) for connector testing
- PostgreSQL (port 5432) for database connector testing
- A test runner service

### Run tests locally

```bash
# Run all e2e tests
cargo test --test e2e --features connectors-full,web-ui

# Run specific connector tests
cargo test --test e2e connectors --features connectors-full

# Run advanced features tests
cargo test --test e2e advanced_features --features web-ui
```

### Run tests with docker-compose

```bash
# Build and run tests in the test runner container
docker-compose -f docker-compose.test.yml run --rm test-runner

# Run tests with verbose output
docker-compose -f docker-compose.test.yml run --rm test-runner cargo test --test e2e -- --nocapture
```

### Stop the test environment

```bash
docker-compose -f docker-compose.test.yml down
```

## Test Structure

- `connectors.rs` - Tests for data connectors (CSV, JSON, Parquet, Kafka)
- `advanced_features.rs` - Tests for advanced features (expression optimization, buffer pooling, performance counters)
- `fixtures/` - Test data and fixtures

## Test Data

Test data is located in `tests/fixtures/`:
- `data.csv` - Sample CSV data for connector testing

## Environment Variables

The following environment variables are available when running tests with docker-compose:

- `KAFKA_BROKERS` - Kafka broker addresses (default: `kafka:29092`)
- `POSTGRES_HOST` - PostgreSQL host (default: `postgres`)
- `POSTGRES_PORT` - PostgreSQL port (default: `5432`)
- `POSTGRES_USER` - PostgreSQL user (default: `testuser`)
- `POSTGRES_PASSWORD` - PostgreSQL password (default: `testpass`)
- `POSTGRES_DB` - PostgreSQL database (default: `testdb`)
