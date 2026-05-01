# M0: Core Engine Skeleton

## Goal
Get a minimal Rust crate compiling with clean architecture.

## Duration
1-2 days

## Deliverables
- Project structure
- Basic RecordBatch pipeline
- Stub operator trait
- CLI that accepts a dummy batch and passes it through

## Tasks

### 1. Project Setup
- [ ] Initialize Cargo project
- [ ] Set up directory structure
- [ ] Configure Cargo.toml with dependencies
- [ ] Set up pyproject.toml for Python integration
- [ ] Create .gitignore

### 2. Core Types
- [ ] Define `Operator` trait
- [ ] Define `RecordBatch` type alias
- [ ] Define basic error types

### 3. Stub Implementation
- [ ] Implement `NoOpOperator` (passes batches through)
- [ ] Implement `Pipeline` struct
- [ ] Add basic pipeline execution logic

### 4. CLI
- [ ] Create main.rs with CLI
- [ ] Add command to process dummy batch
- [ ] Add basic logging

### 5. Tests
- [ ] Unit test for operator trait
- [ ] Unit test for pipeline execution
- [ ] Integration test for CLI

## Implementation Details

### Operator Trait
```rust
pub trait Operator: Send + Sync {
    fn process(&mut self, batch: RecordBatch) -> Result<RecordBatch>;
    fn flush(&mut self) -> Result<Vec<RecordBatch>>;
}
```

### Pipeline
```rust
pub struct Pipeline {
    operators: Vec<Box<dyn Operator>>,
}

impl Pipeline {
    pub fn new() -> Self;
    pub fn add_operator(&mut self, op: Box<dyn Operator>);
    pub fn execute(&mut self, batch: RecordBatch) -> Result<RecordBatch>;
}
```

## Success Criteria
- [ ] Cargo builds successfully
- [ ] CLI runs without errors
- [ ] Dummy batch passes through pipeline
- [ ] All tests pass

## Dependencies
- `arrow` >= 51
- `thiserror` for error handling
- `clap` for CLI
