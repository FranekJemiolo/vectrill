# API Reference

This document provides a comprehensive API reference for Vectrill.

## Python API

### vectrill

The main module for Vectrill.

#### vectrill.source()

Create a data source for a streaming pipeline.

```python
vectrill.source(source_type, **kwargs)
```

**Parameters:**
- `source_type` (str): Type of source ("file", "memory", "kafka")
- `**kwargs`: Source-specific parameters

**Returns:** Stream object

**Example:**
```python
stream = vt.source("file", path="data.csv", format="csv")
```

#### Stream Methods

##### .filter()

Filter rows based on a predicate.

```python
stream.filter(predicate)
```

**Parameters:**
- `predicate` (str): Filter expression

**Returns:** Stream object

**Example:**
```python
stream.filter("temperature > 20")
```

##### .map()

Apply a transformation to the data.

```python
stream.map(expression)
```

**Parameters:**
- `expression` (str): Transformation expression

**Returns:** Stream object

**Example:**
```python
stream.map("temp_f = temperature * 1.8 + 32")
```

##### .group_by()

Group data by columns.

```python
stream.group_by(*columns)
```

**Parameters:**
- `*columns`: Column names to group by

**Returns:** Stream object

**Example:**
```python
stream.group_by("device_id", "location")
```

##### .window()

Apply a time window.

```python
stream.window(window_spec)
```

**Parameters:**
- `window_spec` (str): Window specification (e.g., "10s", "1m")

**Returns:** Stream object

**Example:**
```python
stream.window("10s")
```

##### .agg()

Aggregate grouped data.

```python
stream.agg(aggregations)
```

**Parameters:**
- `aggregations` (dict): Column to aggregation function mapping

**Returns:** Stream object

**Example:**
```python
stream.agg({"temperature": "avg", "humidity": "max"})
```

##### .execute()

Execute the streaming pipeline.

```python
stream.execute()
```

**Returns:** Iterator of RecordBatch objects

**Example:**
```python
for batch in stream.execute():
    print(batch)
```

## Rust API

### vectrill::sequencer

Event sequencing and micro-batching.

#### Sequencer

```rust
pub struct Sequencer {
    config: SequencerConfig,
    // ...
}
```

#### Methods

##### new()

Create a new sequencer.

```rust
pub fn new(config: SequencerConfig) -> Self
```

##### ingest()

Ingest a micro-batch.

```rust
pub fn ingest(&mut self, batch: RecordBatch) -> Result<()>
```

##### flush()

Flush all ordered batches.

```rust
pub fn flush(&mut self) -> Result<Vec<RecordBatch>>
```

### vectrill::operators

Core data processing operators.

#### Operator Trait

```rust
pub trait Operator: Send + Sync {
    fn process(&mut self, batch: RecordBatch) -> Result<RecordBatch>;
    fn name(&self) -> &str;
}
```

#### MapOperator

Apply a transformation function.

```rust
pub struct MapOperator<F>
where
    F: Fn(RecordBatch) -> Result<RecordBatch> + Send + Sync,
```

#### FilterOperator

Filter rows based on a predicate.

```rust
pub struct FilterOperator<F>
where
    F: Fn(&RecordBatch) -> Result<BooleanArray> + Send + Sync,
```

### vectrill::expression

Expression evaluation and compilation.

#### Expr

Expression AST.

```rust
pub enum Expr {
    Column(String),
    Literal(ScalarValue),
    Binary { left: Box<Expr>, op: Operator, right: Box<Expr> },
    Unary { op: UnaryOp, expr: Box<Expr> },
    Function { name: String, args: Vec<Expr> },
    Cast { expr: Box<Expr>, data_type: DataType },
}
```

### vectrill::planner

Query planning and optimization.

#### LogicalPlan

Logical representation of a query.

```rust
pub enum LogicalPlan {
    Source { name: String },
    Map { input: Box<LogicalPlan>, expr: Expr },
    Filter { input: Box<LogicalPlan>, expr: Expr },
    Aggregate { input: Box<LogicalPlan>, groups: Vec<Expr>, aggs: Vec<Expr> },
    // ...
}
```

#### PhysicalPlan

Physical execution plan.

```rust
pub enum PhysicalPlan {
    ScanSource { name: String, attrs: SourceAttributes },
    Map { input: Box<PhysicalPlan>, expr: PhysicalExpr },
    Filter { input: Box<PhysicalPlan>, expr: PhysicalExpr },
    Aggregate { input: Box<PhysicalPlan>, groups: Vec<PhysicalExpr>, aggs: Vec<PhysicalExpr> },
    // ...
}
```

### vectrill::optimization

Query and expression optimization.

#### ExprOptimizer

Expression optimizer with constant folding and CSE.

```rust
pub struct ExprOptimizer {
    cse_cache: HashMap<String, Expr>,
}

impl ExprOptimizer {
    pub fn new() -> Self;
    pub fn optimize(&mut self, expr: Expr) -> Expr;
}
```

### vectrill::memory

Memory optimization utilities.

#### BufferPool

Pool for reusing Arrow arrays.

```rust
pub struct BufferPool {
    pools: Mutex<HashMap<DataType, Vec<ArrayRef>>>,
    max_pool_size: usize,
}

impl BufferPool {
    pub fn new(max_pool_size: usize) -> Self;
    pub fn get_array(&self, data_type: &DataType, capacity: usize) -> ArrayRef;
    pub fn return_array(&self, array: ArrayRef);
}
```

### vectrill::performance

Performance monitoring and metrics.

#### Counter

Atomic counter for performance metrics.

```rust
pub struct Counter {
    counter_type: CounterType,
    value: AtomicU64,
}

impl Counter {
    pub fn new(counter_type: CounterType) -> Self;
    pub fn increment(&self);
    pub fn add(&self, value: u64);
    pub fn get(&self) -> u64;
}
```

#### CounterRegistry

Registry for managing named counters.

```rust
pub struct CounterRegistry {
    counters: HashMap<String, Arc<Counter>>,
}

impl CounterRegistry {
    pub fn new() -> Self;
    pub fn register(&mut self, name: String, counter_type: CounterType) -> Arc<Counter>;
    pub fn get(&self, name: &str) -> Option<Arc<Counter>>;
    pub fn snapshot(&self) -> HashMap<String, u64>;
}
```

#### Timer

Timer for measuring execution time.

```rust
pub struct Timer {
    counter: Arc<Counter>,
    start: Option<Instant>,
}

impl Timer {
    pub fn new(counter: Arc<Counter>) -> Self;
    pub fn start(&mut self);
    pub fn stop(&mut self);
}
```

## Error Handling

### VectrillError

Error types for Vectrill.

```rust
pub enum VectrillError {
    InvalidExpression(String),
    InvalidSchema(String),
    IoError(String),
    NotImplemented(String),
    Connector(String),
    // ...
}
```

## Type Conversions

### ScalarValue

Scalar values in expressions.

```rust
pub enum ScalarValue {
    Null(DataType),
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
    Float32(f32),
    Float64(f64),
    Utf8(String),
    Binary(Vec<u8>),
}
```
