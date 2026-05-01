# M3: Expression Engine

## Goal
Build a vectorized expression compiler and execution engine using Arrow kernels.

## Duration
4-6 days

## Deliverables
- Python AST → IR compiler
- Rust expression evaluator
- Arrow kernel integration
- Column refs, literals, binary ops, boolean logic

## Tasks

### 1. Expression IR
- [ ] Define `Expr` enum (Column, Literal, Binary, Unary, Function)
- [ ] Define `Operator` enum (Eq, Lt, Gt, Add, Sub, And, Or, etc.)
- [ ] Define `UnaryOp` enum (Not, Neg)
- [ ] Define `ScalarValue` enum
- [ ] Add serialization for Python ↔ Rust transfer

### 2. Python AST Compiler
- [ ] Implement Python AST parser
- [ ] Create AST → IR mapping
- [ ] Handle Name nodes (column references)
- [ ] Handle Constant nodes (literals)
- [ ] Handle BinOp nodes (binary operations)
- [ ] Handle BoolOp nodes (boolean logic)
- [ ] Handle Compare nodes (comparisons)
- [ ] Add error handling for unsupported constructs

### 3. Type Inference
- [ ] Implement type annotation for expressions
- [ ] Add implicit casting rules (int → float, scalar → column)
- [ ] Handle null types
- [ ] Validate expression types before execution

### 4. Physical Expression Evaluator
- [ ] Define `PhysicalExpr` trait
- [ ] Implement `ColumnExpr` (column access)
- [ ] Implement `LiteralExpr` (constant values)
- [ ] Implement `BinaryExpr` (binary operations)
- [ ] Implement `UnaryExpr` (unary operations)
- [ ] Add Arrow kernel integration

### 5. Arrow Kernel Integration
- [ ] Integrate comparison kernels (gt, lt, eq, etc.)
- [ ] Integrate arithmetic kernels (add, sub, mul, div)
- [ ] Integrate boolean kernels (and, or, not)
- [ ] Handle type casting
- [ ] Handle null propagation

### 6. Expression Optimization
- [ ] Implement constant folding
- [ ] Implement expression simplification (x AND true → x)
- [ ] Add common subexpression elimination (basic)
- [ ] Add predicate normalization

### 7. Filter Operator
- [ ] Implement `FilterOperator` using expression engine
- [ ] Integrate with Arrow compute::filter
- [ ] Add predicate evaluation
- [ ] Handle null values in predicates

### 8. Map Operator
- [ ] Implement `MapOperator` using expression engine
- [ ] Support computed columns
- [ ] Handle multiple expressions
- [ ] Integrate with Arrow compute kernels

### 9. Tests
- [ ] Unit test for expression compilation
- [ ] Unit test for type inference
- [ ] Unit test for expression evaluation
- [ ] Unit test for constant folding
- [ ] Python test for filter operator
- [ ] Python test for map operator
- [ ] Benchmark expression evaluation

## Implementation Details

### Expression IR
```rust
pub enum Expr {
    Column(String),
    Literal(ScalarValue),
    Binary {
        left: Box<Expr>,
        op: Operator,
        right: Box<Expr>,
    },
    Unary {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Function {
        name: String,
        args: Vec<Expr>,
    },
}
```

### Physical Expression Trait
```rust
pub trait PhysicalExpr: Send + Sync {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef>;
    fn data_type(&self) -> &DataType;
}
```

### Binary Expression Execution
```rust
impl PhysicalExpr for BinaryExpr {
    fn evaluate(&self, batch: &RecordBatch) -> Result<ArrayRef> {
        let left = self.left.evaluate(batch)?;
        let right = self.right.evaluate(batch)?;
        
        match self.op {
            Operator::Gt => arrow::compute::gt(&left, &right),
            Operator::Lt => arrow::compute::lt(&left, &right),
            Operator::Add => arrow::compute::add(&left, &right),
            Operator::And => arrow::compute::and(&left, &right),
            _ => unimplemented!(),
        }
    }
}
```

### Python AST Compiler
```python
def compile_expr(node):
    if isinstance(node, ast.Name):
        return {"type": "column", "name": node.id}
    
    if isinstance(node, ast.Constant):
        return {"type": "literal", "value": node.value}
    
    if isinstance(node, ast.BinOp):
        return {
            "type": "binary",
            "op": map_op(node.op),
            "left": compile_expr(node.left),
            "right": compile_expr(node.right),
        }
    
    if isinstance(node, ast.BoolOp):
        return {
            "type": "binary",
            "op": map_bool_op(node.op),
            "left": compile_expr(node.values[0]),
            "right": compile_expr(node.values[1]),
        }
```

### Filter Execution
```rust
let mask = predicate.evaluate(batch)?;
let result = arrow::compute::filter(batch, &mask)?;
```

## Success Criteria
- [ ] Python expressions compile to Rust IR
- [ ] Expressions evaluate with Arrow kernels
- [ ] Filter and map operators work correctly
- [ ] Type inference prevents runtime errors
- [ ] Constant folding reduces computation
- [ ] All tests pass
- [ ] Benchmarks show vectorized performance

## Performance Targets
- Expression evaluation: > 1M rows/sec
- Filter operation: < 5ms per 100k rows
- Zero intermediate allocations where possible

## Dependencies
- `arrow` >= 51
- `pyo3` >= 0.21
- Python `ast` module

## Critical Design Rules
1. No row-by-row evaluation (must be vectorized)
2. No Python in hot path (compile before execution)
3. Types resolved before execution (no dynamic typing at runtime)
4. Use Arrow compute kernels (not custom loops)
