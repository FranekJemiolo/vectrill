# M5: Operator Fusion

## Goal
Eliminate unnecessary batch materialization by fusing stateless operators.

## Duration
5-8 days

## Deliverables
- Fusion planner (segment builder)
- Fused operator implementation
- Expression tree merging
- Column pruning, predicate pushdown
- Measurable performance improvement

## Tasks

### 1. Operator Categorization
- [ ] Define fusable operator trait
- [ ] Identify stateless operators (Filter, Map, Project, Cast)
- [ ] Identify stateful operators (GroupBy, Window, Join, Sort)
- [ ] Identify boundary operators (Source, Sink)
- [ ] Document fusion rules

### 2. Fusion Segment Builder
- [ ] Implement DAG traversal algorithm
- [ ] Build fusion segments from physical plan
- [ ] Handle segment boundaries at stateful operators
- [ ] Validate segment structure
- [ ] Add segment serialization

### 3. Fused Operator Design
- [ ] Define `FusedOperator` struct
- [ ] Design expression DAG representation
- [ ] Add predicate field (optional)
- [ ] Add projection field
- [ ] Design execution pipeline

### 4. Expression Merging
- [ ] Implement expression tree combination
- [ ] Handle map chaining (map(a) → map(b) → fused expr)
- [ ] Handle predicate + map fusion
- [ ] Handle projection + predicate fusion
- [ ] Add expression dependency analysis

### 5. Column Pruning
- [ ] Implement column usage analysis
- [ ] Track which columns are needed
- [ ] Remove unused columns early
- [ ] Update expression trees for pruned columns
- [ ] Validate pruning correctness

### 6. Predicate Pushdown
- [ ] Implement predicate movement analysis
- [ ] Push predicates toward sources
- [ ] Combine multiple predicates
- [ ] Handle predicate ordering
- [ ] Validate pushdown correctness

### 7. Fused Operator Execution
- [ ] Implement single-pass evaluation
- [ ] Execute expressions in dependency order
- [ ] Apply predicate filtering
- [ ] Apply projection
- [ ] Minimize intermediate allocations

### 8. Buffer Reuse
- [ ] Implement buffer pooling
- [ ] Reuse Arrow arrays where possible
- [ ] Minimize allocations in hot path
- [ ] Track buffer lifetimes
- [ ] Add memory reuse metrics

### 9. Common Subexpression Elimination
- [ ] Identify duplicate subexpressions
- [ ] Build expression DAG (not tree)
- [ ] Cache intermediate results
- [ ] Reuse computed arrays
- [ ] Validate CSE correctness

### 10. Tests
- [ ] Test fusion segment building
- [ ] Test expression merging
- [ ] Test column pruning
- [ ] Test predicate pushdown
- [ ] Test fused operator execution
- [ ] Benchmark: fused vs non-fused performance
- [ ] Validate semantic equivalence

## Implementation Details

### Fusable Operator Trait
```rust
pub trait FusableOperator {
    fn expressions(&self) -> Vec<&Expr>;
    fn predicate(&self) -> Option<&Expr>;
    fn projection(&self) -> Option<&[usize]>;
}
```

### Fusion Segment
```rust
pub struct FusionSegment {
    pub operators: Vec<Box<dyn FusableOperator>>,
    pub boundary: Option<Box<dyn Operator>>,
}
```

### Fused Operator
```rust
pub struct FusedOperator {
    pub expressions: Vec<FusedExpr>,
    pub predicate: Option<Expr>,
    pub projection: Vec<usize>,
}

impl Operator for FusedOperator {
    fn process(&mut self, batch: RecordBatch) -> Result<RecordBatch> {
        // 1. Evaluate expressions in dependency order
        let computed = self.eval_all(&batch);
        
        // 2. Apply predicate
        let filtered = if let Some(pred) = &self.predicate {
            self.apply_predicate(&computed, pred)?
        } else {
            computed
        };
        
        // 3. Apply projection
        self.apply_projection(filtered)
    }
}
```

### Expression DAG
```rust
pub struct ExprDAG {
    pub nodes: Vec<ExprNode>,
    pub edges: Vec<(usize, usize)>, // (from, to)
}

pub struct ExprNode {
    pub expr: Expr,
    pub id: usize,
}
```

### Column Pruning
```rust
pub fn prune_columns(plan: &PhysicalPlan, needed_columns: &HashSet<String>) -> PhysicalPlan {
    // Remove columns not in needed_columns
    // Update expressions to use pruned schema
}
```

### Predicate Pushdown
```rust
pub fn push_down_predicates(plan: PhysicalPlan) -> PhysicalPlan {
    // Move predicates toward sources
    // Combine predicates at same level
}
```

### Buffer Reuse
```rust
pub struct BufferPool {
    buffers: HashMap<DataType, Vec<ArrayRef>>,
}

impl BufferPool {
    pub fn get(&mut self, dtype: &DataType, capacity: usize) -> ArrayRef;
    pub fn return_buffer(&mut self, buffer: ArrayRef);
}
```

## Success Criteria
- [ ] Fusion segments built correctly
- [ ] Expressions merged without semantic changes
- [ ] Column pruning removes unused columns
- [ ] Predicates pushed to optimal positions
- [ ] Fused operator executes correctly
- [ ] Benchmarks show 2-5x performance improvement
- [ ] Memory allocations reduced significantly
- [ ] All tests pass

## Performance Targets
- Throughput improvement: 2-5x vs non-fused
- Allocation reduction: > 50%
- Kernel call reduction: > 60%
- Cache locality improved

## Example Transformation

### Before (Non-fused)
```
Source → Filter → Map → Map → Project → Aggregate
```

### After (Fused)
```
Source → FusedOperator(
    predicate: temp > 20,
    expr: temp_f = temp * 1.8 + 32,
    projection: [device_id, temp_f]
) → Aggregate
```

## Dependencies
- Query planner from M4
- Expression engine from M3
- Arrow compute kernels

## Critical Design Rules
1. Only fuse stateless operators
2. Never fuse across stateful boundaries
3. Preserve semantic equivalence
4. Avoid over-fusing (huge expressions → register pressure)
5. Respect memory layout (Arrow alignment)
6. Handle nulls correctly in fused predicates

## Pitfalls to Avoid
- **Over-fusing**: Large expressions can hurt performance due to register pressure
- **Breaking semantics**: Especially null handling and short-circuit logic
- **Ignoring memory layout**: Arrow arrays must stay aligned
- **Fusing across watermarks**: Unsafe for streaming semantics
