# M4: Query Planner (Logical → Physical)

## Goal
Build a query planner that compiles Python DSL into physical execution DAG.

## Duration
4-6 days

## Deliverables
- Python DSL builder
- Logical plan IR
- Optimizer (basic rules)
- Physical plan mapping
- DAG execution graph

## Tasks

### 1. Python DSL Layer
- [ ] Define `Node` class for lazy expression graph
- [ ] Define `Stream` class (user-facing API)
- [ ] Implement `source()` function
- [ ] Implement `filter()` method
- [ ] Implement `map()` method
- [ ] Implement `group_by()` method
- [ ] Implement `window()` method
- [ ] Implement `agg()` method
- [ ] Implement `project()` method

### 2. Logical Plan IR
- [ ] Define `LogicalPlan` enum in Rust
- [ ] Implement `Source` node
- [ ] Implement `Filter` node
- [ ] Implement `Map` node
- [ ] Implement `GroupBy` node
- [ ] Implement `Window` node
- [ ] Implement `Aggregate` node
- [ ] Implement `Project` node
- [ ] Add schema propagation

### 3. Python → Logical Plan Compiler
- [ ] Implement recursive node traversal
- [ ] Add Python → Rust IR serialization
- [ ] Handle node type mapping
- [ ] Add error handling for invalid DAGs
- [ ] Validate plan structure

### 4. Logical Plan Optimizer
- [ ] Implement rule-based optimizer framework
- [ ] Add filter pushdown rule
- [ ] Add map fusion rule
- [ ] Add column pruning rule
- [ ] Add projection elimination rule
- [ ] Implement optimizer pipeline

### 5. Physical Plan IR
- [ ] Define `PhysicalPlan` enum
- [ ] Implement `ScanSource` operator
- [ ] Implement `Filter` operator (with compiled expr)
- [ ] Implement `Map` operator (with compiled expr)
- [ ] Implement `HashAggregate` operator
- [ ] Implement `WindowedAggregate` operator
- [ ] Implement `Project` operator

### 6. Logical → Physical Compiler
- [ ] Implement plan transformation
- [ ] Map logical nodes to physical operators
- [ ] Add expression compilation integration
- [ ] Handle operator ordering
- [ ] Validate physical plan

### 7. Execution Graph Builder
- [ ] Define `ExecNode` struct
- [ ] Build DAG from physical plan
- [ ] Add topological sort
- [ ] Implement node ID assignment
- [ ] Validate graph structure

### 8. Tests
- [ ] Test DSL construction
- [ ] Test logical plan compilation
- [ ] Test optimizer rules
- [ ] Test physical plan generation
- [ ] Test execution graph building
- [ ] End-to-end test: DSL → execution

## Implementation Details

### Python DSL
```python
class Node:
    def __init__(self, op, inputs=None, attrs=None):
        self.op = op
        self.inputs = inputs or []
        self.attrs = attrs or {}

class Stream:
    def __init__(self, node: Node):
        self.node = node
    
    def filter(self, expr: str):
        return Stream(Node("filter", [self.node], {"expr": expr}))
    
    def map(self, expr: str):
        return Stream(Node("map", [self.node], {"expr": expr}))
    
    def group_by(self, key: str):
        return Stream(Node("group_by", [self.node], {"key": key}))
    
    def window(self, spec: str):
        return Stream(Node("window", [self.node], {"spec": spec}))
    
    def agg(self, spec: dict):
        return Stream(Node("agg", [self.node], {"spec": spec}))

def source(name: str):
    return Stream(Node("source", [], {"name": name}))
```

### Logical Plan IR
```rust
pub enum LogicalPlan {
    Source { name: String },
    Filter {
        input: Box<LogicalPlan>,
        expr: Expr,
    },
    Map {
        input: Box<LogicalPlan>,
        expr: Expr,
    },
    GroupBy {
        input: Box<LogicalPlan>,
        key: String,
    },
    Window {
        input: Box<LogicalPlan>,
        spec: WindowSpec,
    },
    Aggregate {
        input: Box<LogicalPlan>,
        spec: AggSpec,
    },
    Project {
        input: Box<LogicalPlan>,
        columns: Vec<String>,
    },
}
```

### Optimizer
```rust
pub struct Optimizer {
    rules: Vec<Box<dyn OptimizerRule>>,
}

impl Optimizer {
    pub fn optimize(&self, plan: LogicalPlan) -> LogicalPlan {
        let mut plan = plan;
        for rule in &self.rules {
            plan = rule.apply(plan);
        }
        plan
    }
}

pub trait OptimizerRule {
    fn apply(&self, plan: LogicalPlan) -> LogicalPlan;
}
```

### Physical Plan
```rust
pub enum PhysicalPlan {
    ScanSource { name: String },
    Filter { expr: Arc<dyn PhysicalExpr> },
    Map { expr: Arc<dyn PhysicalExpr> },
    HashAggregate { key: String, agg: AggPlan },
    WindowedAggregate { window: WindowSpec, agg: AggPlan },
    Project { columns: Vec<usize> },
}
```

### Execution Graph
```rust
pub struct ExecNode {
    pub op: Box<dyn Operator>,
    pub inputs: Vec<NodeId>,
}

pub struct OperatorGraph {
    pub nodes: Vec<ExecNode>,
}
```

## Success Criteria
- [ ] Python DSL builds lazy expression graph
- [ ] Graph compiles to logical plan
- [ ] Optimizer applies rules correctly
- [ ] Logical plan transforms to physical plan
- [ ] Physical plan builds execution DAG
- [ ] End-to-end: DSL → executable graph
- [ ] All tests pass

## Example Pipeline
```python
stream = (
    source("kafka")
    .filter("temp > 20")
    .map("temp_f = temp * 1.8 + 32")
    .group_by("device_id")
    .window("10s")
    .agg({"temp_f": "avg"})
)
```

## Dependencies
- `arrow` >= 51
- `pyo3` >= 0.21
- Expression engine from M3

## Critical Design Rules
1. Plans must be immutable (no mutation after compilation)
2. Logical = what, Physical = how
3. Separate concerns: Python (DAG), Compiler (optimization), Rust (execution)
4. Schema must propagate through plan
