# M8: Performance + Advanced Features

## Goal
Make the engine competitive with advanced optimizations and optional high-performance features.

## Duration
Ongoing

## Deliverables
- Expression optimization (constant folding, CSE)
- Fusion improvements (DAG-level, kernel batching)
- Memory optimization (buffer reuse, allocation pooling)
- Optional JIT/codegen
- SIMD specialization

## Tasks

### 1. Expression Optimization
- [ ] Implement advanced constant folding
- [ ] Implement common subexpression elimination (CSE)
- [ ] Add expression canonicalization
- [ ] Implement predicate simplification
- [ ] Add expression cost modeling
- [ ] Implement expression reordering based on cost

### 2. DAG-Level Fusion
- [ ] Extend fusion to handle DAGs (not just chains)
- [ ] Implement cross-branch fusion
- [ ] Add fusion decision heuristics
- [ ] Implement fusion size limits
- [ ] Add fusion cost-benefit analysis

### 3. Kernel Batching
- [ ] Group similar kernel operations
- [ ] Reduce kernel dispatch overhead
- [ ] Implement batched Arrow compute calls
- [ ] Add kernel fusion for arithmetic
- [ ] Optimize boolean mask operations

### 4. Memory Optimization
- [ ] Implement buffer pooling
- [ ] Add arena allocator for arrays
- [ ] Implement memory reuse for intermediates
- [ ] Add memory tracking and metrics
- [ ] Implement zero-copy projections

### 5. SIMD Specialization
- [ ] Identify SIMD-friendly operations
- [ ] Implement SIMD-optimized kernels
- [ ] Add CPU feature detection
- [ ] Implement vectorized string operations
- [ ] Add SIMD-optimized aggregations

### 6. Adaptive Execution
- [ ] Implement cost-based optimizer
- [ ] Add runtime statistics collection
- [ ] Implement adaptive batch sizing
- [ ] Add adaptive fusion decisions
- [ ] Implement adaptive parallelism

### 7. JIT Compilation (Optional)
- [ ] Evaluate JIT options (LLVM, Cranelift)
- [ ] Implement expression JIT
- [ ] Add JIT code caching
- [ ] Implement JIT fallback
- [ ] Add JIT warmup

### 8. Parallel Execution
- [ ] Implement multi-threaded operator execution
- [ ] Add work-stealing scheduler
- [ ] Implement parallel aggregation
- [ ] Add partition-aware execution
- [ ] Implement parallel windowing

### 9. Caching
- [ ] Implement result caching
- [ ] Add query plan caching
- [ ] Implement compiled expression caching
- [ ] Add data page caching
- [ ] Implement metadata caching

### 10. Monitoring and Profiling
- [ ] Add performance counters
- [ ] Implement flame graph support
- [ ] Add memory profiling
- [ ] Implement query latency tracking
- [ ] Add operator-level metrics

### 11. Benchmarks
- [ ] Create comprehensive benchmark suite
- [ ] Benchmark vs Polars (for comparison)
- [ ] Benchmark vs PySpark (for comparison)
- [ ] Benchmark vs Flink (for comparison)
- [ ] Add regression tests

## Implementation Details

### Advanced CSE
```rust
pub struct CSEOptimizer {
    cache: HashMap<Expr, Expr>,
}

impl CSEOptimizer {
    pub fn optimize(&mut self, expr: Expr) -> Expr {
        if let Some(cached) = self.cache.get(&expr) {
            return cached.clone();
        }
        
        let optimized = match expr {
            Expr::Binary { left, op, right } => {
                Expr::Binary {
                    left: Box::new(self.optimize(*left)),
                    op,
                    right: Box::new(self.optimize(*right)),
                }
            }
            _ => expr,
        };
        
        self.cache.insert(optimized.clone(), optimized.clone());
        optimized
    }
}
```

### Buffer Pool
```rust
pub struct BufferPool {
    pools: HashMap<DataType, Vec<ArrayRef>>,
    max_size_per_pool: usize,
}

impl BufferPool {
    pub fn get(&mut self, dtype: &DataType, capacity: usize) -> ArrayRef {
        let pool = self.pools.entry(dtype.clone()).or_insert_with(Vec::new);
        pool.pop()
            .filter(|arr| arr.len() >= capacity)
            .unwrap_or_else(|| make_array(dtype, capacity))
    }
    
    pub fn return_buffer(&mut self, buffer: ArrayRef) {
        if self.pools.len() < self.max_size_per_pool {
            self.pools.entry(buffer.data_type().clone())
                .or_insert_with(Vec::new)
                .push(buffer);
        }
    }
}
```

### SIMD Specialization
```rust
#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

pub fn simd_add_f64(left: &[f64], right: &[f64], out: &mut [f64]) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        let chunks = left.len() / 4;
        for i in 0..chunks {
            let a = _mm256_loadu_pd(left.get_unchecked(i * 4..));
            let b = _mm256_loadu_pd(right.get_unchecked(i * 4..));
            let c = _mm256_add_pd(a, b);
            _mm256_storeu_pd(out.get_unchecked_mut(i * 4..), c);
        }
    }
}
```

### Adaptive Batch Sizing
```rust
pub struct AdaptiveBatchSizer {
    target_latency: Duration,
    current_size: usize,
    history: VecDeque<(usize, Duration)>,
}

impl AdaptiveBatchSizer {
    pub fn adjust(&mut self, latency: Duration) {
        self.history.push_back((self.current_size, latency));
        if self.history.len() > 10 {
            self.history.pop_front();
        }
        
        let avg_latency: Duration = self.history.iter()
            .map(|(_, l)| *l)
            .sum::<Duration>() / self.history.len() as u32;
        
        if avg_latency > self.target_latency {
            self.current_size = (self.current_size * 9) / 10; // Reduce by 10%
        } else {
            self.current_size = (self.current_size * 11) / 10; // Increase by 10%
        }
    }
}
```

### Cost-Based Optimizer
```rust
pub struct CostBasedOptimizer {
    statistics: Statistics,
}

impl CostBasedOptimizer {
    pub fn estimate_cost(&self, plan: &PhysicalPlan) -> Cost {
        match plan {
            PhysicalPlan::Filter { expr } => {
                let selectivity = self.estimate_selectivity(expr);
                Cost::new(self.input_rows * selectivity)
            }
            PhysicalPlan::HashAggregate { key, agg } => {
                let groups = self.estimate_distinct(key);
                Cost::new(groups * agg.cost_per_group())
            }
            _ => Cost::default(),
        }
    }
}
```

## Success Criteria
- [ ] Expression optimization reduces computation
- [ ] CSE eliminates duplicate work
- [ ] DAG-level fusion improves performance
- [ ] Kernel batching reduces overhead
- [ ] Buffer pooling reduces allocations
- [ ] SIMD operations improve vectorized performance
- [ ] Adaptive execution responds to workload
- [ ] Benchmarks show competitive performance
- [ ] Monitoring provides actionable insights

## Performance Targets
- Throughput: > 1M rows/sec (overall)
- Latency: < 10ms (micro-batch)
- Memory overhead: < 1.2x input
- Allocation reduction: > 70% vs baseline
- SIMD speedup: 2-4x for supported operations

## Benchmark Suite

### TPC-H Queries
- Q1: Pricing Summary Report
- Q3: Shipping Priority
- Q5: Local Supplier Volume
- Q6: Forecasting Revenue Change

### Streaming Benchmarks
- Event ordering with multiple sources
- Windowed aggregation with sliding windows
- Session windowing with gaps
- Late data handling

### Comparison Benchmarks
- Polars (for batch operations)
- PySpark (for API comparison)
- Apache Flink (for streaming semantics)

## Dependencies
- `criterion` for benchmarking
- `pprof` for profiling
- `SIMD` crates (optional)
- `LLVM` or `Cranelift` (for JIT, optional)

## Critical Design Rules
1. Measure before optimizing
2. Don't optimize for microbenchmarks only
3. Profile real workloads
4. Keep code maintainable
5. Add fallbacks for advanced features
6. Document optimization decisions

## Future Extensions (Beyond M8)
- GPU acceleration (CUDA, ROCm)
- Distributed execution
- Persistent state stores
- Advanced query optimization (join reordering, etc.)
- Machine learning-based optimization
