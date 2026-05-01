# M6: Streaming Semantics

## Goal
Transform from batch system to streaming engine with watermarks, windows, and state management.

## Duration
6-10 days

## Deliverables
- Watermark system
- Event-time handling
- Window operators (tumbling, sliding, session)
- Late data handling
- State management (in-memory)

## Tasks

### 1. Watermark System
- [ ] Define `Watermark` struct
- [ ] Implement watermark calculation per source
- [ ] Add global watermark aggregation
- [ ] Implement watermark advancement logic
- [ ] Add watermark tracking and metrics

### 2. Event Time Handling
- [ ] Extract timestamp from events
- [ ] Handle missing timestamps
- [ ] Support multiple timestamp granularities
- [ ] Add timestamp validation
- [ ] Implement event-time vs processing-time separation

### 3. Window Operators
- [ ] Define `WindowSpec` enum (Tumbling, Sliding, Session)
- [ ] Implement tumbling window logic
- [ ] Implement sliding window logic
- [ ] Implement session window logic
- [ ] Add window size and slide configuration
- [ ] Handle window keying

### 4. Window State Management
- [ ] Define window state structure
- [ ] Implement in-memory state store
- [ ] Add state keying (window_key, group_key)
- [ ] Implement state expiration
- [ ] Add state size tracking

### 5. Windowed Aggregation
- [ ] Implement `WindowedAggregate` operator
- [ ] Support aggregations per window
- [ ] Handle early results (incremental)
- [ ] Handle final results (on window close)
- [ ] Add watermark-triggered flushing

### 6. Late Data Handling
- [ ] Define `LateDataPolicy` enum (Drop, Allow, SideOutput)
- [ ] Implement late data detection
- [ ] Add side output channel for late events
- [ ] Implement allowed lateness configuration
- [ ] Add late data metrics

### 7. State Snapshots (Basic)
- [ ] Implement state serialization
- [ ] Add checkpoint trigger logic
- [ ] Implement state restoration
- [ ] Add checkpoint directory configuration
- [ ] Implement incremental checkpointing

### 8. Trigger Policies
- [ ] Define `Trigger` enum (OnWatermark, OnTime, OnCount)
- [ ] Implement watermark-based triggers
- [ ] Implement time-based triggers
- [ ] Implement count-based triggers
- [ ] Add early firing support

### 9. Integration with Query Planner
- [ ] Add window operator to logical plan
- [ ] Add window operator to physical plan
- [ ] Integrate with DSL (`.window()`)
- [ ] Add window configuration validation
- [ ] Update optimizer for window-aware rules

### 10. Tests
- [ ] Unit test for watermark calculation
- [ ] Unit test for tumbling windows
- [ ] Unit test for sliding windows
- [ ] Unit test for session windows
- [ ] Unit test for late data handling
- [ ] Unit test for state management
- [ ] Integration test: end-to-end streaming pipeline
- [ ] Benchmark: windowing performance

## Implementation Details

### Watermark System
```rust
pub struct Watermark {
    pub timestamp: i64,
    pub source: String,
}

pub struct WatermarkTracker {
    pub per_source: HashMap<String, i64>,
    pub global: i64,
    pub max_lateness: i64,
}

impl WatermarkTracker {
    pub fn update(&mut self, source: &str, timestamp: i64) {
        self.per_source.insert(source.to_string(), timestamp);
        self.global = self.per_source.values().min().unwrap_or(&i64::MAX) - self.max_lateness;
    }
}
```

### Window Spec
```rust
pub enum WindowSpec {
    Tumbling {
        size: Duration,
    },
    Sliding {
        size: Duration,
        slide: Duration,
    },
    Session {
        gap: Duration,
    },
}
```

### Window State
```rust
pub struct WindowState {
    pub window_key: WindowKey,
    pub group_key: Option<String>,
    pub aggregates: HashMap<String, AggregateState>,
    pub count: usize,
}

pub struct WindowKey {
    pub start: i64,
    pub end: i64,
}
```

### Windowed Aggregate Operator
```rust
pub struct WindowedAggregate {
    pub window: WindowSpec,
    pub agg: AggSpec,
    pub state: HashMap<WindowKey, WindowState>,
    pub watermark: i64,
}

impl Operator for WindowedAggregate {
    fn process(&mut self, batch: RecordBatch) -> Result<Vec<RecordBatch>> {
        // 1. Assign events to windows
        // 2. Update window state
        // 3. Check for closed windows (watermark)
        // 4. Emit results for closed windows
    }
}
```

### Late Data Policy
```rust
pub enum LateDataPolicy {
    Drop,
    Allow,
    SideOutput,
}

pub fn handle_late_event(event: &Event, watermark: i64, policy: &LateDataPolicy) -> Result<()> {
    if event.timestamp < watermark {
        match policy {
            LateDataPolicy::Drop => Ok(()),
            LateDataPolicy::Allow => Ok(()),
            LateDataPolicy::SideOutput => send_to_side_output(event),
        }
    } else {
        Ok(())
    }
}
```

### Trigger Policy
```rust
pub enum Trigger {
    OnWatermark,
    OnTime(Duration),
    OnCount(usize),
    EarlyAndLate {
        early: Trigger,
        late: Trigger,
    },
}
```

## Success Criteria
- [ ] Watermarks calculated correctly per source
- [ ] Global watermark aggregation works
- [ ] Tumbling windows group events correctly
- [ ] Sliding windows with overlap work
- [ ] Session windows handle gaps correctly
- [ ] Late data handled according to policy
- [ ] State managed and expired correctly
- [ ] Windows close on watermark advancement
- [ ] Integration with DSL works
- [ ] All tests pass

## Performance Targets
- Window assignment: > 500k events/sec
- State updates: < 1μs per event
- Window close latency: < 10ms after watermark
- Memory overhead: < 2x input data

## Example Pipeline
```python
stream = (
    source("kafka")
    .group_by("device_id")
    .window("10s")  # Tumbling window
    .agg({"temperature": "avg", "humidity": "max"})
)
```

## Dependencies
- Query planner from M4
- Expression engine from M3
- Sequencer from M1

## Critical Design Rules
1. Event time only (no processing time for ordering)
2. Watermarks are monotonic
3. Windows are closed by watermark, not time
4. State is keyed by (window_key, group_key)
5. Late data policy is configurable
6. State must fit in memory (M6 limitation)

## Future Extensions (Beyond M6)
- Distributed state store (RocksDB, Redis)
- Exactly-once semantics with checkpoints
- Custom window functions
- Window merging (session windows)
