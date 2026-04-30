# M9: Web UI for Metrics/Job Inspection

## Goal
Provide a web-based interface for monitoring job execution, viewing metrics, and inspecting query plans in real-time.

## Duration
Medium

## Deliverables
- Metrics collection system
- Web dashboard for job inspection
- Real-time metrics streaming
- Query plan visualization
- Job history and logs

## Tasks

### 1. Metrics Collection System
- [ ] Define metrics schema (counters, gauges, histograms)
- [ ] Implement metrics registry in Rust
- [ ] Add instrumentation to operators
- [ ] Add instrumentation to connectors
- [ ] Implement metrics aggregation
- [ ] Add metrics export (Prometheus format)

### 2. Web Server
- [ ] Choose web framework (Axum/Actix-web)
- [ ] Implement HTTP/HTTPS server
- [ ] Add WebSocket support for real-time updates
- [ ] Implement REST API for metrics
- [ ] Add authentication/authorization
- [ ] Configure CORS for development

### 3. Job Inspection Dashboard
- [ ] Design dashboard layout
- [ ] Implement job list view
- [ ] Implement job detail view
- [ ] Add query plan visualization
- [ ] Add execution timeline view
- [ ] Add operator-level metrics

### 4. Real-time Metrics Streaming
- [ ] Implement WebSocket endpoint
- [ ] Add metrics push mechanism
- [ ] Implement client-side polling fallback
- [ ] Add real-time charts/graphs
- [ ] Implement alert system for anomalies

### 5. Query Plan Visualization
- [ ] Implement plan tree renderer
- [ ] Add interactive node inspection
- [ ] Show operator statistics
- [ ] Add plan optimization hints
- [ ] Implement plan comparison

### 6. Job History and Logs
- [ ] Implement job persistence
- [ ] Add log aggregation
- [ ] Implement log search/filter
- [ ] Add job replay capability
- [ ] Export job reports

## Implementation Details

### Metrics Schema
```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Metric {
    Counter {
        name: String,
        value: u64,
        labels: HashMap<String, String>,
    },
    Gauge {
        name: String,
        value: f64,
        labels: HashMap<String, String>,
    },
    Histogram {
        name: String,
        buckets: Vec<f64>,
        counts: Vec<u64>,
        labels: HashMap<String, String>,
    },
}
```

### Metrics Registry
```rust
pub struct MetricsRegistry {
    counters: RwLock<HashMap<String, AtomicU64>>,
    gauges: RwLock<HashMap<String, AtomicU64>>,
    histograms: RwLock<HashMap<String, Histogram>>,
}

impl MetricsRegistry {
    pub fn increment_counter(&self, name: &str, labels: HashMap<String, String>) {
        // Increment counter with labels
    }
    
    pub fn record_histogram(&self, name: &str, value: f64, labels: HashMap<String, String>) {
        // Record histogram value
    }
    
    pub fn export_prometheus(&self) -> String {
        // Export in Prometheus format
    }
}
```

### Web Server (Axum)
```rust
use axum::{
    routing::{get, post},
    Router,
    Json,
    response::Json as ResponseJson,
};

async fn get_metrics() -> Json<Vec<Metric>> {
    // Return current metrics
}

async fn get_jobs() -> Json<Vec<JobInfo>> {
    // Return job list
}

async fn get_job_detail(job_id: Uuid) -> Json<JobDetail> {
    // Return job details
}

pub fn create_router() -> Router {
    Router::new()
        .route("/api/metrics", get(get_metrics))
        .route("/api/jobs", get(get_jobs))
        .route("/api/jobs/:id", get(get_job_detail))
        .route("/ws/metrics", get(metrics_websocket))
}
```

### Dashboard UI (React + TypeScript)
```typescript
interface DashboardProps {
  jobs: JobInfo[];
  metrics: Metric[];
}

const Dashboard: React.FC<DashboardProps> = ({ jobs, metrics }) => {
  return (
    <div className="dashboard">
      <JobList jobs={jobs} />
      <MetricsPanel metrics={metrics} />
      <QueryPlanViewer />
    </div>
  );
};
```

### Query Plan Visualization
```rust
#[derive(Serialize)]
struct PlanNode {
    id: String,
    operator: String,
    metrics: OperatorMetrics,
    children: Vec<PlanNode>,
}

pub fn visualize_plan(plan: &PhysicalPlan) -> PlanNode {
    // Convert physical plan to visualizable tree
}
```

## Tech Stack

### Backend (Rust)
- **Web Framework**: Axum (async, performant, tokio-based)
- **WebSocket**: tokio-tungstenite
- **Serialization**: serde
- **Metrics**: prometheus-client (optional) or custom implementation

### Frontend
- **Framework**: React with TypeScript
- **Build Tool**: Vite
- **UI Library**: shadcn/ui + TailwindCSS
- **Charts**: Recharts or Chart.js
- **Real-time**: WebSocket client or polling

### Alternative: Single Binary with Embedded UI
- **Framework**: Tauri or Leptos (Rust + WASM)
- **Advantages**: Single binary, no separate frontend build
- **Trade-offs**: Less flexible UI, more complex build

## Success Criteria
- [ ] Metrics collection adds < 5% overhead
- [ ] Dashboard updates in real-time (< 100ms latency)
- [ ] Query plans visualized accurately
- [ ] Job history searchable and filterable
- [ ] Web server handles 100+ concurrent connections

## Performance Targets
- Metrics collection: < 1μs per operation
- Dashboard load time: < 2s
- WebSocket message latency: < 50ms
- API response time: < 100ms (p95)

## Dependencies
- `axum` for web server
- `tokio-tungstenite` for WebSockets
- `serde` for serialization
- `prometheus` (optional) for metrics export
- Frontend: React, TypeScript, Vite

## Critical Design Rules
1. Metrics collection must not impact query performance
2. Dashboard should work without JavaScript (progressive enhancement)
3. Real-time updates should gracefully degrade to polling
4. Sensitive data should be filtered from logs
5. Web server should be optional (feature flag)

## Future Extensions (Beyond M9)
- Distributed tracing integration (Jaeger, OpenTelemetry)
- Alerting and notifications
- Multi-tenant support
- Query editor with syntax highlighting
- Cost analysis and optimization suggestions
