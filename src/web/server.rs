//! Web server implementation using Axum

use crate::metrics::MetricsRegistry;
use axum::{
    extract::{ws::Message, Path, State, WebSocketUpgrade},
    response::{Html, IntoResponse, Json},
    routing::get,
    Router,
};
use futures::stream::StreamExt;
use serde::Serialize;
use std::sync::Arc;
use tokio::time::{interval, Duration};
use tower_http::cors::{Any, CorsLayer};

/// Metrics response
#[derive(Debug, Serialize)]
struct MetricsResponse {
    metrics: Vec<crate::metrics::Metric>,
}

/// Health check response
#[derive(Debug, Serialize)]
struct HealthResponse {
    status: String,
    version: String,
}

/// Job detail response
#[derive(Debug, Serialize)]
struct JobDetail {
    id: String,
    status: String,
    created_at: String,
    query_plan: Option<QueryPlanNode>,
}

/// Query plan node
#[derive(Debug, Serialize)]
struct QueryPlanNode {
    operator: String,
    children: Option<Vec<QueryPlanNode>>,
}

/// Job info (placeholder for now)
#[derive(Debug, Serialize)]
struct JobInfo {
    id: String,
    status: String,
    created_at: String,
}

/// Create the web router
pub fn create_router(registry: Arc<MetricsRegistry>) -> Router {
    Router::new()
        .route("/", get(dashboard))
        .route("/health", get(health_check))
        .route("/api/metrics", get(get_metrics))
        .route("/api/jobs", get(list_jobs))
        .route("/api/jobs/:id", get(get_job_detail))
        .route("/ws/metrics", get(metrics_websocket))
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .with_state(registry)
}

/// Dashboard endpoint
async fn dashboard() -> Html<&'static str> {
    Html(include_str!("static/index.html"))
}

/// Health check endpoint
async fn health_check() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Get metrics endpoint
async fn get_metrics(State(registry): State<Arc<MetricsRegistry>>) -> Json<MetricsResponse> {
    let metrics = registry.get_metrics().await;
    Json(MetricsResponse { metrics })
}

/// List jobs endpoint (placeholder)
async fn list_jobs() -> Json<Vec<JobInfo>> {
    Json(vec![])
}

/// Get job detail endpoint (placeholder)
async fn get_job_detail(Path(id): Path<String>) -> Json<JobDetail> {
    Json(JobDetail {
        id,
        status: "completed".to_string(),
        created_at: chrono::Utc::now().to_rfc3339(),
        query_plan: Some(QueryPlanNode {
            operator: "Scan".to_string(),
            children: Some(vec![QueryPlanNode {
                operator: "Filter".to_string(),
                children: Some(vec![QueryPlanNode {
                    operator: "Map".to_string(),
                    children: Some(vec![QueryPlanNode {
                        operator: "Aggregate".to_string(),
                        children: None,
                    }]),
                }]),
            }]),
        }),
    })
}

/// WebSocket endpoint for real-time metrics streaming
async fn metrics_websocket(
    ws: WebSocketUpgrade,
    State(registry): State<Arc<MetricsRegistry>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_metrics_websocket(socket, registry))
}

async fn handle_metrics_websocket(
    mut socket: axum::extract::ws::WebSocket,
    registry: Arc<MetricsRegistry>,
) {
    let mut ticker = interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let metrics = registry.get_metrics().await;
                let json = serde_json::to_string(&metrics).unwrap_or_else(|_| "[]".to_string());

                if socket.send(Message::Text(json)).await.is_err() {
                    break;
                }
            }
            msg = socket.next() => {
                match msg {
                    Some(Ok(Message::Close(_))) => {
                        break;
                    }
                    Some(Ok(_)) => {
                        // Ignore other messages
                    }
                    Some(Err(_)) | None => {
                        break;
                    }
                }
            }
        }
    }
}

/// Run the web server
pub async fn run_server(
    addr: &str,
    registry: Arc<MetricsRegistry>,
) -> Result<(), Box<dyn std::error::Error>> {
    let app = create_router(registry);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("Web server listening on {}", addr);

    axum::serve(listener, app).await?;
    Ok(())
}
