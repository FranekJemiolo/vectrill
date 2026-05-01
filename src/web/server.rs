//! Web server implementation using Axum

use crate::metrics::MetricsRegistry;
use serde::Serialize;
use std::sync::Arc;

#[cfg(any(feature = "web-ui", feature = "spreadsheet"))]
use axum::{
    extract::{ws::Message, Path, State, WebSocketUpgrade},
    response::{Html, IntoResponse, Json},
    routing::{get, post},
    Router,
};

#[cfg(any(feature = "web-ui", feature = "spreadsheet"))]
use futures::stream::StreamExt;

#[cfg(any(feature = "web-ui", feature = "spreadsheet"))]
use tokio::time::{interval, Duration};

#[cfg(any(feature = "web-ui", feature = "spreadsheet"))]
use tower_http::cors::{Any, CorsLayer};

#[cfg(feature = "spreadsheet")]
use crate::spreadsheet::api::{OperationType, SpreadsheetRequest, SpreadsheetResponse};

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
#[cfg(any(feature = "web-ui", feature = "spreadsheet"))]
pub fn create_router(registry: Arc<MetricsRegistry>) -> Router {
    let mut router = Router::new()
        .route("/", get(dashboard))
        .route("/health", get(health_check))
        .route("/api/metrics", get(get_metrics))
        .route("/api/jobs", get(list_jobs))
        .route("/api/jobs/:id", get(get_job_detail))
        .route("/ws/metrics", get(metrics_websocket));

    // Add spreadsheet API endpoints if spreadsheet feature is enabled
    #[cfg(feature = "spreadsheet")]
    {
        router = router
            .route("/api/v1/spreadsheet/transform", post(spreadsheet_transform))
            .route("/api/v1/spreadsheet/validate", post(spreadsheet_validate))
            .route("/api/v1/spreadsheet/preview", post(spreadsheet_preview))
            .route("/api/v1/spreadsheet/templates", get(spreadsheet_templates))
            .route(
                "/api/v1/spreadsheet/transformations",
                get(spreadsheet_transformations),
            );
    }

    router
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .with_state(registry)
}

/// Dashboard endpoint
#[cfg(any(feature = "web-ui", feature = "spreadsheet"))]
async fn dashboard() -> Html<&'static str> {
    Html(include_str!("static/index.html"))
}

/// Health check endpoint
#[cfg(any(feature = "web-ui", feature = "spreadsheet"))]
async fn health_check() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "healthy".to_string(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    })
}

/// Get metrics endpoint
#[cfg(any(feature = "web-ui", feature = "spreadsheet"))]
async fn get_metrics(State(registry): State<Arc<MetricsRegistry>>) -> Json<MetricsResponse> {
    let metrics = registry.get_metrics().await;
    Json(MetricsResponse { metrics })
}

/// List jobs endpoint (placeholder)
#[cfg(any(feature = "web-ui", feature = "spreadsheet"))]
async fn list_jobs() -> Json<Vec<JobInfo>> {
    Json(vec![])
}

/// Get job detail endpoint (placeholder)
#[cfg(any(feature = "web-ui", feature = "spreadsheet"))]
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
#[cfg(any(feature = "web-ui", feature = "spreadsheet"))]
async fn metrics_websocket(
    ws: WebSocketUpgrade,
    State(registry): State<Arc<MetricsRegistry>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_metrics_websocket(socket, registry))
}

#[cfg(any(feature = "web-ui", feature = "spreadsheet"))]
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

/// Spreadsheet transform endpoint
#[cfg(feature = "spreadsheet")]
async fn spreadsheet_transform(
    Json(request): Json<SpreadsheetRequest>,
) -> Json<SpreadsheetResponse> {
    // For now, return a mock response
    Json(SpreadsheetResponse {
        request_id: request.request_id.clone(),
        success: true,
        data: Some(request.data.clone()),
        error: None,
        metadata: None,
    })
}

/// Spreadsheet validate endpoint
#[cfg(feature = "spreadsheet")]
async fn spreadsheet_validate(
    Json(request): Json<SpreadsheetRequest>,
) -> Json<SpreadsheetResponse> {
    // For now, return validation success
    Json(SpreadsheetResponse {
        request_id: request.request_id.clone(),
        success: true,
        data: None,
        error: None,
        metadata: None,
    })
}

/// Spreadsheet preview endpoint
#[cfg(feature = "spreadsheet")]
async fn spreadsheet_preview(Json(request): Json<SpreadsheetRequest>) -> Json<SpreadsheetResponse> {
    // For now, return preview of data
    Json(SpreadsheetResponse {
        request_id: request.request_id.clone(),
        success: true,
        data: Some(request.data.clone()),
        error: None,
        metadata: None,
    })
}

/// Spreadsheet templates endpoint
#[cfg(feature = "spreadsheet")]
async fn spreadsheet_templates() -> Json<serde_json::Value> {
    // For now, return mock templates
    Json(serde_json::json!({
        "templates": [
            {
                "name": "remove_duplicates",
                "description": "Remove duplicate rows",
                "category": "data_cleaning"
            },
            {
                "name": "filter_data",
                "description": "Filter data by conditions",
                "category": "data_filtering"
            }
        ]
    }))
}

/// Spreadsheet transformations endpoint
#[cfg(feature = "spreadsheet")]
async fn spreadsheet_transformations() -> Json<serde_json::Value> {
    // For now, return available transformations
    Json(serde_json::json!({
        "transformations": [
            {
                "type": "filter",
                "name": "Filter",
                "description": "Filter rows by conditions"
            },
            {
                "type": "map",
                "name": "Map",
                "description": "Transform columns"
            },
            {
                "type": "aggregate",
                "name": "Aggregate",
                "description": "Aggregate data"
            }
        ]
    }))
}

/// Run the web server
#[cfg(any(feature = "web-ui", feature = "spreadsheet"))]
pub async fn run_server(
    addr: &str,
    registry: Arc<MetricsRegistry>,
) -> Result<(), Box<dyn std::error::Error>> {
    let app = create_router(registry);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    println!("Web server listening on {}", addr);

    #[cfg(any(feature = "web-ui", feature = "spreadsheet"))]
    {
        axum::serve(listener, app).await?;
    }
    Ok(())
}
