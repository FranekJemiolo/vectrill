//! Spreadsheet API Server Binary
//!
//! Provides HTTP API endpoints for spreadsheet integration with Vectrill.

use vectrill::metrics::global_registry;
use vectrill::web::run_server;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize metrics registry
    let registry = Arc::new(global_registry());
    
    // Default to port 8080
    let addr = std::env::var("VECTRILL_API_PORT")
        .unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    
    println!("Starting Vectrill Spreadsheet API Server on {}", addr);
    
    // Run the web server
    run_server(&addr, registry).await
}
