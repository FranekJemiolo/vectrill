//! Real-time Processing for Spreadsheet Integration
//!
//! Provides real-time data processing capabilities for spreadsheet applications,
//! allowing users to stream data transformations and get live updates.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;
use tokio::time::{interval, sleep};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::error::{Result, VectrillError};

/// Real-time processor for spreadsheet data
pub struct RealTimeProcessor {
    /// Vectrill API client
    api_client: Arc<Mutex<VectrillApiClient>>,
    /// WebSocket connection for real-time updates
    websocket: Option<tokio_tungstenite::WebSocketStream<tokio_tungstenite::tungstenite::TungsteniteStream>>,
    /// Processing configuration
    config: RealTimeConfig,
    /// Active transformation pipelines
    pipelines: HashMap<String, TransformationPipeline>,
    /// Update channel for subscribers
    update_sender: mpsc::UnboundedSender<RealTimeUpdate>,
}

/// Configuration for real-time processing
#[derive(Debug, Clone)]
pub struct RealTimeConfig {
    /// Enable real-time processing
    pub enabled: bool,
    /// Update frequency in milliseconds
    pub update_frequency_ms: u64,
    /// Batch size for processing
    pub batch_size: usize,
    /// Maximum number of concurrent pipelines
    pub max_pipelines: usize,
    /// WebSocket server URL
    pub websocket_url: String,
    /// Authentication token
    pub auth_token: Option<String>,
}

impl Default for RealTimeConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            update_frequency_ms: 1000, // 1 second
            batch_size: 1000,
            max_pipelines: 5,
            websocket_url: "ws://localhost:8080/ws".to_string(),
            auth_token: None,
        }
    }
}

/// Real-time update message
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RealTimeUpdate {
    /// Update ID
    pub id: String,
    /// Update type
    pub update_type: RealTimeUpdateType,
    /// Update timestamp
    pub timestamp: u64,
    /// Update data
    pub data: serde_json::Value,
    /// Source of update
    pub source: String,
}

/// Types of real-time updates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RealTimeUpdateType {
    /// Data transformation update
    TransformationUpdate,
    /// Cell value change
    CellChange,
    /// New row added
    RowAdded,
    /// Row deleted
    RowDeleted,
    /// Column added
    ColumnAdded,
    /// Column deleted
    ColumnDeleted,
    /// Processing status update
    StatusUpdate,
}

/// Transformation update data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformationUpdate {
    /// Pipeline ID
    pub pipeline_id: String,
    /// Transformation step
    pub step_index: usize,
    /// Step status
    pub status: TransformationStepStatus,
    /// Progress percentage (0-100)
    pub progress: f64,
    /// Processed row count
    pub processed_rows: usize,
    /// Total row count
    pub total_rows: usize,
    /// Error message if any
    pub error: Option<String>,
}

/// Transformation step status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransformationStepStatus {
    /// Step is pending
    Pending,
    /// Step is running
    Running,
    /// Step completed successfully
    Completed,
    /// Step failed
    Failed,
}

/// Cell change data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CellChange {
    /// Cell reference (e.g., "A1")
    pub cell_reference: String,
    /// Old value
    pub old_value: serde_json::Value,
    /// New value
    pub new_value: serde_json::Value,
    /// Change type
    pub change_type: CellChangeType,
}

/// Cell change type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CellChangeType {
    /// Value was edited
    Edited,
    /// Value was added
    Added,
    /// Value was deleted
    Deleted,
    /// Formula was changed
    FormulaChanged,
}

/// Transformation pipeline for real-time processing
#[derive(Debug, Clone)]
pub struct TransformationPipeline {
    /// Pipeline ID
    pub id: String,
    /// Pipeline name
    pub name: String,
    /// Transformation steps
    pub steps: Vec<TransformationStep>,
    /// Current step index
    pub current_step: usize,
    /// Pipeline status
    pub status: PipelineStatus,
    /// Created timestamp
    pub created_at: SystemTime,
    /// Last updated timestamp
    pub updated_at: SystemTime,
}

/// Transformation step
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransformationStep {
    /// Step ID
    pub id: String,
    /// Step name
    pub name: String,
    /// Step type
    pub step_type: String,
    /// Step parameters
    pub parameters: HashMap<String, serde_json::Value>,
    /// Step status
    pub status: TransformationStepStatus,
    /// Execution time in milliseconds
    pub execution_time_ms: u64,
}

/// Pipeline status
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PipelineStatus {
    /// Pipeline is idle
    Idle,
    /// Pipeline is running
    Running,
    /// Pipeline completed successfully
    Completed,
    /// Pipeline failed
    Failed,
    /// Pipeline is paused
    Paused,
}

impl RealTimeProcessor {
    /// Create new real-time processor
    pub fn new(config: RealTimeConfig) -> Self {
        Self {
            api_client: Arc::new(Mutex::new(VectrillApiClient::new(&config))),
            websocket: None,
            config,
            pipelines: HashMap::new(),
            update_sender: mpsc::unbounded_channel(),
        }
    }

    /// Start real-time processing
    pub async fn start(&mut self) -> Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        // Connect to WebSocket server
        let ws_url = &self.config.websocket_url;
        let request = tokio_tungstenite::tungstenite::ConnectRequest::builder()
            .uri(ws_url.parse().map_err(|e| VectrillError::NetworkError(format!("Invalid WebSocket URL: {}", e)))?)
            .header("Authorization", self.config.auth_token.clone().unwrap_or_default())
            .build();

        let (ws_stream, _) = tokio_tungstenite::connect_async(request)
            .await
            .map_err(|e| VectrillError::NetworkError(format!("WebSocket connection failed: {}", e)))?;

        self.websocket = Some(ws_stream);

        // Start update broadcasting
        self.start_update_broadcaster().await?;

        Ok(())
    }

    /// Start update broadcaster
    async fn start_update_broadcaster(&mut self) -> Result<()> {
        if let Some(ref mut ws_stream) = self.websocket.take() {
            let mut interval = interval(Duration::from_millis(self.config.update_frequency_ms));
            
            loop {
                tokio::select! {
                    // Handle WebSocket messages
                    ws_message = ws_stream.next() => {
                        match ws_message {
                            Some(Ok(msg)) => {
                                if let Err(e) = self.handle_websocket_message(msg).await {
                                    eprintln!("Error handling WebSocket message: {}", e);
                                }
                            }
                            Some(Err(e)) => {
                                eprintln!("WebSocket error: {}", e);
                                break;
                            }
                            None => {
                                // Connection closed
                                break;
                            }
                        }
                    }
                    
                    // Send periodic updates
                    _ = interval.tick() => {
                        if let Err(e) = self.send_periodic_updates().await {
                            eprintln!("Error sending updates: {}", e);
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Handle incoming WebSocket message
    async fn handle_websocket_message(&mut self, msg: tokio_tungstenite::tungstenite::Message) -> Result<()> {
        match msg {
            tokio_tungstenite::tungstenite::Message::Text(text) => {
                let update: RealTimeUpdate = serde_json::from_str(text)
                    .map_err(|e| VectrillError::NetworkError(format!("Failed to parse update: {}", e)))?;

                self.process_update(update).await?;
            }
            tokio_tungstenite::tungstenite::Message::Binary(data) => {
                // Handle binary data if needed
                eprintln!("Received binary data: {} bytes", data.len());
            }
            tokio_tungstenite::tungstenite::Message::Close(_) => {
                eprintln!("WebSocket connection closed");
            }
            _ => {}
        }

        Ok(())
    }

    /// Process incoming update
    async fn process_update(&mut self, update: RealTimeUpdate) -> Result<()> {
        match update.update_type {
            RealTimeUpdateType::TransformationUpdate => {
                self.handle_transformation_update(update).await?;
            }
            RealTimeUpdateType::CellChange => {
                self.handle_cell_change(update).await?;
            }
            RealTimeUpdateType::RowAdded => {
                self.handle_row_addition(update).await?;
            }
            RealTimeUpdateType::RowDeleted => {
                self.handle_row_deletion(update).await?;
            }
            RealTimeUpdateType::ColumnAdded => {
                self.handle_column_addition(update).await?;
            }
            RealTimeUpdateType::ColumnDeleted => {
                self.handle_column_deletion(update).await?;
            }
            RealTimeUpdateType::StatusUpdate => {
                self.handle_status_update(update).await?;
            }
        }

        Ok(())
    }

    /// Handle transformation update
    async fn handle_transformation_update(&mut self, update: RealTimeUpdate) -> Result<()> {
        if let Some(transform_data) = update.data.get("transformation") {
            // Create or update transformation pipeline
            let pipeline_id = transform_data.get("pipeline_id")
                .and_then(|v| v.as_str())
                .unwrap_or(&Uuid::new_v4().to_string());

            let pipeline = self.pipelines.entry(pipeline_id.clone()).or_insert_with(|| {
                TransformationPipeline {
                    id: pipeline_id.clone(),
                    name: format!("Pipeline {}", pipeline_id),
                    steps: vec![],
                    current_step: 0,
                    status: PipelineStatus::Idle,
                    created_at: SystemTime::now(),
                    updated_at: SystemTime::now(),
                }
            });

            // Update pipeline status
            if let Some(status_data) = transform_data.get("status") {
                if let Ok(status) = serde_json::from_value::<PipelineStatus>(status_data) {
                    pipeline.status = status;
                    pipeline.updated_at = SystemTime::now();
                }
            }

            // Broadcast update to subscribers
            let _ = self.update_sender.send(update).await;
        }

        Ok(())
    }

    /// Handle cell change
    async fn handle_cell_change(&mut self, update: RealTimeUpdate) -> Result<()> {
        if let Some(cell_data) = update.data.get("cell") {
            // Extract cell information
            let cell_ref = cell_data.get("reference")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown");

            let old_value = cell_data.get("old_value").cloned().unwrap_or_default();
            let new_value = cell_data.get("new_value").cloned().unwrap_or_default();

            println!("Cell change detected: {} -> {}", cell_ref, old_value);

            // Broadcast to subscribers
            let _ = self.update_sender.send(update).await;
        }

        Ok(())
    }

    /// Handle row addition
    async fn handle_row_addition(&mut self, update: RealTimeUpdate) -> Result<()> {
        if let Some(row_data) = update.data.get("row") {
            let row_index = row_data.get("index")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);

            let values = row_data.get("values")
                .and_then(|v| v.as_array())
                .map(|arr| arr.to_vec())
                .unwrap_or_default();

            println!("Row {} added with {} values", row_index, values.len());

            // Broadcast to subscribers
            let _ = self.update_sender.send(update).await;
        }

        Ok(())
    }

    /// Handle row deletion
    async fn handle_row_deletion(&mut self, update: RealTimeUpdate) -> Result<()> {
        if let Some(row_data) = update.data.get("row") {
            let row_index = row_data.get("index")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);

            println!("Row {} deleted", row_index);

            // Broadcast to subscribers
            let _ = self.update_sender.send(update).await;
        }

        Ok(())
    }

    /// Handle column addition
    async fn handle_column_addition(&mut self, update: RealTimeUpdate) -> Result<()> {
        if let Some(col_data) = update.data.get("column") {
            let column_name = col_data.get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown");

            println!("Column {} added", column_name);

            // Broadcast to subscribers
            let _ = self.update_sender.send(update).await;
        }

        Ok(())
    }

    /// Handle column deletion
    async fn handle_column_deletion(&mut self, update: RealTimeUpdate) -> Result<()> {
        if let Some(col_data) = update.data.get("column") {
            let column_name = col_data.get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown");

            println!("Column {} deleted", column_name);

            // Broadcast to subscribers
            let _ = self.update_sender.send(update).await;
        }

        Ok(())
    }

    /// Handle status update
    async fn handle_status_update(&mut self, update: RealTimeUpdate) -> Result<()> {
        if let Some(status_data) = update.data.get("status") {
            let message = status_data.get("message")
                .and_then(|v| v.as_str())
                .unwrap_or("Status updated");

            println!("Status update: {}", message);

            // Broadcast to subscribers
            let _ = self.update_sender.send(update).await;
        }

        Ok(())
    }

    /// Send periodic updates
    async fn send_periodic_updates(&mut self) -> Result<()> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();

        // Create status update
        let status_update = RealTimeUpdate {
            id: Uuid::new_v4().to_string(),
            update_type: RealTimeUpdateType::StatusUpdate,
            timestamp,
            data: serde_json::json!({
                "message": "Real-time processing active",
                "active_pipelines": self.pipelines.len(),
                "timestamp": timestamp
            }),
            source: "system".to_string(),
        };

        // Send to all subscribers
        let _ = self.update_sender.send(status_update).await;

        Ok(())
    }

    /// Subscribe to real-time updates
    pub async fn subscribe(&mut self) -> mpsc::UnboundedReceiver<RealTimeUpdate> {
        self.update_sender.subscribe()
    }

    /// Create new transformation pipeline
    pub fn create_pipeline(&mut self, name: &str, steps: Vec<TransformationStep>) -> String {
        let pipeline_id = Uuid::new_v4().to_string();
        
        let pipeline = TransformationPipeline {
            id: pipeline_id.clone(),
            name: name.to_string(),
            steps,
            current_step: 0,
            status: PipelineStatus::Idle,
            created_at: SystemTime::now(),
            updated_at: SystemTime::now(),
        };

        self.pipelines.insert(pipeline_id.clone(), pipeline);
        
        println!("Created pipeline '{}' with ID '{}'", name, pipeline_id);
        
        pipeline_id
    }

    /// Get active pipelines
    pub fn get_pipelines(&self) -> Vec<TransformationPipeline> {
        self.pipelines.values().cloned().collect()
    }

    /// Get pipeline by ID
    pub fn get_pipeline(&self, pipeline_id: &str) -> Option<&TransformationPipeline> {
        self.pipelines.get(pipeline_id)
    }

    /// Update pipeline status
    pub fn update_pipeline_status(&mut self, pipeline_id: &str, status: PipelineStatus) -> Result<()> {
        if let Some(pipeline) = self.pipelines.get_mut(pipeline_id) {
            pipeline.status = status;
            pipeline.updated_at = SystemTime::now();
            
            // Broadcast status change
            let update = RealTimeUpdate {
                id: Uuid::new_v4().to_string(),
                update_type: RealTimeUpdateType::StatusUpdate,
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis(),
                data: serde_json::json!({
                    "pipeline_id": pipeline_id,
                    "status": status
                }),
                source: "system".to_string(),
            };

            let _ = self.update_sender.send(update).await;
        }

        Ok(())
    }
}

/// Mock Vectrill API client for testing
#[cfg(test)]
pub struct VectrillApiClient {
    server_url: String,
    auth_token: Option<String>,
}

#[cfg(test)]
impl VectrillApiClient {
    pub fn new(config: &super::GoogleSheetsConfig) -> Self {
        Self {
            server_url: config.vectrill_server_url.clone(),
            auth_token: config.auth_token.clone(),
        }
    }

    pub fn transform_data(&self, _request: &super::TransformRequest) -> Result<super::TransformResponse> {
        // Mock implementation for testing
        Ok(super::TransformResponse {
            request_id: Uuid::new_v4().to_string(),
            data: super::SpreadsheetData {
                headers: vec!["Transformed".to_string()],
                rows: vec![vec![super::CellValue::String("Mock Data".to_string())]],
            },
            stats: super::ProcessingStats {
                input_rows: 100,
                output_rows: 100,
                processing_time_ms: 100,
                memory_usage_mb: 10.0,
            },
            validation: super::ValidationResult {
                is_valid: true,
                errors: vec![],
                warnings: vec![],
            },
        })
    }

    pub fn get_templates(&self) -> Result<Vec<super::TemplateInfo>> {
        // Mock implementation for testing
        Ok(vec![
            super::TemplateInfo {
                id: "template_1".to_string(),
                name: "Data Cleaning".to_string(),
                description: "Remove duplicates and clean data".to_string(),
                category: "Data Processing".to_string(),
            },
            super::TemplateInfo {
                id: "template_2".to_string(),
                name: "Aggregation".to_string(),
                description: "Summarize data by groups".to_string(),
                category: "Data Analysis".to_string(),
            },
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_real_time_processor_creation() {
        let config = RealTimeConfig::default();
        let processor = RealTimeProcessor::new(config);
        
        assert!(!processor.config.enabled);
        assert_eq!(processor.config.update_frequency_ms, 1000);
        assert_eq!(processor.config.batch_size, 1000);
        assert_eq!(processor.config.max_pipelines, 5);
    }

    #[tokio::test]
    async fn test_pipeline_creation() {
        let config = RealTimeConfig::default();
        let mut processor = RealTimeProcessor::new(config);
        
        let pipeline_id = processor.create_pipeline("Test Pipeline", vec![]);
        
        assert!(processor.pipelines.contains_key(&pipeline_id));
        
        let pipeline = processor.get_pipeline(&pipeline_id).unwrap();
        assert_eq!(pipeline.name, "Test Pipeline");
        assert_eq!(pipeline.steps.len(), 0);
        assert!(matches!(pipeline.status, PipelineStatus::Idle));
    }

    #[tokio::test]
    async fn test_update_subscription() {
        let config = RealTimeConfig::default();
        let mut processor = RealTimeProcessor::new(config);
        
        let mut receiver = processor.subscribe().await;
        
        // Simulate update
        let update = RealTimeUpdate {
            id: Uuid::new_v4().to_string(),
            update_type: RealTimeUpdateType::StatusUpdate,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            data: serde_json::json!({"test": "data"}),
            source: "test".to_string(),
        };

        let _ = processor.update_sender.send(update).await;
        
        // Receive update
        let received_update = receiver.recv().await.unwrap();
        assert_eq!(received_update.id, update.id);
        assert_eq!(received_update.update_type, RealTimeUpdateType::StatusUpdate);
    }
}
