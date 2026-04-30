//! Ingestion layer for managing data flow from connectors

use crate::{connectors::Connector, error::Result, RecordBatch};
use std::sync::Arc;
use tokio::sync::mpsc;

/// Ingestion channel for transporting batches
pub type BatchSender = mpsc::Sender<Arc<RecordBatch>>;
pub type BatchReceiver = mpsc::Receiver<Arc<RecordBatch>>;

/// Create a bounded channel for batch transport
pub fn create_channel(capacity: usize) -> (BatchSender, BatchReceiver) {
    mpsc::channel(capacity)
}

/// Ingestion manager that runs connectors and feeds batches into channels
pub struct IngestionManager {
    connectors: Vec<Box<dyn Connector>>,
    sender: BatchSender,
}

impl IngestionManager {
    /// Create a new ingestion manager
    pub fn new(connectors: Vec<Box<dyn Connector>>, sender: BatchSender) -> Self {
        Self { connectors, sender }
    }

    /// Start all connectors and begin ingestion
    pub async fn start(&mut self) -> Result<()> {
        let mut tasks = Vec::new();

        for connector in self.connectors.drain(..) {
            let name = connector.name().to_string();
            let sender = self.sender.clone();

            let task = tokio::spawn(async move {
                let mut connector = connector;
                while let Some(batch_result) = connector.next_batch().await {
                    match batch_result {
                        Ok(batch) => {
                            if sender.send(Arc::new(batch)).await.is_err() {
                                eprintln!("Channel closed for connector {}", name);
                                break;
                            }
                        }
                        Err(e) => {
                            eprintln!("Error in connector {}: {}", name, e);
                            break;
                        }
                    }
                }
            });

            tasks.push(task);
        }

        // Keep tasks alive
        for task in tasks {
            task.await.ok();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::MemoryConnector;

    #[tokio::test]
    async fn test_channel_creation() {
        let (sender, mut receiver) = create_channel(10);
        assert!(sender
            .send(Arc::new(RecordBatch::new_empty(Arc::new(
                arrow::datatypes::Schema::empty()
            ))))
            .await
            .is_ok());
        assert!(receiver.recv().await.is_some());
    }

    #[tokio::test]
    async fn test_ingestion_manager() {
        let connector =
            Box::new(MemoryConnector::new("test".to_string(), 2, 5)) as Box<dyn Connector>;
        let (sender, mut receiver) = create_channel(100);

        let mut manager = IngestionManager::new(vec![connector], sender);
        manager.start().await.unwrap();

        // Receive batches
        let mut total_rows = 0;
        for _ in 0..2 {
            if let Some(batch) = receiver.recv().await {
                total_rows += batch.num_rows();
            }
        }

        assert_eq!(total_rows, 10);
    }
}
