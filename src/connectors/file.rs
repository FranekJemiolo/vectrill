//! File connector for reading from local files

use crate::{error::Result, RecordBatch};
use arrow::datatypes::SchemaRef;
use std::path::PathBuf;

use super::Connector;
use async_trait::async_trait;

/// File connector for reading data from local files
pub struct FileConnector {
    path: PathBuf,
    schema: SchemaRef,
    // TODO: Add file reader implementation
}

impl FileConnector {
    /// Create a new file connector
    pub fn new(path: PathBuf, schema: SchemaRef) -> Self {
        Self { path, schema }
    }
}

#[async_trait]
impl Connector for FileConnector {
    async fn next_batch(&mut self) -> Option<Result<RecordBatch>> {
        // TODO: Implement file reading
        // This will be completed in M7 with actual file format support
        None
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn name(&self) -> &str {
        self.path.to_str().unwrap_or("file")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_file_connector_creation() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let connector = FileConnector::new(PathBuf::from("/tmp/test.json"), schema);
        assert_eq!(connector.name(), "/tmp/test.json");
    }
}
