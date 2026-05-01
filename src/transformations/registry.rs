//! Custom transformation registry

use crate::transformations::Transformation;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Transformation registry for managing custom transformations
pub struct TransformationRegistry {
    transformations: HashMap<String, Box<dyn Fn() -> Box<dyn Transformation> + Send + Sync>>,
}

impl TransformationRegistry {
    /// Create a new transformation registry
    pub fn new() -> Self {
        Self {
            transformations: HashMap::new(),
        }
    }

    /// Register a transformation
    pub fn register<F>(&mut self, name: String, factory: F)
    where
        F: Fn() -> Box<dyn Transformation> + Send + Sync + 'static,
    {
        self.transformations.insert(name, Box::new(factory));
    }

    /// Create a transformation by name
    pub fn create(&self, name: &str) -> Option<Box<dyn Transformation>> {
        self.transformations.get(name).map(|factory| factory())
    }

    /// List all registered transformations
    pub fn list_transformations(&self) -> Vec<String> {
        self.transformations.keys().cloned().collect()
    }
}

impl Default for TransformationRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Global transformation registry
pub static TRANSFORMATION_REGISTRY: std::sync::LazyLock<Arc<Mutex<TransformationRegistry>>> =
    std::sync::LazyLock::new(|| Arc::new(Mutex::new(TransformationRegistry::new())));

/// Register a transformation globally
pub fn register_transformation<F>(name: String, factory: F)
where
    F: Fn() -> Box<dyn Transformation> + Send + Sync + 'static,
{
    let mut registry = TRANSFORMATION_REGISTRY.lock().unwrap();
    registry.register(name, factory);
}

/// Create a transformation by name
pub fn create_transformation(name: &str) -> Option<Box<dyn Transformation>> {
    let registry = TRANSFORMATION_REGISTRY.lock().unwrap();
    registry.create(name)
}

/// List all registered transformations
pub fn list_transformations() -> Vec<String> {
    let registry = TRANSFORMATION_REGISTRY.lock().unwrap();
    registry.list_transformations()
}

/// Transformation pipeline for chaining multiple transformations
pub struct TransformationPipeline {
    transformations: Vec<Box<dyn Transformation>>,
    name: String,
}

impl TransformationPipeline {
    /// Create a new transformation pipeline
    pub fn new(name: String) -> Self {
        Self {
            transformations: Vec::new(),
            name,
        }
    }

    /// Add a transformation to the pipeline
    pub fn add_transform(mut self, transform: Box<dyn Transformation>) -> Self {
        self.transformations.push(transform);
        self
    }

    /// Apply all transformations in sequence
    pub async fn apply(
        &mut self,
        batch: crate::RecordBatch,
    ) -> Result<crate::RecordBatch, crate::VectrillError> {
        let mut current_batch = batch;

        for transform in &mut self.transformations {
            current_batch = transform.apply(&current_batch).await?;
        }

        Ok(current_batch)
    }

    /// Get the pipeline name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the number of transformations in the pipeline
    pub fn len(&self) -> usize {
        self.transformations.len()
    }

    /// Check if the pipeline is empty
    pub fn is_empty(&self) -> bool {
        self.transformations.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transformations::builtin::{FilterOperator, FilterTransform, FilterValue};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_transformation_registry() {
        let mut registry = TransformationRegistry::new();

        // Register a transformation
        registry.register("test_filter".to_string(), || {
            Box::new(FilterTransform::new(
                "value".to_string(),
                FilterOperator::GreaterThan,
                FilterValue::Int64(100),
                Arc::new(Schema::new(vec![Field::new(
                    "value",
                    DataType::Int64,
                    false,
                )])),
            )) as Box<dyn Transformation>
        });

        // List transformations
        let list = registry.list_transformations();
        assert_eq!(list.len(), 1);
        assert!(list.contains(&"test_filter".to_string()));

        // Create transformation
        let transform = registry.create("test_filter");
        assert!(transform.is_some());
    }

    #[test]
    fn test_transformation_pipeline() {
        let pipeline = TransformationPipeline::new("test_pipeline".to_string());

        assert_eq!(pipeline.name(), "test_pipeline");
        assert_eq!(pipeline.len(), 0);
        assert!(pipeline.is_empty());
    }
}
