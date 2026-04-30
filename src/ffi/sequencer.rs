//! Python bindings for the Sequencer

#[cfg(feature = "python")]
use pyo3::prelude::*;
#[cfg(feature = "python")]
use pyo3::types::PyDict;

#[cfg(feature = "python")]
use crate::sequencer::{Sequencer, SequencerConfig, Ordering, LateDataPolicy};
#[cfg(feature = "python")]
use super::arrow_bridge::{pyany_to_record_batch, export_batch_to_python};

/// Python wrapper for Sequencer
#[cfg(feature = "python")]
#[pyclass(name = "Sequencer")]
pub struct PySequencer {
    inner: Sequencer,
}

#[cfg(feature = "python")]
#[pymethods]
impl PySequencer {
    /// Create a new Sequencer with optional configuration
    #[new]
    fn new(config: Option<Bound<'_, PyDict>>) -> PyResult<Self> {
        let sequencer_config = if let Some(config_dict) = config {
            // Parse configuration from Python dict
            let mut sequencer_config = SequencerConfig::new();
            
            // Parse ordering
            if let Some(ordering_str) = config_dict.get_item("ordering")? {
                let ordering = match ordering_str.extract::<&str>()? {
                    "by_timestamp" => Ordering::ByTimestamp,
                    "by_key_then_timestamp" => Ordering::ByKeyThenTimestamp,
                    _ => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "Invalid ordering. Use 'by_timestamp' or 'by_key_then_timestamp'"
                    )),
                };
                sequencer_config.ordering = ordering;
            }
            
            // Parse late data policy
            if let Some(policy_str) = config_dict.get_item("late_data_policy")? {
                let policy = match policy_str.extract::<&str>()? {
                    "drop" => LateDataPolicy::Drop,
                    "allow" => LateDataPolicy::Allow,
                    "side_output" => LateDataPolicy::SideOutput,
                    _ => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                        "Invalid late_data_policy. Use 'drop', 'allow', or 'side_output'"
                    )),
                };
                sequencer_config.late_data_policy = policy;
            }
            
            // Parse max lateness
            if let Some(max_lateness) = config_dict.get_item("max_lateness_ms")? {
                sequencer_config.max_lateness_ms = max_lateness.extract()?;
            }
            
            // Parse batch size
            if let Some(batch_size) = config_dict.get_item("batch_size")? {
                sequencer_config.batch_size = batch_size.extract()?;
            }
            
            // Parse flush interval
            if let Some(flush_interval) = config_dict.get_item("flush_interval_ms")? {
                sequencer_config.flush_interval_ms = flush_interval.extract()?;
            }
            
            sequencer_config
        } else {
            SequencerConfig::default()
        };
        
        let sequencer = Sequencer::new(sequencer_config);
        Ok(PySequencer { inner: sequencer })
    }
    
    /// Ingest a batch from Python (Arrow, PyArrow, or Polars)
    fn ingest(&mut self, batch: Bound<'_, PyAny>) -> PyResult<()> {
        let record_batch = pyany_to_record_batch(&batch)?;
        self.inner.ingest(record_batch)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!("Failed to ingest batch: {}", e)))?;
        Ok(())
    }
    
    /// Get the next ordered batch as Arrow C Data Interface
    fn next_batch(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        if let Some(batch) = self.inner.next_batch() {
            let (array, schema) = export_batch_to_python(&batch, py)?;
            // Return a tuple (array, schema) representing the Arrow C Data Interface
            Ok(Some((array, schema).to_object(py)))
        } else {
            Ok(None)
        }
    }
    
    /// Get the current watermark
    #[getter]
    fn get_watermark(&self) -> i64 {
        self.inner.watermark()
    }
    
    /// Get the number of pending batches
    #[getter]
    fn get_pending_batches(&self) -> usize {
        self.inner.pending_batches()
    }
    
    /// Force flush any buffered data
    fn flush(&mut self, py: Python) -> PyResult<Option<PyObject>> {
        if let Some(batch) = self.inner.next_batch() {
            let (array, schema) = export_batch_to_python(&batch, py)?;
            Ok(Some((array, schema).to_object(py)))
        } else {
            Ok(None)
        }
    }
    
    /// Create a default configuration dictionary
    #[staticmethod]
    fn default_config(py: Python) -> PyResult<PyObject> {
        let config = PyDict::new_bound(py);
        
        config.set_item("ordering", "by_timestamp")?;
        config.set_item("late_data_policy", "drop")?;
        config.set_item("max_lateness_ms", 1000i64)?;
        config.set_item("batch_size", 1000usize)?;
        config.set_item("flush_interval_ms", 100i64)?;
        
        Ok(config.into())
    }
    
    /// Get a string representation
    fn __repr__(&self) -> String {
        format!(
            "Sequencer(watermark={}, pending_batches={})",
            self.inner.watermark(),
            self.inner.pending_batches()
        )
    }
    
    /// Get a string representation
    fn __str__(&self) -> String {
        self.__repr__()
    }
}
