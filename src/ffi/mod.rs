//! Python FFI bindings using PyO3

#[cfg(feature = "python")]
use pyo3::prelude::*;

#[cfg(feature = "python")]
mod arrow_bridge;
#[cfg(feature = "python")]
mod sequencer;

#[cfg(feature = "python")]
pub use arrow_bridge::*;
#[cfg(feature = "python")]
pub use sequencer::*;

/// Python module definition
#[cfg(feature = "python")]
#[pymodule]
fn _rust(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Add sequencer class
    m.add_class::<PySequencer>()?;
    
    // Add module version
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    
    Ok(())
}
