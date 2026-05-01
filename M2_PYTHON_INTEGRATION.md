# M2: Python Integration (FFI)

## Goal
Make the Rust engine usable from Python with zero-copy Arrow exchange.

## Duration
2-3 days

## Deliverables
- PyO3 module
- Arrow C Data Interface bridge
- Python wrapper class
- Zero-copy Arrow ↔ Polars integration

## Tasks

### 1. PyO3 Setup
- [ ] Configure Cargo.toml for cdylib
- [ ] Add PyO3 dependencies
- [ ] Set up maturin build configuration
- [ ] Create Python package structure

### 2. Arrow FFI Bridge
- [ ] Implement Arrow C Data Interface export
- [ ] Create PyCapsule wrappers for ArrowArray and ArrowSchema
- [ ] Implement memory ownership management
- [ ] Add destructor for Rust-owned memory

### 3. Python Bindings
- [ ] Create `PySequencer` class
- [ ] Implement `ingest_arrow` method
- [ ] Implement `next_batch` method
- [ ] Add configuration methods
- [ ] Expose via PyO3 module

### 4. Python Wrapper
- [ ] Create clean Python API in `vectrill/__init__.py`
- [ ] Implement `Sequencer` wrapper class
- [ ] Add Polars integration (DataFrame → Arrow → Rust)
- [ ] Add type hints
- [ ] Add docstrings

### 5. Build System
- [ ] Configure maturin in pyproject.toml
- [ ] Add Python dependencies (polars, pyarrow)
- [ ] Create Makefile for dev workflow
- [ ] Add editable install instructions

### 6. Tests
- [ ] Python test for basic ingestion
- [ ] Python test for batch retrieval
- [ ] Python test for Polars integration
- [ ] Test zero-copy (verify no data copy)
- [ ] Integration test with real DataFrame

## Implementation Details

### Cargo.toml Configuration
```toml
[lib]
name = "vectrill"
crate-type = ["cdylib"]

[dependencies]
pyo3 = { version = "0.21", features = ["extension-module"] }
arrow = "51"
```

### PyO3 Module
```rust
#[pyclass]
pub struct PySequencer {
    inner: Sequencer,
}

#[pymethods]
impl PySequencer {
    #[new]
    fn new(config: Option<SequencerConfig>) -> Self;
    
    fn ingest_arrow(&mut self, batch: &PyAny) -> PyResult<()>;
    
    fn next_batch(&mut self) -> PyResult<Option<PyObject>>;
}

#[pymodule]
fn _rust(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PySequencer>()?;
    Ok(())
}
```

### Python Wrapper
```python
import polars as pl
from ._rust import PySequencer

class Sequencer:
    def __init__(self, config=None):
        self._inner = PySequencer(config)
    
    def ingest(self, df: pl.DataFrame):
        self._inner.ingest_arrow(df.to_arrow())
    
    def next_batch(self) -> pl.DataFrame | None:
        batch = self._inner.next_batch()
        if batch is None:
            return None
        return pl.from_arrow(batch)
```

### Arrow FFI Export
```rust
use arrow_c_data::{FFI_ArrowArray, FFI_ArrowSchema};

fn export_batch(batch: &RecordBatch) -> PyResult<(PyObject, PyObject)> {
    let mut array = FFI_ArrowArray::empty();
    let mut schema = FFI_ArrowSchema::empty();
    
    export_array(&batch.into(), &mut array)?;
    export_schema(batch.schema(), &mut schema)?;
    
    let py_array = PyCapsule::new(py, array, Some("arrow_array"))?;
    let py_schema = PyCapsule::new(py, schema, Some("arrow_schema"))?;
    
    Ok((py_array, py_schema))
}
```

## Success Criteria
- [ ] `maturin develop` builds successfully
- [ ] Python imports vectrill without errors
- [ ] Can ingest Polars DataFrame
- [ ] Can retrieve Polars DataFrame
- [ ] Zero-copy verified (no data duplication)
- [ ] All Python tests pass

## Dev Workflow
```bash
# Install in editable mode
maturin develop

# Rebuild after changes
maturin develop

# Run Python tests
pytest tests/python
```

## Dependencies
- `pyo3` >= 0.21
- `arrow` >= 51
- `arrow-c-data` >= 51
- `maturin` >= 1.0
- Python: `polars`, `pyarrow`

## Critical Design Rules
1. Keep Rust core Python-agnostic (no Python types in core modules)
2. Python = orchestration only (batching logic stays in Rust)
3. Arrow is the contract (everything crosses boundary as RecordBatch)
4. Rust owns memory (Python must not free Arrow buffers)
