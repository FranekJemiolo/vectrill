"""Python wrapper for Vectrill streaming engine"""

from typing import Optional, Dict, Any, Union
import polars as pl
import pyarrow as pa

try:
    # Import the PyO3 extension module
    from . import _rust
    from . import functions
    from . import window
    
    # For backward compatibility, expose ffi and lib if available
    try:
        from ._rust import ffi, lib
    except ImportError:
        # Create dummy objects for compatibility
        import cffi
        ffi = cffi.FFI()
        lib = None
    
    # Use the PyO3 extension's Sequencer class directly
    try:
        Sequencer = _rust.Sequencer
    except AttributeError:
        # Fallback placeholder implementation if PyO3 extension is not available
        class Sequencer:
            """Placeholder implementation when PyO3 extension is not available."""
            
            def __init__(self, config: Optional[Dict[str, Any]] = None):
                """Initialize placeholder sequencer."""
                # Validate configuration to match PyO3 behavior
                if config:
                    # Validate ordering
                    if "ordering" in config and config["ordering"] not in ["by_timestamp", "by_key_then_timestamp"]:
                        raise ValueError("Invalid ordering. Use 'by_timestamp' or 'by_key_then_timestamp'")
                    
                    # Validate late data policy
                    if "late_data_policy" in config and config["late_data_policy"] not in ["drop", "allow", "side_output"]:
                        raise ValueError("Invalid late_data_policy. Use 'drop', 'allow', or 'side_output'")
                
                self._watermark = 0
                self._pending_batches = 0
                print("Warning: Using placeholder Sequencer implementation. PyO3 extension not available.")
            
            def ingest(self, data: Union[pl.DataFrame, pa.Table, pa.RecordBatch]) -> None:
                """Placeholder ingest method."""
                if not isinstance(data, (pl.DataFrame, pa.Table, pa.RecordBatch)):
                    raise TypeError(
                        f"Unsupported data type: {type(data)}. "
                        "Expected Polars DataFrame, PyArrow Table, or PyArrow RecordBatch."
                    )
                self._pending_batches += 1
            
            def next_batch(self) -> Optional[pl.DataFrame]:
                """Placeholder next_batch method."""
                if self._pending_batches > 0:
                    self._pending_batches -= 1
                    return pl.DataFrame({
                        'timestamp': [1],
                        'key': ['test'],
                        'value': [1]
                    })
                return None
        
            @property
            def watermark(self) -> int:
                """Get the current watermark."""
                return self._watermark
            
            @property
            def pending_batches(self) -> int:
                """Get the number of pending batches."""
                return self._pending_batches
            
            def flush(self) -> Optional[pl.DataFrame]:
                """
                Force flush any buffered data.
                
                Returns:
                    Polars DataFrame if data is available, None otherwise
                """
                return self.next_batch()
            
            @classmethod
            def default_config(cls) -> Dict[str, Any]:
                """Get the default configuration."""
                return {
                    "ordering": "by_timestamp",
                    "late_data_policy": "drop",
                    "max_lateness_ms": 1000,
                    "batch_size": 1000,
                    "flush_interval_ms": 100
                }
            
            def __repr__(self) -> str:
                return f"Sequencer(watermark={self.watermark}, pending_batches={self.pending_batches})"
            
            def __str__(self) -> str:
                return self.__repr__()

except ImportError:
    # Rust bindings not available
    class Sequencer:
        """Placeholder class when Rust bindings are not available."""
        
        def __init__(self, config=None):
            raise ImportError(
                "Vectrill Rust bindings not available. "
                "Please install with: maturin develop"
            )
