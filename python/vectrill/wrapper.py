"""Python wrapper for Vectrill streaming engine"""

from typing import Optional, Dict, Any, Union
import polars as pl
import pyarrow as pa

try:
    from ._rust import ffi, lib
    
    class Sequencer:
        """Python wrapper for the Rust Sequencer implementation."""
        
        def __init__(self, config: Optional[Dict[str, Any]] = None):
            """
            Initialize a new Sequencer.
            
            Args:
                config: Optional configuration dictionary with keys:
                    - ordering: str ('by_timestamp' or 'by_key_then_timestamp')
                    - late_data_policy: str ('drop', 'allow', or 'side_output')
                    - max_lateness_ms: int (maximum allowed lateness in milliseconds)
                    - batch_size: int (target batch size)
                    - flush_interval_ms: int (flush interval in milliseconds)
            """
            # Validate configuration
            if config:
                # Validate ordering
                if "ordering" in config and config["ordering"] not in ["by_timestamp", "by_key_then_timestamp"]:
                    raise ValueError("Invalid ordering. Use 'by_timestamp' or 'by_key_then_timestamp'")
                
                # Validate late data policy
                if "late_data_policy" in config and config["late_data_policy"] not in ["drop", "allow", "side_output"]:
                    raise ValueError("Invalid late_data_policy. Use 'drop', 'allow', or 'side_output'")
            
            # For now, create a simple placeholder implementation
            # The actual CFFI bindings would need to be implemented
            self._watermark = 0
            self._pending_batches = 0
        
        def ingest(self, data: Union[pl.DataFrame, pa.Table, pa.RecordBatch]) -> None:
            """
            Ingest a batch of data into the sequencer.
            
            Args:
                data: Polars DataFrame, PyArrow Table, or PyArrow RecordBatch
            """
            if not isinstance(data, (pl.DataFrame, pa.Table, pa.RecordBatch)):
                raise TypeError(
                    f"Unsupported data type: {type(data)}. "
                    "Expected Polars DataFrame, PyArrow Table, or PyArrow RecordBatch."
                )
            # Placeholder implementation for M2
            self._pending_batches += 1
        
        def next_batch(self) -> Optional[pl.DataFrame]:
            """
            Get the next ordered batch from the sequencer.
            
            Returns:
                Polars DataFrame if a batch is available, None otherwise
            """
            if self._pending_batches > 0:
                self._pending_batches -= 1
                # Placeholder implementation for M2
                return pl.DataFrame({
                    "timestamp": [0],
                    "key": ["placeholder"],
                    "value": [0]
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
