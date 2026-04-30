"""Python wrapper for Vectrill streaming engine"""

from typing import Optional, Dict, Any, Union
import polars as pl
import pyarrow as pa

try:
    from ._rust import PySequencer
    
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
            self._inner = PySequencer(config)
        
        def ingest(self, data: Union[pl.DataFrame, pa.Table, pa.RecordBatch]) -> None:
            """
            Ingest a batch of data into the sequencer.
            
            Args:
                data: Polars DataFrame, PyArrow Table, or PyArrow RecordBatch
            """
            if isinstance(data, pl.DataFrame):
                # Convert Polars DataFrame to PyArrow
                arrow_table = data.to_arrow()
                self._inner.ingest(arrow_table)
            elif isinstance(data, (pa.Table, pa.RecordBatch)):
                # Use PyArrow directly
                self._inner.ingest(data)
            else:
                raise TypeError(
                    f"Unsupported data type: {type(data)}. "
                    "Expected Polars DataFrame, PyArrow Table, or PyArrow RecordBatch."
                )
        
        def next_batch(self) -> Optional[pl.DataFrame]:
            """
            Get the next ordered batch from the sequencer.
            
            Returns:
                Polars DataFrame if a batch is available, None otherwise
            """
            result = self._inner.next_batch()
            if result is None:
                return None
            
            # Convert Arrow C Data Interface back to PyArrow then Polars
            array_capsule, schema_capsule = result
            
            # This is a simplified conversion - in practice we'd need to properly
            # reconstruct the Arrow object from the C Data Interface
            # For now, we'll use a placeholder implementation for M2
            return pl.DataFrame({
                "timestamp": [0],
                "key": ["placeholder"],
                "value": [0]
            })
        
        @property
        def watermark(self) -> int:
            """Get the current watermark."""
            return self._inner.watermark
        
        @property
        def pending_batches(self) -> int:
            """Get the number of pending batches."""
            return self._inner.pending_batches
        
        def flush(self) -> Optional[pl.DataFrame]:
            """
            Force flush any buffered data.
            
            Returns:
                Polars DataFrame if data is available, None otherwise
            """
            result = self._inner.flush()
            if result is None:
                return None
            
            # Similar conversion as in next_batch
            array_capsule, schema_capsule = result
            
            # Placeholder implementation for M2
            return pl.DataFrame({
                "timestamp": [0],
                "key": ["placeholder"],
                "value": [0]
            })
        
        @classmethod
        def default_config(cls) -> Dict[str, Any]:
            """Get the default configuration."""
            return PySequencer.default_config()
        
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
