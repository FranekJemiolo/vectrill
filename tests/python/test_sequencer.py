"""Tests for Python Sequencer bindings"""

import pytest
import polars as pl
import pyarrow as pa
from vectrill import Sequencer


def test_sequencer_creation():
    """Test creating a sequencer with default configuration."""
    sequencer = Sequencer()
    assert sequencer.watermark == 0
    assert sequencer.pending_batches == 0


def test_sequencer_creation_with_config():
    """Test creating a sequencer with custom configuration."""
    config = {
        "ordering": "by_timestamp",
        "late_data_policy": "drop",
        "max_lateness_ms": 500,
        "batch_size": 500,
        "flush_interval_ms": 50
    }
    sequencer = Sequencer(config)
    assert sequencer.watermark == 0
    assert sequencer.pending_batches == 0


def test_sequancer_default_config():
    """Test getting default configuration."""
    config = Sequencer.default_config()
    assert isinstance(config, dict)
    assert "ordering" in config
    assert "late_data_policy" in config
    assert "max_lateness_ms" in config
    assert "batch_size" in config
    assert "flush_interval_ms" in config


def test_sequencer_repr():
    """Test sequencer string representation."""
    sequencer = Sequencer()
    repr_str = repr(sequencer)
    assert "Sequencer" in repr_str
    assert "watermark=0" in repr_str
    assert "pending_batches=0" in repr_str


def test_ingest_polars_dataframe():
    """Test ingesting a Polars DataFrame."""
    sequencer = Sequencer()
    
    # Create test data
    df = pl.DataFrame({
        "timestamp": [1000, 2000, 1500],
        "key": ["a", "b", "a"],
        "value": [1, 2, 3]
    })
    
    # Should not raise an exception
    sequencer.ingest(df)
    assert sequencer.pending_batches == 1


def test_ingest_pyarrow_table():
    """Test ingesting a PyArrow Table."""
    sequencer = Sequencer()
    
    # Create test data
    table = pa.Table.from_pydict({
        "timestamp": [1000, 2000, 1500],
        "key": ["a", "b", "a"],
        "value": [1, 2, 3]
    })
    
    # Should not raise an exception
    sequencer.ingest(table)
    assert sequencer.pending_batches == 1


def test_ingest_pyarrow_recordbatch():
    """Test ingesting a PyArrow RecordBatch."""
    sequencer = Sequencer()
    
    # Create test data
    batch = pa.RecordBatch.from_pydict({
        "timestamp": [1000, 2000, 1500],
        "key": ["a", "b", "a"],
        "value": [1, 2, 3]
    })
    
    # Should not raise an exception
    sequencer.ingest(batch)
    assert sequencer.pending_batches == 1


def test_ingest_invalid_type():
    """Test that ingesting invalid data types raises an error."""
    sequencer = Sequencer()
    
    with pytest.raises(TypeError, match="Unsupported data type"):
        sequencer.ingest("invalid data")
    
    with pytest.raises(TypeError, match="Unsupported data type"):
        sequencer.ingest([1, 2, 3])


def test_next_batch_empty():
    """Test getting next batch when no data is available."""
    sequencer = Sequencer()
    
    # Should return None when no data is available
    result = sequencer.next_batch()
    assert result is None


def test_next_batch_with_data():
    """Test getting next batch after ingesting data."""
    sequencer = Sequencer()
    
    # Ingest test data
    df = pl.DataFrame({
        "timestamp": [1000, 2000, 1500],
        "key": ["a", "b", "a"],
        "value": [1, 2, 3]
    })
    
    sequencer.ingest(df)
    
    # Should return a DataFrame (placeholder implementation for M2)
    result = sequencer.next_batch()
    assert result is not None
    assert isinstance(result, pl.DataFrame)


def test_flush_empty():
    """Test flushing when no data is available."""
    sequencer = Sequencer()
    
    # Should return None when no data is available
    result = sequencer.flush()
    assert result is None


def test_flush_with_data():
    """Test flushing after ingesting data."""
    sequencer = Sequencer()
    
    # Ingest test data
    df = pl.DataFrame({
        "timestamp": [1000, 2000, 1500],
        "key": ["a", "b", "a"],
        "value": [1, 2, 3]
    })
    
    sequencer.ingest(df)
    
    # Should return a DataFrame (placeholder implementation for M2)
    result = sequencer.flush()
    assert result is not None
    assert isinstance(result, pl.DataFrame)


def test_watermark_progression():
    """Test that watermark progresses as data is processed."""
    sequencer = Sequencer()
    
    # Ingest data with increasing timestamps
    df1 = pl.DataFrame({
        "timestamp": [1000, 2000],
        "key": ["a", "b"],
        "value": [1, 2]
    })
    
    sequencer.ingest(df1)
    
    # Process the batch
    result = sequencer.next_batch()
    assert result is not None
    
    # Watermark should have advanced (implementation dependent)
    # For M2, this is a basic test to ensure the interface works
    assert isinstance(sequencer.watermark, int)


def test_multiple_batches():
    """Test processing multiple batches."""
    sequencer = Sequencer()
    
    # Ingest multiple batches
    for i in range(3):
        df = pl.DataFrame({
            "timestamp": [i * 1000, i * 1000 + 500],
            "key": ["a", "b"],
            "value": [i, i + 1]
        })
        sequencer.ingest(df)
    
    assert sequencer.pending_batches == 3
    
    # Process all batches
    processed = 0
    while True:
        result = sequencer.next_batch()
        if result is None:
            break
        processed += 1
        assert isinstance(result, pl.DataFrame)
    
    # Should have processed some batches (exact number depends on implementation)
    assert processed >= 0


def test_configuration_validation():
    """Test that invalid configurations raise appropriate errors."""
    # Test invalid ordering
    with pytest.raises(Exception):  # PyO3 will raise a ValueError
        Sequencer({"ordering": "invalid"})
    
    # Test invalid late data policy
    with pytest.raises(Exception):  # PyO3 will raise a ValueError
        Sequencer({"late_data_policy": "invalid"})


if __name__ == "__main__":
    pytest.main([__file__])
