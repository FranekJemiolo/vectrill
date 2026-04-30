"""Basic Python tests for Vectrill

These tests will be expanded as milestones are implemented.
"""

import pytest


def test_import():
    """Test that the package can be imported."""
    import vectrill
    assert vectrill.__version__ == "0.1.0"


# More tests will be added as milestones are implemented
# def test_sequencer():
#     """Test basic sequencer functionality."""
#     import vectrill as vt
#     import polars as pl
#     
#     seq = vt.Sequencer()
#     df = pl.DataFrame({
#         "timestamp": [1, 2, 3],
#         "value": [10, 20, 30]
#     })
#     
#     seq.ingest(df)
#     result = seq.next_batch()
#     
#     assert result is not None
#     assert len(result) == 3
