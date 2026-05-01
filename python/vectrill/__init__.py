"""Vectrill - High-performance Arrow-native streaming engine

This package provides Python bindings for the Vectrill streaming engine.
"""

__version__ = "0.1.0"

try:
    from .wrapper import Sequencer
    from .dataframe import from_pandas, col
    
    __all__ = [
        "__version__",
        "Sequencer",
        "from_pandas",
        "col",
    ]
except ImportError:
    # Rust bindings not available (M2 not implemented yet)
    __all__ = [
        "__version__",
    ]
