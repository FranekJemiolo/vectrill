"""Vectrill - High-performance Arrow-native streaming engine

This package provides Python bindings for the Vectrill streaming engine.
"""

__version__ = "0.1.0"

try:
    from ._rust import PySequencer
    from .wrapper import Sequencer
    
    __all__ = [
        "__version__",
        "Sequencer",
    ]
except ImportError:
    # Rust bindings not available (M2 not implemented yet)
    __all__ = [
        "__version__",
    ]
