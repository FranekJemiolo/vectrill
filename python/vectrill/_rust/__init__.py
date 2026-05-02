__all__ = ["lib", "ffi"]

import os
import sys
from .ffi import ffi

# For PyO3 extensions built with maturin, the library is automatically loaded
# We need to handle both development and installed scenarios

def load_library():
    """Load the Rust library, trying multiple approaches"""
    
    # First try to find the library in target directories (development)
    # Platform-specific library filename
    if sys.platform.startswith('linux'):
        lib_filename = 'lib_rust.so'
    elif sys.platform.startswith('darwin'):
        lib_filename = 'lib_rust.dylib'
    elif sys.platform.startswith('win'):
        lib_filename = 'rust.dll'
    else:
        # Fallback to .so for other Unix-like systems
        lib_filename = 'lib_rust.so'

    # Try to find the library in various locations
    lib_paths = [
        os.path.join(os.path.dirname(__file__), lib_filename),  # _rust directory
        os.path.join(os.path.dirname(__file__), '..', '..', '..', 'target', 'release', lib_filename),  # target/release
        os.path.join(os.path.dirname(__file__), '..', '..', '..', 'target', 'debug', lib_filename),  # target/debug
    ]
    
    for lib_path in lib_paths:
        try:
            return ffi.dlopen(lib_path)
        except OSError:
            continue
    
    # If we can't find the library manually, it might be loaded as a PyO3 extension
    # In that case, we'll create a dummy lib object that raises an informative error
    class DummyLib:
        def __getattr__(self, name):
            raise ImportError(f"Rust library function '{name}' not available. The library may not be properly built or installed.")
    
    return DummyLib()

# Load the library
lib = load_library()

del os, sys
