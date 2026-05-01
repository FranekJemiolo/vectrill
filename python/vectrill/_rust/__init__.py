__all__ = ["lib", "ffi"]

import os
import sys
from .ffi import ffi

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

lib = ffi.dlopen(os.path.join(os.path.dirname(__file__), lib_filename))
del os, sys
