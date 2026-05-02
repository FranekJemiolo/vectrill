#!/usr/bin/env python3
"""
Test script to verify GitHub installation works
"""

import subprocess
import sys
import tempfile
import os

def test_github_installation():
    """Test installation from GitHub"""
    print("🧪 Testing GitHub installation...")
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a virtual environment
        venv_dir = os.path.join(temp_dir, "test_env")
        
        print(f"📁 Creating test environment in {venv_dir}")
        
        # Create virtual environment
        subprocess.run([sys.executable, "-m", "venv", venv_dir], check=True)
        
        # Get python and pip paths in the virtual environment
        if sys.platform == "win32":
            python_exe = os.path.join(venv_dir, "Scripts", "python.exe")
            pip_exe = os.path.join(venv_dir, "Scripts", "pip.exe")
        else:
            python_exe = os.path.join(venv_dir, "bin", "python")
            pip_exe = os.path.join(venv_dir, "bin", "pip")
        
        print("📦 Installing Vectrill from GitHub...")
        
        # Install from GitHub
        install_cmd = [python_exe, "-m", "pip", "install", "git+https://github.com/FranekJemiolo/vectrill"]
        
        try:
            result = subprocess.run(install_cmd, capture_output=True, text=True, timeout=300)
            
            if result.returncode == 0:
                print("✅ Installation successful!")
                
                # Test import
                print("🧪 Testing import...")
                test_cmd = [python_exe, "-c", "import vectrill; print('Vectrill imported successfully!')"]
                
                test_result = subprocess.run(test_cmd, capture_output=True, text=True, timeout=30)
                
                if test_result.returncode == 0:
                    print("✅ Import test passed!")
                    print(test_result.stdout.strip())
                    return True
                else:
                    print("❌ Import test failed:")
                    print(test_result.stderr)
                    return False
            else:
                print("❌ Installation failed:")
                print(result.stderr)
                return False
                
        except subprocess.TimeoutExpired:
            print("❌ Installation timed out")
            return False
        except Exception as e:
            print(f"❌ Error during installation: {e}")
            return False

def test_current_installation():
    """Test current installation"""
    print("🧪 Testing current installation...")
    
    try:
        import vectrill
        print("✅ Vectrill is already installed and importable!")
        
        # Test basic functionality
        import pandas as pd
        data = pd.DataFrame({
            'value': [1, 2, 3, 4, 5],
            'category': ['A', 'B', 'A', 'B', 'A']
        })
        
        df = vectrill.from_pandas(data)
        result = df.filter(vectrill.col('value') > 2)
        print(f"✅ Basic functionality test passed! Result shape: {len(result)} rows")
        
        return True
        
    except ImportError as e:
        print(f"❌ Vectrill not available: {e}")
        return False
    except Exception as e:
        print(f"❌ Error testing Vectrill: {e}")
        return False

if __name__ == "__main__":
    print("🚀 Vectrill Installation Test Suite")
    print("=" * 40)
    
    # Test current installation first
    current_ok = test_current_installation()
    
    print("\n" + "-" * 40)
    
    # Test GitHub installation (this might take a while)
    github_ok = test_github_installation()
    
    print("\n" + "=" * 40)
    print("📊 Test Results:")
    print(f"Current Installation: {'✅ PASS' if current_ok else '❌ FAIL'}")
    print(f"GitHub Installation:  {'✅ PASS' if github_ok else '❌ FAIL'}")
    
    if current_ok and github_ok:
        print("\n🎉 All tests passed!")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed!")
        sys.exit(1)
