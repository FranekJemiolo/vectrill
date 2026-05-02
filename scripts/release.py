#!/usr/bin/env python3
"""
Release script for Vectrill package
Handles version management and release process
"""

import subprocess
import sys
import re
from pathlib import Path

def get_current_version():
    """Get current version from git tags"""
    try:
        result = subprocess.run(
            ['git', 'describe', '--tags', '--abbrev=0'],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout.strip()
    except subprocess.CalledProcessError:
        return "v0.1.0"

def bump_version(version_type='patch'):
    """Bump version based on type"""
    current = get_current_version().lstrip('v')
    parts = current.split('.')
    
    if version_type == 'patch':
        parts[-1] = str(int(parts[-1]) + 1)
    elif version_type == 'minor':
        parts[-2] = str(int(parts[-2]) + 1)
        parts[-1] = '0'
    elif version_type == 'major':
        parts[0] = str(int(parts[0]) + 1)
        parts[1] = '0'
        parts[2] = '0'
    
    return f"v{'.'.join(parts)}"

def create_release_notes(version):
    """Generate release notes"""
    try:
        # Get previous tag
        result = subprocess.run(
            ['git', 'describe', '--tags', '--abbrev=0', 'HEAD^'],
            capture_output=True,
            text=True,
            check=True
        )
        previous_tag = result.stdout.strip()
        
        # Get commits since previous tag
        commits = subprocess.run(
            ['git', 'log', '--pretty=format:- %s (%h)', f'{previous_tag}..HEAD'],
            capture_output=True,
            text=True,
            check=True
        )
        
        notes = f"""## Release {version}

### Changes since {previous_tag}

{commits.stdout}

### Installation

```bash
# Install from PyPI
pip install vectrill

# Install from GitHub
pip install git+https://github.com/FranekJemiolo/vectrill@{version}
```

### Development Installation

```bash
git clone https://github.com/FranekJemiolo/vectrill.git
cd vectrill
pip install -e .
```
"""
        return notes
        
    except subprocess.CalledProcessError:
        return f"""## Release {version}

### Initial Release

High-performance Arrow-native streaming engine with Python DSL and Rust execution core.

### Installation

```bash
# Install from PyPI
pip install vectrill

# Install from GitHub
pip install git+https://github.com/FranekJemiolo/vectrill@{version}
```
"""

def create_tag_and_push(version):
    """Create git tag and push"""
    try:
        # Create tag
        subprocess.run(['git', 'tag', version], check=True)
        
        # Push tag
        subprocess.run(['git', 'push', 'origin', version], check=True)
        
        print(f"✅ Created and pushed tag {version}")
        return True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to create tag: {e}")
        return False

def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/release.py [patch|minor|major|version]")
        print("Example: python scripts/release.py patch")
        print("Example: python scripts/release.py v0.2.0")
        sys.exit(1)
    
    arg = sys.argv[1]
    
    if arg in ['patch', 'minor', 'major']:
        version = bump_version(arg)
        print(f"Bumping {arg} version to {version}")
    elif arg.startswith('v'):
        version = arg
        print(f"Using specified version {version}")
    else:
        print("Invalid argument. Use patch, minor, major, or specify version like v0.2.0")
        sys.exit(1)
    
    # Generate release notes
    notes = create_release_notes(version)
    
    # Create release notes file
    release_file = Path(f"RELEASE_NOTES_{version}.md")
    release_file.write_text(notes)
    print(f"📝 Generated release notes: {release_file}")
    
    # Ask for confirmation
    response = input(f"Create release {version}? (y/N): ")
    if response.lower() == 'y':
        if create_tag_and_push(version):
            print(f"🚀 Release {version} triggered!")
            print("📋 GitHub Actions will build and publish the release automatically")
        else:
            print("❌ Release failed")
            sys.exit(1)
    else:
        print("❌ Release cancelled")

if __name__ == "__main__":
    main()
