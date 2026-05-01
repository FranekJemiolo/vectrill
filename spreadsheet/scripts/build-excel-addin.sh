#!/bin/bash

# Excel COM Add-in Build Script
# This script builds the Excel COM add-in for Windows

set -e

echo "🔨 Building Excel COM Add-in"

# Check prerequisites
check_prerequisites() {
    echo "📋 Checking Excel add-in build prerequisites..."
    
    # Check if we're on Windows
    if [[ "$OSTYPE" != "msys" && "$OSTYPE" != "win32" && "$OSTYPE" != "cygwin" ]]; then
        echo "❌ Excel COM add-in can only be built on Windows"
        exit 1
    fi
    
    # Check for Visual Studio
    if ! command -v msbuild &> /dev/null; then
        echo "❌ Visual Studio or MSBuild is not installed"
        exit 1
    fi
    
    # Check for .NET Framework
    if ! dotnet --list-runtimes | grep -q "Microsoft.WindowsDesktop.App"; then
        echo "❌ .NET Framework Desktop runtime is not installed"
        exit 1
    fi
    
    echo "✅ Prerequisites check passed"
}

# Build Rust core library
build_rust_core() {
    echo "🦀 Building Rust core library..."
    cd /Users/franek/personal_workspace/vectrill
    
    # Build with spreadsheet features
    cargo build --release --features spreadsheet
    
    echo "✅ Rust core library built"
}

# Build Excel COM add-in
build_excel_addin() {
    echo "📊 Building Excel COM add-in..."
    cd /Users/franek/personal_workspace/vectrill/spreadsheet/excel/com-addin
    
    # Restore NuGet packages
    nuget restore VectrillExcelAddin.sln
    
    # Build the solution
    msbuild VectrillExcelAddin.sln /p:Configuration=Release /p:Platform=x64
    
    echo "✅ Excel COM add-in built"
}

# Create output directory
create_output_dir() {
    echo "📁 Creating output directory..."
    
    mkdir -p /Users/franek/personal_workspace/vectrill/spreadsheet/dist/excel
    
    # Copy built files
    cp /Users/franek/personal_workspace/vectrill/spreadsheet/excel/com-addin/VectrillExcelAddin/bin/x64/Release/VectrillExcelAddin.dll \
       /Users/franek/personal_workspace/vectrill/spreadsheet/dist/excel/
    
    cp /Users/franek/personal_workspace/vectrill/spreadsheet/excel/com-addin/VectrillExcelAddin/bin/x64/Release/VectrillExcelAddin.addin \
       /Users/franek/personal_workspace/vectrill/spreadsheet/dist/excel/
    
    echo "✅ Output directory created"
}

# Run tests
run_tests() {
    echo "🧪 Running Excel add-in tests..."
    cd /Users/franek/personal_workspace/vectrill/spreadsheet/excel/com-addin
    
    # Run unit tests
    dotnet test --configuration Release
    
    echo "✅ Tests completed"
}

# Create version info
create_version_info() {
    echo "📝 Creating version information..."
    
    local version=$(cd /Users/franek/personal_workspace/vectrill && cargo metadata --no-deps --format-version 1 | jq -r '.packages[0].version')
    local build_date=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    local git_commit=$(cd /Users/franek/personal_workspace/vectrill && git rev-parse HEAD)
    
    cat > /Users/franek/personal_workspace/vectrill/spreadsheet/dist/excel/version.json << EOF
{
  "version": "$version",
  "build_date": "$build_date",
  "git_commit": "$git_commit",
  "platform": "windows",
  "architecture": "x64"
}
EOF
    
    echo "✅ Version information created"
}

# Main execution
main() {
    check_prerequisites
    build_rust_core
    build_excel_addin
    create_output_dir
    run_tests
    create_version_info
    
    echo "🎉 Excel COM add-in build completed successfully!"
    echo ""
    echo "📦 Output files:"
    echo "  - DLL: spreadsheet/dist/excel/VectrillExcelAddin.dll"
    echo "  - Add-in: spreadsheet/dist/excel/VectrillExcelAddin.addin"
    echo "  - Version: spreadsheet/dist/excel/version.json"
    echo ""
    echo "📋 Next steps:"
    echo "  1. Run installer: ./scripts/create-installer.sh"
    echo "  2. Test in Excel: Register the add-in"
    echo "  3. Run integration tests: ./scripts/test-excel-addin.sh"
}

main "$@"
