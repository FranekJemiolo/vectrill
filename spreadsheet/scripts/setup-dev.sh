#!/bin/bash

# Development Environment Setup Script
# This script sets up the development environment for the spreadsheet integration

set -e

echo "🚀 Setting up Vectrill Spreadsheet Integration Development Environment"

# Check prerequisites
check_prerequisites() {
    echo "📋 Checking prerequisites..."
    
    # Check Rust
    if ! command -v cargo &> /dev/null; then
        echo "❌ Rust is not installed. Please install Rust 1.70+"
        exit 1
    fi
    
    # Check Node.js
    if ! command -v node &> /dev/null; then
        echo "❌ Node.js is not installed. Please install Node.js 18+"
        exit 1
    fi
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        echo "❌ Python 3 is not installed"
        exit 1
    fi
    
    echo "✅ All prerequisites are installed"
}

# Install Rust dependencies
install_rust_deps() {
    echo "🦀 Installing Rust dependencies..."
    cd /Users/franek/personal_workspace/vectrill
    cargo build --features spreadsheet
    echo "✅ Rust dependencies installed"
}

# Install Node.js dependencies
install_node_deps() {
    echo "📦 Installing Node.js dependencies..."
    cd /Users/franek/personal_workspace/vectrill/spreadsheet
    npm install
    echo "✅ Node.js dependencies installed"
}

# Install Python dependencies
install_python_deps() {
    echo "🐍 Installing Python dependencies..."
    cd /Users/franek/personal_workspace/vectrill
    pip install -r requirements.txt
    echo "✅ Python dependencies installed"
}

# Set up pre-commit hooks
setup_pre_commit() {
    echo "🔧 Setting up pre-commit hooks..."
    cd /Users/franek/personal_workspace/vectrill
    
    # Install pre-commit
    pip install pre-commit
    
    # Run pre-commit install
    pre-commit install
    
    echo "✅ Pre-commit hooks set up"
}

# Create development configuration
create_dev_config() {
    echo "⚙️ Creating development configuration..."
    
    # Create environment file
    cat > /Users/franek/personal_workspace/vectrill/spreadsheet/.env.development << EOF
# Development Environment Configuration
API_HOST=localhost
API_PORT=8080
API_PROTOCOL=http
DATABASE_URL=sqlite:./dev.db
LOG_LEVEL=debug
ENABLE_CORS=true
ENABLE_AUTH=false
EOF
    
    echo "✅ Development configuration created"
}

# Run initial tests
run_initial_tests() {
    echo "🧪 Running initial tests..."
    cd /Users/franek/personal_workspace/vectrill
    
    # Run Rust tests
    cargo test --features spreadsheet --lib
    
    # Run Python tests
    python -m pytest tests/spreadsheet/ -v
    
    echo "✅ Initial tests completed"
}

# Main execution
main() {
    check_prerequisites
    install_rust_deps
    install_node_deps
    install_python_deps
    setup_pre_commit
    create_dev_config
    run_initial_tests
    
    echo "🎉 Development environment setup completed successfully!"
    echo ""
    echo "📚 Next steps:"
    echo "  1. Start the API server: cd spreadsheet && npm run dev"
    echo "  2. Run tests: cargo test --features spreadsheet"
    echo "  3. Check code quality: pre-commit run --all-files"
    echo "  4. View documentation: open docs/index.html"
}

main "$@"
