#!/bin/bash

# Google Apps Script Build Script
# This script builds and packages the Google Apps Script integration

set -e

echo "📄 Building Google Apps Script Integration"

# Check prerequisites
check_prerequisites() {
    echo "📋 Checking Google Apps Script build prerequisites..."
    
    # Check Node.js
    if ! command -v node &> /dev/null; then
        echo "❌ Node.js is not installed. Please install Node.js 18+"
        exit 1
    fi
    
    # Check npm
    if ! command -v npm &> /dev/null; then
        echo "❌ npm is not installed"
        exit 1
    fi
    
    # Check gcloud CLI
    if ! command -v gcloud &> /dev/null; then
        echo "⚠️  gcloud CLI is not installed. Required for deployment"
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

# Build Apps Script
build_apps_script() {
    echo "📝 Building Google Apps Script..."
    cd /Users/franek/personal_workspace/vectrill/spreadsheet/google-sheets/apps-script
    
    # Install dependencies
    npm install
    
    # Build the Apps Script
    npm run build
    
    echo "✅ Google Apps Script built"
}

# Create output directory
create_output_dir() {
    echo "📁 Creating output directory..."
    
    mkdir -p /Users/franek/personal_workspace/vectrill/spreadsheet/dist/google-sheets
    
    # Copy built files
    cp -r /Users/franek/personal_workspace/vectrill/spreadsheet/google-sheets/apps-script/dist/* \
       /Users/franek/personal_workspace/vectrill/spreadsheet/dist/google-sheets/
    
    echo "✅ Output directory created"
}

# Create deployment package
create_deployment_package() {
    echo "📦 Creating deployment package..."
    
    cd /Users/franek/personal_workspace/vectrill/spreadsheet/dist/google-sheets
    
    # Create manifest
    cat > appsscript.json << EOF
{
  "timeZone": "America/New_York",
  "dependencies": {},
  "exceptionLogging": "STACKDRIVER",
  "runtimeVersion": "V8",
  "oauthScopes": [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/script.external_request",
    "https://www.googleapis.com/auth/script.container.ui"
  ],
  "addOns": {
    "common": {
      "name": "Vectrill",
      "logoUrl": "https://www.vectrill.com/logo.png",
      "layoutProperties": {
        "primaryColor": "#2772ae",
        "secondaryColor": "#ffffff"
      }
    },
    "sheets": {
      "homepageTrigger": {
        "runFunction": "onHomepage"
      }
    }
  }
}
EOF
    
    # Create deployment script
    cat > deploy.sh << EOF
#!/bin/bash

# Deploy to Google Apps Script
echo "🚀 Deploying to Google Apps Script..."

# Check if gcloud is authenticated
if ! gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q "@"; then
    echo "❌ Please run: gcloud auth login"
    exit 1
fi

# Deploy the script
gcloud script deploy vectrill-sheets \
    --source=. \
    --title="Vectrill Sheets Integration" \
    --description="Vectrill data transformation for Google Sheets"

echo "✅ Deployment completed"
EOF
    
    chmod +x deploy.sh
    
    echo "✅ Deployment package created"
}

# Run tests
run_tests() {
    echo "🧪 Running Google Apps Script tests..."
    cd /Users/franek/personal_workspace/vectrill/spreadsheet/google-sheets/apps-script
    
    # Run unit tests
    npm test
    
    echo "✅ Tests completed"
}

# Create version info
create_version_info() {
    echo "📝 Creating version information..."
    
    local version=$(cd /Users/franek/personal_workspace/vectrill && cargo metadata --no-deps --format-version 1 | jq -r '.packages[0].version')
    local build_date=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
    local git_commit=$(cd /Users/franek/personal_workspace/vectrill && git rev-parse HEAD)
    
    cat > /Users/franek/personal_workspace/vectrill/spreadsheet/dist/google-sheets/version.json << EOF
{
  "version": "$version",
  "build_date": "$build_date",
  "git_commit": "$git_commit",
  "platform": "google-sheets",
  "runtime": "apps-script"
}
EOF
    
    echo "✅ Version information created"
}

# Main execution
main() {
    check_prerequisites
    build_rust_core
    build_apps_script
    create_output_dir
    create_deployment_package
    run_tests
    create_version_info
    
    echo "🎉 Google Apps Script build completed successfully!"
    echo ""
    echo "📦 Output files:"
    echo "  - Apps Script: spreadsheet/dist/google-sheets/"
    echo "  - Manifest: spreadsheet/dist/google-sheets/appsscript.json"
    echo "  - Deploy script: spreadsheet/dist/google-sheets/deploy.sh"
    echo "  - Version: spreadsheet/dist/google-sheets/version.json"
    echo ""
    echo "📋 Next steps:"
    echo "  1. Deploy to Google: cd dist/google-sheets && ./deploy.sh"
    echo "  2. Test in Sheets: Open Google Sheets and enable the add-on"
    echo "  3. Run integration tests: ./scripts/test-google-sheets.sh"
}

main "$@"
