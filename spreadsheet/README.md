# Vectrill Spreadsheet Integration

This document provides comprehensive information about the Vectrill spreadsheet integration, including Excel COM add-in, Google Sheets integration, and real-time processing capabilities.

## Overview

The Vectrill spreadsheet integration enables users to access Vectrill's high-performance data processing capabilities directly from their favorite spreadsheet applications. This integration supports:
├── api/                         # Core API implementation
│   ├── rest/                   # REST API endpoints
│   ├── auth/                   # Authentication middleware
│   └── middleware/             # Request/response middleware
├── excel/                      # Excel integration
│   ├── com-addin/              # COM add-in source code
│   ├── ui/                     # Excel UI components
│   └── installer/              # Installation packages
├── google-sheets/              # Google Sheets integration
│   ├── apps-script/            # Google Apps Script code
│   ├── web-ui/                 # Web-based UI components
│   └── marketplace/            # Google Workspace Marketplace assets
├── templates/                  # Transformation templates
│   ├── data-cleaning/          # Data cleaning templates
│   ├── data-analysis/          # Data analysis templates
│   ├── data-transformation/    # Data transformation templates
│   └── data-filtering/         # Data filtering templates
├── tests/                      # Comprehensive test suite
│   ├── unit/                   # Unit tests
│   ├── integration/            # Integration tests
│   ├── e2e/                    # End-to-end tests
│   └── performance/            # Performance tests
├── docs/                       # Documentation
└── scripts/                    # Build and deployment scripts
```

## Components

### API Layer
- **REST API**: JSON-based API for spreadsheet operations
- **Authentication**: OAuth2 and Windows authentication
- **Middleware**: Rate limiting, validation, error handling

### Excel Integration
- **COM Add-in**: Native Excel integration with ribbon UI
- **Data Processing**: Real-time transformation of Excel data
- **Templates**: Pre-built transformation templates

### Google Sheets Integration
- **Apps Script**: Custom functions and menu integration
- **Web UI**: Sidebar interface for complex operations
- **Cloud Processing**: Server-side data transformation

### Template System
- **12 Pre-built Templates**: Common business transformation patterns
- **Custom Templates**: User-defined transformation sequences
- **Template Registry**: Centralized template management

## Getting Started

### Prerequisites
- Rust 1.70+
- Node.js 18+
- Visual Studio 2022 (for Excel development)
- Google Cloud Project (for Google Sheets development)

### Installation
```bash
# Install Rust dependencies
cargo build --features spreadsheet

# Install Node.js dependencies
npm install

# Set up development environment
./scripts/setup-dev.sh
```

### Running Tests
```bash
# Run all tests
cargo test --features spreadsheet

# Run integration tests
cargo test --test integration --features spreadsheet

# Run performance tests
cargo test --test performance --features spreadsheet
```

## Development

### Code Style
- Follow Rust naming conventions
- Use `cargo fmt` for formatting
- Use `cargo clippy` for linting
- Pre-commit hooks enforce code quality

### Testing
- Unit tests for individual components
- Integration tests for API endpoints
- End-to-end tests for full workflows
- Performance tests for scalability

### Documentation
- API documentation with OpenAPI 3.0
- User guides for Excel and Google Sheets
- Developer documentation for extensibility
- Troubleshooting guides

## Deployment

### Excel Add-in
```bash
# Build COM add-in
./scripts/build-excel-addin.sh

# Create installer
./scripts/create-installer.sh
```

### Google Sheets
```bash
# Build Apps Script
./scripts/build-apps-script.sh

# Deploy to Google Workspace Marketplace
./scripts/deploy-marketplace.sh
```

## Support

For issues and questions:
- Create GitHub issue
- Check documentation
- Review troubleshooting guide
- Contact support team

## License

This project is licensed under the MIT License - see the LICENSE file for details.
