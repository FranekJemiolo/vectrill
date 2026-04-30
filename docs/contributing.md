# Contributing

This document provides guidelines for contributing to Vectrill.

## Development Workflow

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Run tests: `cargo test`
5. Run e2e tests: `./scripts/run_e2e_tests.sh`
6. Commit your changes
7. Push to your fork
8. Submit a pull request

## Setting Up Development Environment

### Prerequisites

- Rust 1.70+
- Python 3.12+
- uv (for Python dependency management)
- maturin (for Python bindings)
- Docker (for e2e tests)

### Installation

```bash
# Clone your fork
git clone https://github.com/your-username/vectrill.git
cd vectrill

# Install Python dependencies
uv sync
uv sync --dev

# Build the project
cargo build

# Install Python package
maturin develop
```

## Running Tests

### Unit Tests

```bash
# Run all tests
cargo test

# Run specific test
cargo test test_sequencer_creation

# Run tests with output
cargo test -- --nocapture
```

### E2E Tests

```bash
# Run e2e tests with docker-compose
./scripts/run_e2e_tests.sh

# Run e2e tests locally (without docker-compose)
./scripts/run_e2e_tests.sh --skip-docker

# Run e2e tests with verbose output
./scripts/run_e2e_tests.sh --verbose
```

### Python Tests

```bash
# Run Python tests
pytest tests/python/

# Run specific Python test
pytest tests/python/test_sequencer.py
```

## Code Style

### Rust

Use the standard Rust formatting:

```bash
# Format code
cargo fmt

# Check formatting
cargo fmt -- --check
```

Run clippy for linting:

```bash
# Run clippy
cargo clippy

# Run clippy with strict warnings
cargo clippy -- -D warnings
```

### Python

Use Black for formatting:

```bash
# Format Python code
black python/ tests/python/

# Check formatting
black --check python/ tests/python/
```

Use Ruff for linting:

```bash
# Run ruff
ruff check python/ tests/python/

# Fix issues
ruff check --fix python/ tests/python/
```

## Commit Messages

Use clear, descriptive commit messages:

```
Add expression optimizer with constant folding

- Implemented ExprOptimizer with constant folding for arithmetic operations
- Added CSE for common subexpression elimination
- Added tests for optimization correctness
```

## Pull Request Guidelines

### PR Description

Include in your PR description:

- Summary of changes
- Motivation for the change
- Related issues (if any)
- Testing approach
- Breaking changes (if any)

### Checklist

- [ ] Tests pass locally
- [ ] E2E tests pass
- [ ] Code is formatted (cargo fmt, black)
- [ ] Clippy passes without warnings
- [ ] Documentation is updated
- [ ] PR description is complete

## Adding Features

### New Operators

When adding a new operator:

1. Implement the `Operator` trait
2. Add unit tests in `tests/operators/`
3. Add integration tests
4. Update documentation
5. Add example usage

### New Connectors

When adding a new connector:

1. Implement the `Connector` trait
2. Add tests in `tests/e2e/connectors.rs`
3. Add to feature flags in `Cargo.toml`
4. Update documentation
5. Add example usage

### New Expressions

When adding a new expression:

1. Add to the `Expr` enum
2. Implement evaluation in the expression engine
3. Add compilation support
4. Add tests
5. Update documentation

## Documentation

### Code Documentation

Use Rustdoc for Rust code:

```rust
/// Creates a new sequencer with the given configuration.
///
/// # Arguments
///
/// * `config` - The sequencer configuration
///
/// # Returns
///
/// A new `Sequencer` instance
pub fn new(config: SequencerConfig) -> Self {
    // ...
}
```

### Documentation Updates

Update relevant documentation when making changes:

- README.md for user-facing changes
- docs/ for detailed documentation
- Examples for new features
- API reference for new functions/types

## Performance Considerations

### Benchmarking

When making performance changes:

1. Add benchmarks in `benches/`
2. Run benchmarks before and after
3. Include benchmark results in PR description

### Profiling

Use pprof for profiling:

```bash
cargo pprof --bench sequencer --features performance
```

## Issue Reporting

### Bug Reports

Include in bug reports:

- Rust and Python versions
- Minimal reproduction code
- Expected vs actual behavior
- Error messages and stack traces

### Feature Requests

Include in feature requests:

- Use case description
- Proposed API/design
- Alternatives considered
- Potential implementation approach

## Code Review

### Reviewing PRs

When reviewing PRs:

- Check for correct implementation
- Verify tests pass
- Check documentation
- Consider performance impact
- Check for breaking changes

### Addressing Review Comments

- Respond to all review comments
- Make requested changes or discuss alternatives
- Update PR description as needed

## Release Process

Releases follow semantic versioning:

- **Major**: Breaking changes
- **Minor**: New features, backward compatible
- **Patch**: Bug fixes, backward compatible

## Getting Help

- Open an issue for bugs or feature requests
- Start a discussion for questions
- Check existing issues and discussions

## License

By contributing to Vectrill, you agree that your contributions will be licensed under the MIT OR Apache-2.0 license.

## Community Guidelines

- Be respectful and inclusive
- Provide constructive feedback
- Focus on what is best for the community
- Show empathy towards other community members

Thank you for contributing to Vectrill!
