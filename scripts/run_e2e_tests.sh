#!/bin/bash
# Script to run end-to-end tests for vectrill

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default values
COMPOSE_FILE="docker-compose.test.yml"
TEST_FEATURES="connectors-full,web-ui"
TEST_TARGET="e2e"
CLEANUP=true
VERBOSE=false
SKIP_DOCKER=false

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --no-cleanup)
            CLEANUP=false
            shift
            ;;
        --skip-docker)
            SKIP_DOCKER=true
            shift
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --features)
            TEST_FEATURES="$2"
            shift 2
            ;;
        --target)
            TEST_TARGET="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --no-cleanup        Don't stop docker-compose services after tests"
            echo "  --skip-docker       Run tests locally without docker-compose"
            echo "  --verbose           Enable verbose output"
            echo "  --features FLAGS    Cargo features to use (default: connectors-full,web-ui)"
            echo "  --target TARGET    Cargo test target (default: e2e)"
            echo "  --help              Show this help message"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo -e "${GREEN}=== Vectrill E2E Test Runner ===${NC}"
echo ""

# Function to cleanup on exit
cleanup() {
    if [ "$CLEANUP" = true ] && [ "$SKIP_DOCKER" = false ]; then
        echo -e "${YELLOW}Cleaning up docker-compose services...${NC}"
        docker-compose -f "$COMPOSE_FILE" down
        echo -e "${GREEN}Cleanup complete${NC}"
    fi
}

# Set trap for cleanup
trap cleanup EXIT INT TERM

# Start docker-compose services
if [ "$SKIP_DOCKER" = false ]; then
    echo -e "${YELLOW}Starting docker-compose services...${NC}"
    docker-compose -f "$COMPOSE_FILE" up -d
    
    # Wait for services to be ready
    echo -e "${YELLOW}Waiting for services to be ready...${NC}"
    sleep 10
    
    # Check if services are running
    if ! docker-compose -f "$COMPOSE_FILE" ps | grep -q "Up"; then
        echo -e "${RED}Failed to start docker-compose services${NC}"
        docker-compose -f "$COMPOSE_FILE" logs
        exit 1
    fi
    
    echo -e "${GREEN}Services started successfully${NC}"
    echo ""
fi

# Build the project
echo -e "${YELLOW}Building project with features: $TEST_FEATURES${NC}"
if [ "$VERBOSE" = true ]; then
    cargo build --features "$TEST_FEATURES" --verbose
else
    cargo build --features "$TEST_FEATURES"
fi
echo -e "${GREEN}Build complete${NC}"
echo ""

# Run tests
echo -e "${YELLOW}Running e2e tests...${NC}"
if [ "$SKIP_DOCKER" = false ]; then
    # Run tests in the test runner container
    if [ "$VERBOSE" = true ]; then
        docker-compose -f "$COMPOSE_FILE" run --rm test-runner cargo test --test "$TEST_TARGET" --features "$TEST_FEATURES" -- --nocapture
    else
        docker-compose -f "$COMPOSE_FILE" run --rm test-runner cargo test --test "$TEST_TARGET" --features "$TEST_FEATURES"
    fi
else
    # Run tests locally
    if [ "$VERBOSE" = true ]; then
        cargo test --test "$TEST_TARGET" --features "$TEST_FEATURES" -- --nocapture
    else
        cargo test --test "$TEST_TARGET" --features "$TEST_FEATURES"
    fi
fi

TEST_RESULT=$?

echo ""
if [ $TEST_RESULT -eq 0 ]; then
    echo -e "${GREEN}=== All tests passed! ===${NC}"
else
    echo -e "${RED}=== Tests failed ===${NC}"
    if [ "$SKIP_DOCKER" = false ]; then
        echo -e "${YELLOW}Showing docker-compose logs...${NC}"
        docker-compose -f "$COMPOSE_FILE" logs
    fi
fi

exit $TEST_RESULT
