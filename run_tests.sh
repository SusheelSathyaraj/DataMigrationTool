#!/bin/bash

# Data Migration Tool - Comprehensive tests runner
# This script runs all tests with detailed reporting

set -e

echo "Starting Data Migration Tool Test Suite"
echo "======================================="

#Check if Go is installed
if ! command -v go &> /dev/null; then
    print_status "Go is not installed. Please install go and try again"
    exit 1
fi

print_status "Go version: $(go version)"

#Create directory for test results
mkdir -p test_results
TEST_RESULTS_DIR="test_results"

echo ""
print_status "Test Plan..."
echo "1. Code Formatting and Linting"
echo "2. Unit Tests with coverage"
echo "3. Integration Tests"
echo "4. Benchmark Tests"
echo "5. Race Condition detection"
echo "6. Build Verification"
echo ""

#Initialise counter
total_tests=0
passed_test=0


