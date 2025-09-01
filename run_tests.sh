#!/bin/bash

# Data Migration Tool - Comprehensive tests runner
# This script runs all tests with detailed reporting

set -e

echo "Starting Data Migration Tool Test Suite"
echo "======================================="

# Function to run tests with timing
run_test_with_timing() {
    local test_name="$1"
    local test_command="$2"
    
    print_status "Running $test_name..."
    start_time=$(date +%s)
    
    if eval "$test_command"; then
        end_time=$(date +%s)
        duration=$((end_time - start_time))
        print_status "$test_name completed in ${duration}s"
        return 0
    else
        end_time=$(date +%s)
        duration=$((end_time - start_time))
        print_status "$test_name failed after ${duration}s"
        return 1
    fi
}

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
passed_tests=0

#Test 1: Code formatting
total_tests=$((total_tests + 1))
if run_test_with_timing "Code Formatting Check" "go fmt ./... && [ -z \"\$(go fmt ./...)\" ]"; then
    passed_tests=$((passed_tests + 1))
fi

#Test 2: Go vet
total_tests=$((total_tests + 1))
if run_test_with_timing "Go Vet Analysis" "go vet ./..."; then
    passed_tests=$((passed_tests + 1))
fi

#Test 3: Unit Tests with Coverage
total_tests=$((total_tests + 1))
echo ""
print_status "Running unit tests with coverage..."
if go test -v -cover -coverprofile="${TEST_RESULTS_DIR}/coverage.out" ./... > "${TEST_RESULTS_DIR}/test_output.log" 2>&1; then
    passed_tests=$((passed_tests + 1))
    
    # Generate coverage report
    go tool cover -html="${TEST_RESULTS_DIR}/coverage.out" -o "${TEST_RESULTS_DIR}/coverage.html"
    
    # Extract coverage percentage
    coverage=$(go tool cover -func="${TEST_RESULTS_DIR}/coverage.out" | grep total | awk '{print $3}')
    print_status "Unit tests passed with ${coverage} coverage"
    print_status "Coverage report: ${TEST_RESULTS_DIR}/coverage.html"
else
    print_status "Unit tests failed"
    print_status "Check ${TEST_RESULTS_DIR}/test_output.log for details"
fi

# Test 4: Race condition detection
total_tests=$((total_tests + 1))
if run_test_with_timing "Race Condition Detection" "go test -race ./..."; then
    passed_tests=$((passed_tests + 1))
fi

# Test 5: Benchmark tests
total_tests=$((total_tests + 1))
echo ""
print_status "Running benchmark tests..."
if go test -bench=. -benchmem ./... > "${TEST_RESULTS_DIR}/benchmark_results.log" 2>&1; then
    passed_tests=$((passed_tests + 1))
    print_status "Benchmark tests completed"
    print_status "Benchmark results: ${TEST_RESULTS_DIR}/benchmark_results.log"
    
    # Show benchmark summary
    echo ""
    print_status "Performance Summary:"
    grep "Benchmark" "${TEST_RESULTS_DIR}/benchmark_results.log" | head -5
else
    print_status "Benchmark tests failed"
fi

# Test 6: Build verification
total_tests=$((total_tests + 1))
if run_test_with_timing "Build Verification" "go build -o ${TEST_RESULTS_DIR}/migration_tool ."; then
    passed_tests=$((passed_tests + 1))
    print_status "Binary created: ${TEST_RESULTS_DIR}/migration_tool"
fi

# Test 7: Module verification
total_tests=$((total_tests + 1))
if run_test_with_timing "Module Verification" "go mod verify && go mod tidy"; then
    passed_tests=$((passed_tests + 1))
fi

# Test 8: Integration test (if available)
if [ -f "tests/integration_test.go" ]; then
    total_tests=$((total_tests + 1))
    if run_test_with_timing "Integration Tests" "go test -v ./tests/..."; then
        passed_tests=$((passed_tests + 1))
    fi
fi

echo ""
echo "============================================="
print_status "TEST SUMMARY"
echo "============================================="

if [ $passed_tests -eq $total_tests ]; then
    print_status "ALL TESTS PASSED! ($passed_tests/$total_tests)"
    exit_code=0
else
    failed_tests=$((total_tests - passed_tests))
    print_status "Some tests failed: $passed_tests/$total_tests passed, $failed_tests failed"
    exit_code=1
fi

echo ""
print_status "Test artifacts saved in: $TEST_RESULTS_DIR/"
echo "   - coverage.html: Test coverage report"
echo "   - coverage.out: Coverage data file"
echo "   - test_output.log: Detailed test output"
echo "   - benchmark_results.log: Performance benchmarks"
echo "   - migration_tool: Compiled binary"

echo ""
print_status "Quick commands:"
echo "   View coverage: open $TEST_RESULTS_DIR/coverage.html"
echo "   View test logs: cat $TEST_RESULTS_DIR/test_output.log"
echo "   View benchmarks: cat $TEST_RESULTS_DIR/benchmark_results.log"
echo "   Test binary: ./$TEST_RESULTS_DIR/migration_tool --help"

echo ""
if [ $exit_code -eq 0 ]; then
    print_status "Ready for production deployment!"
else
    print_status "Please fix failing tests before deployment."
fi

exit $exit_code