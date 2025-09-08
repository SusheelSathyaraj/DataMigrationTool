.DEFAULT_GOAL := help
.PHONY : fmt vet clean build run test test-coverage test-race test-bench test-integration test-all install deps lint security audit

#Variables
BINARY_NAME=migration-tool
BINARY_PATH=./$(BINARY_NAME)
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "v1.0.0-dev")
BUILD_TIME=$(shell date -u '+%Y-%m-%d_%H:%M:%S')
GIT_COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
LDFLAGS=-ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME) -X main.GitCommit=$(GIT_COMMIT)"

fmt:
	@echo "Formatting Go Code"
	@go fmt ./...
	@echo "Code Formatted Successfully"
vet: fmt
	@echo "Running Go Vet analysis"
	@go vet ./...
	@echo "Statis Analysis Completed"

security:
	@echo "Checking for security vulnerabilities..."
	@if command -v gosec >/dev/null 2>&1; then \
		gosec ./...; \
	else \
		echo "gosec not installed. Run: go install github.com/securecodewarrior/gosec/v2/cmd/gosec@latest"; \
	fi

lint: fmt
	@echo "Running Linter..."
	@if command-v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...
		echo "Linting Completed"; \
	else \
		echo "golangci-lint not installed. Run go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest"; \
	fi

deps:
	@echo "Installing Dependcies..."
	@go mod download
	@go mod tidy
	@echo "Dependcies installed"

audit: deps
	@echo "Auditing Dependcies..."
	@if command -v govulncheck >/dev/null 2>&1; then \
		govulncheck ./...; \
	else \
		echo "govulnsec not installed. Run go install golang.org/x/vuln/cmd/govulncheck@latest"; \
	fi
	@echo "Dependcy Audit Completed"

clean: 
	@echo "CLeaning Build artifacts"
	@go clean
	@rm -rf test_results/
	@rm -rf migration_snapshots/
	@rm -f $(BINARY_NAME) binary
	@echo "Cleanup Completed"

build: clean vet
	@echo "Building $(BINARY_NAME) $(VERSION)..."
	@go build $(LDFLAGS) -o $(BINARY_NAME)
	@echo "Build Completed: $(BINARY_PATH)"
	@echo "Binary Info:"
	@ls -lh $(BINARY_NAME)

build-all: clean vet
	@echo "Building for multiple platforms..."
	@mkdir -p dist
	@GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o dist/$(BINARY_NAME)-linux-amd64
	@GOOS=windows GOARCH=amd64 go build $(LDFLAGS) -o dist/$(BINARY_NAME)-windows-amd.exe
	@echo "Multi- Platform build completed"
	@ls -lh dist/

install: build
	@echo "Installing $(BINARY_NAME)..."
	@sudo mv $(BINARY_NAME) /usr/local/bin/
	@echo "$(BINARY_NAME) installed to /usr/local/bin/"

run: build
	@echo "Running $(BINARY_NAME) with arguments: $(ARGS)"
	@$(BINARY_PATH) $(ARGS)

run-example: build
	@echo "Running example migration..."
	@$(BINARY_PATH) --source=mysql --target=postgresql --mode=full --dry-run

version: build
	@$(BINARY_PATH) --version

app-help: build
	@$(BINARY_PATH) --help

test:
	@echo "Running unit tests..."
	@go test -v ./...
	@echo "Unit tests completed"

test-coverage:
	@echo "Running tests with coverage report"
	@mkdir -p test_results
	@go test -v -cover -coverprofile=test_results/coverage.out ./...
	@go tool cover -html=test_results/coverage.out -o test_results/coverage.html
	@go tool cover -func=test_results/coverage.out | grep total

test-race:
	@echo "Running tests with race detection..."
	@go test -race ./...
	@echo "Race condition tests completed"

test-bench:
	@echo "Running benchmark tests..."
	@mkdir -p test_results
	@go test -bench=. -benchmem ./... | tee test_results/benchmark_results.log
	@echo "Benchmark Results: test_results/benchmark_results.log"

test-integration:
	@echo "Running integration tests..."
	@go test -v ./tests/...
	@echo "Integration Test completed"

test-all:
	@echo "Running comprehensive test suite"
	@chmod +x run_tests.sh
	@./run_tests.sh

mocks:
	@echo "Generating mocks..."
	@if command -v mockgen >/dev/null 2>&1; then \
		mockgen -source=database/interface.go -destination=tests/mocks/database_mock.go; \
		echo "Mocks generated"
	else \
		echo "mockgen not installed. Run go install github.com/golang/mock/mockgen@latest"; \
	fi

docs:
	@echo "Generating Documentation..."
	@if command -v godoc >/dev/null 2>&1; then \
		echo "Documentation Server: http://github.com/SusheelSathyaraj/DataMigrationTool/"
		godoc -http=:6060; \
	else \
		echo "godoc not installed. Run go install golang.org/x/tools/cmd/godoc@latest"; \
	fi

update-deps:
	@echo "Checking fo Dependcies update"
	@go list -u -m all
	@echo "To update: go get -u ./..."

profile: build
	@echo "Running CPU profiling..."
	@mkdir -p test_results
	@go test -cpuprofile=test_results/cpu.prof -bench=. ./...
	@echo "CPU profile: test_results/cpu/prof"
	@echo "View with: go tool pprof test_results/cpu.prof"

profile-mem: build
	@echo "Running Memory profiling"
	@mkdir -p test_results
	@go test -memprofle=test_results/mem.prof -bench=. ./...
	@echo "Memory Profile: test_results/mem/prof"
	@echo "View with: go tool pprof test_results/mem.prof"

release: test-all build-all
	@echo "Preparing Release $(VERSION)..."
	@mkdir -p release
	@cp dist/* release/
	@cp README.md LICENSE release/
	@cd release && tar -czf $(BINARY_NAME)-$(VERSION).tar.gz *
	@echo "Release Package: release/$(BINARY_NAME)-$(VERSION).tar.gz"

tag:
	@if [ -z "$(TAG)" ]; then \
		echo "TAG is required. Usage: make tag TAG=v1.0.0"; \
		exit 1; \
	fi
	@echo "Creating tag $(TAG)..."
	@git tag -a $(TAG) -m "Release $(TAG)"
	@git push origin $(TAG)
	@echo "Tag $(TAG) created and pushed"

cleanup-snapshots: build
	@echo "Cleaning old snapshots..."
	@$(BINARY_PATH) --cleanup-snapshots=168h
	@echo "Snapshot Cleanup Completed"

list-snapshots: build
	@echo "Listing migration snapshots..."
	@$(BINARY_PATH) --list-snapshots

stats:
	@echo "Project Statistics..."
	@echo "Total Files: $$(find . -name '*.go' | wc -l)"
	@echo "Lines of Code: $$(find . -name '*.go' -exec wc -l {} + | tail -n1 | awk '{print $$1}')"
	@echo "Test Files $$(find . -name '*_test.go' | wc -l)"
	@echo "Dependencies: $$(go list -m all | wc -l)"
	@echo "Latest Tag: $$(git describe --tags --abbrev=0 2>/dev/null || echo 'No Tags')"

##Help, Shows Available Commands
help:
	@echo "Data Migration Tool - Makefile Commands"
	@echo ""
	@echo "Development:"
	@echo "	fmt 			Format Go Code"
	@echo "	vet 			Run go vet analysis"
	@echo "	lint 			Run golangci-lint"
	@echo "	security 		Check for security vulnerabilities"
	@echo "	deps 			Install Dependencies"
	@echo "	audit 			Audit Dependencies for vulnerabilities"
	@echo ""
	@echo "	Build:"
	@echo "	clean	 		Clean build artifacts"
	@echo "	build	 		Build binary"
	@echo "	build-all 		Build for multiple platforms"
	@echo " install			Install binary to system"
	@echo ""
	@echo "	Run:"
	@echo "	run ARGS=\"...\" Run with arguments"
	@echo "	run-example		Run example migration"
	@echo "	version 		Show Version"
	@echo " app-help		Show application help"
	@echo ""
	@echo "	Testing:"
	@echo "	test			Run unit tests"
	@echo "	test-coverage	Run tests with coverage"
	@echo "	test-race	 	Run tests with race detection"
	@echo " test-bench		Run benchmark tests"
	@echo " test-integration	Run integration tests"
	@echo " test-all		Run comprehensive test suite"
	@echo ""
	@echo "	Utilities:"
	@echo "	mocks 			Generate mocks"
	@echo "	docs			Generate Documentation"
	@echo "	update-deps 	Check Dependcy updates"
	@echo " profile			CPU profiling"
	@echo " profile-mem		Memory profiling"
	@echo " stats			Show project Statistics"
	@echo ""
	@echo "	Maintenance:"
	@echo "	cleanup-snapshots 	Clean old migration snapshots"
	@echo "	list-snapshots	List migration snapshots"
	@echo ""
	@echo "	Examples:"
	@echo "	make run ARGS=\"--source=mysql --target=postgresql --mode=full\"
	@echo " make test-coverage"
	@echo " make release"
	@echo " make tag TAG=v1.2.0"	