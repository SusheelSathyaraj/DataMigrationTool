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
	@echo "Running binary with arguments: $(ARGS)"
	./binary $(ARGS)
test: run
	go test -v ./...