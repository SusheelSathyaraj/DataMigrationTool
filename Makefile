.DEFAULT_GOAL := build
.PHONY : fmt vet clean build run test
fmt:
	go fmt ./...
vet: fmt
	go vet ./...
clean: vet
	go clean
build: clean
	go build -o binary
run: build
	@echo "Running binary with arguments: $(ARGS)"
	./binary $(ARGS)
test: run
	go test -v ./...