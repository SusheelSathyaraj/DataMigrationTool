.DEFAULT_GOAL := build
.PHONY : fmt vet clean build run
fmt:
	go fmt
vet: fmt
	go vet
clean: vet
	go clean
build: clean
	go build -o binary
run: build
	./binary