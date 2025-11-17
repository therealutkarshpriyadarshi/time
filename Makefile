.PHONY: all build test bench clean docker-build docker-run lint fmt help

# Variables
BINARY_NAME=tsdb
DOCKER_IMAGE=ghcr.io/therealutkarshpriyadarshi/time
VERSION?=$(shell git describe --tags --always --dirty)
LDFLAGS=-ldflags="-w -s -X main.version=$(VERSION)"

# Default target
all: test build

# Build the binary
build:
	@echo "Building $(BINARY_NAME) $(VERSION)..."
	go build $(LDFLAGS) -o $(BINARY_NAME) ./cmd/tsdb

# Build for multiple platforms
build-all:
	@echo "Building for multiple platforms..."
	GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o $(BINARY_NAME)-linux-amd64 ./cmd/tsdb
	GOOS=linux GOARCH=arm64 go build $(LDFLAGS) -o $(BINARY_NAME)-linux-arm64 ./cmd/tsdb
	GOOS=darwin GOARCH=amd64 go build $(LDFLAGS) -o $(BINARY_NAME)-darwin-amd64 ./cmd/tsdb
	GOOS=darwin GOARCH=arm64 go build $(LDFLAGS) -o $(BINARY_NAME)-darwin-arm64 ./cmd/tsdb
	GOOS=windows GOARCH=amd64 go build $(LDFLAGS) -o $(BINARY_NAME)-windows-amd64.exe ./cmd/tsdb

# Run tests
test:
	@echo "Running tests..."
	go test -v -race -cover ./...

# Run unit tests only (short mode)
test-short:
	@echo "Running short tests..."
	go test -v -short ./...

# Run integration tests
test-integration:
	@echo "Running integration tests..."
	go test -v -tags=integration ./tests/

# Run stress tests
test-stress:
	@echo "Running stress tests (this may take a while)..."
	go test -v -tags=stress -timeout=30m ./tests/

# Run chaos tests
test-chaos:
	@echo "Running chaos tests..."
	go test -v -tags=chaos ./tests/

# Run all test suites
test-all: test test-integration test-stress test-chaos

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	go test -bench=. -benchmem ./benchmarks/

# Run benchmarks with profiling
bench-cpu:
	@echo "Running CPU profiling..."
	go test -bench=. -cpuprofile=cpu.prof ./benchmarks/
	go tool pprof -http=:8081 cpu.prof

bench-mem:
	@echo "Running memory profiling..."
	go test -bench=. -memprofile=mem.prof ./benchmarks/
	go tool pprof -http=:8081 mem.prof

# Generate test coverage
coverage:
	@echo "Generating coverage report..."
	go test -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

# Lint code
lint:
	@echo "Running linter..."
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)
	golangci-lint run ./...

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...
	gofmt -s -w .

# Tidy dependencies
tidy:
	@echo "Tidying dependencies..."
	go mod tidy

# Docker build
docker-build:
	@echo "Building Docker image..."
	docker build -t $(DOCKER_IMAGE):$(VERSION) .
	docker tag $(DOCKER_IMAGE):$(VERSION) $(DOCKER_IMAGE):latest

# Docker run
docker-run:
	@echo "Running TSDB in Docker..."
	docker run -d \
		--name tsdb \
		-p 8080:8080 \
		-v tsdb-data:/data \
		$(DOCKER_IMAGE):latest

# Docker compose up
docker-compose-up:
	@echo "Starting services with docker-compose..."
	docker-compose up -d

# Docker compose down
docker-compose-down:
	@echo "Stopping services..."
	docker-compose down

# Install binary
install: build
	@echo "Installing $(BINARY_NAME) to /usr/local/bin..."
	sudo mv $(BINARY_NAME) /usr/local/bin/

# Clean build artifacts
clean:
	@echo "Cleaning..."
	rm -f $(BINARY_NAME)
	rm -f $(BINARY_NAME)-*
	rm -f *.prof
	rm -f *.out
	rm -f *.test
	rm -f coverage.html
	rm -rf data/
	go clean -cache -testcache

# Run the server locally
run: build
	@echo "Starting TSDB server..."
	./$(BINARY_NAME) start --data-dir=./data --log-level=debug

# Development mode (with auto-rebuild)
dev:
	@echo "Running in development mode..."
	@which air > /dev/null || (echo "Installing air..." && go install github.com/cosmtrek/air@latest)
	air

# Help
help:
	@echo "Available targets:"
	@echo "  make build              - Build the binary"
	@echo "  make build-all          - Build for all platforms"
	@echo "  make test               - Run all unit tests"
	@echo "  make test-short         - Run short tests only"
	@echo "  make test-integration   - Run integration tests"
	@echo "  make test-stress        - Run stress tests"
	@echo "  make test-chaos         - Run chaos tests"
	@echo "  make test-all           - Run all test suites"
	@echo "  make bench              - Run benchmarks"
	@echo "  make bench-cpu          - Run CPU profiling"
	@echo "  make bench-mem          - Run memory profiling"
	@echo "  make coverage           - Generate test coverage report"
	@echo "  make lint               - Run linter"
	@echo "  make fmt                - Format code"
	@echo "  make tidy               - Tidy dependencies"
	@echo "  make docker-build       - Build Docker image"
	@echo "  make docker-run         - Run in Docker"
	@echo "  make docker-compose-up  - Start with docker-compose"
	@echo "  make docker-compose-down- Stop docker-compose services"
	@echo "  make install            - Install binary to /usr/local/bin"
	@echo "  make clean              - Clean build artifacts"
	@echo "  make run                - Run server locally"
	@echo "  make dev                - Run in development mode (auto-rebuild)"
	@echo "  make help               - Show this help message"
