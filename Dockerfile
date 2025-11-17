# Multi-stage build for minimal final image
FROM golang:1.22-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make gcc musl-dev

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s -X main.version=$(git describe --tags --always --dirty)" \
    -o tsdb \
    ./cmd/tsdb

# Run tests to ensure binary works
RUN go test -v -short ./...

# Final stage - minimal runtime image
FROM alpine:latest

# Install ca-certificates for HTTPS
RUN apk --no-cache add ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1000 tsdb && \
    adduser -D -u 1000 -G tsdb tsdb

# Set working directory
WORKDIR /app

# Copy binary from builder
COPY --from=builder /build/tsdb /usr/local/bin/tsdb

# Create data directory
RUN mkdir -p /data && chown -R tsdb:tsdb /data

# Switch to non-root user
USER tsdb

# Expose HTTP port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/-/healthy || exit 1

# Volume for persistent data
VOLUME ["/data"]

# Default command
ENTRYPOINT ["tsdb"]
CMD ["start", "--data-dir=/data", "--listen=:8080", "--log-format=json"]
