# TSDB Examples

This directory contains example programs demonstrating how to use the TSDB Go client library.

## Prerequisites

Make sure the TSDB server is running:

```bash
# Start the server
tsdb start --listen=:8080 --data-dir=./data
```

Or use the compiled binary:

```bash
go run cmd/tsdb/main.go start --listen=:8080 --data-dir=./data
```

## Examples

### Basic Client Usage

Demonstrates basic operations: write, query, range query, metadata.

```bash
go run examples/basic_client.go
```

**What it does:**
- Writes CPU, memory, and disk metrics
- Queries instant values
- Performs range queries
- Fetches labels and label values
- Checks server health

### Monitoring Example

Simulates a monitoring system collecting metrics from multiple servers.

```bash
go run examples/monitoring_example.go
```

**What it does:**
- Simulates 3 servers
- Writes metrics every 10 seconds for 1 minute
- Collects CPU, memory, disk, and HTTP request metrics
- Queries and analyzes the collected data
- Calculates statistics (avg, min, max)

## Client Library Features

The examples demonstrate these key features:

### Writing Metrics

```go
c := client.NewClient("http://localhost:8080")

metrics := []client.Metric{
    {
        Labels: map[string]string{
            "__name__": "cpu_usage",
            "host":     "server1",
        },
        Timestamp: time.Now(),
        Value:     0.75,
    },
}

err := c.Write(context.Background(), metrics)
```

### Instant Query

```go
results, err := c.Query(
    context.Background(),
    `{__name__="cpu_usage",host="server1"}`,
    time.Now(),
)
```

### Range Query

```go
results, err := c.QueryRange(
    context.Background(),
    `{__name__="cpu_usage"}`,
    time.Now().Add(-1*time.Hour),
    time.Now(),
    1*time.Minute,
)
```

### Metadata Queries

```go
// Get all labels
labels, err := c.Labels(context.Background())

// Get values for a specific label
values, err := c.LabelValues(context.Background(), "host")
```

### Health Check

```go
healthy, err := c.Health(context.Background())
```

## Custom Examples

You can create your own examples by:

1. Creating a new `.go` file in this directory
2. Importing the client package:
   ```go
   import "github.com/therealutkarshpriyadarshi/time/pkg/client"
   ```
3. Creating a client and using the API
4. Running with `go run examples/your_example.go`

## API Reference

For complete API documentation, see [docs/API.md](../docs/API.md).

## Tips

- **Batching**: Write multiple metrics in a single request for better performance
- **Context Timeouts**: Always use context with timeouts for requests
- **Error Handling**: Check errors and implement retry logic for production use
- **Label Naming**: Use consistent label naming conventions
- **Cardinality**: Avoid high-cardinality labels (labels with many unique values)

## Troubleshooting

### Connection Refused

If you get "connection refused" errors:
1. Make sure the TSDB server is running
2. Check that the server address matches (default: `http://localhost:8080`)
3. Verify no firewall is blocking the connection

### Query Returns No Results

If queries return no results:
1. Wait a moment after writing before querying (data needs to be flushed)
2. Check that your label matchers are correct
3. Verify the time range includes the written data
4. Use the `/api/v1/labels` endpoint to see available labels

### Performance Issues

For better performance:
1. Batch write multiple metrics together
2. Use specific label matchers in queries
3. Limit query time ranges
4. Enable compaction and retention in the server
