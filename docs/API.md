# TSDB HTTP API Reference

This document describes the HTTP API for the Time-Series Database (TSDB). The API is designed to be compatible with Prometheus remote write/read protocols where applicable.

## Table of Contents

- [Overview](#overview)
- [API Endpoints](#api-endpoints)
  - [Write Endpoints](#write-endpoints)
  - [Query Endpoints](#query-endpoints)
  - [Metadata Endpoints](#metadata-endpoints)
  - [Admin Endpoints](#admin-endpoints)
  - [Health Endpoints](#health-endpoints)
- [Data Formats](#data-formats)
- [Error Handling](#error-handling)
- [Examples](#examples)

## Overview

**Base URL**: `http://localhost:8080` (default)

**Content-Type**: `application/json`

All API endpoints return JSON responses with a consistent structure.

## API Endpoints

### Write Endpoints

#### Write Metrics

Writes time-series metrics to the TSDB using the Prometheus remote write format.

**Endpoint**: `POST /api/v1/write`

**Request Body**:
```json
{
  "timeseries": [
    {
      "labels": [
        {"name": "__name__", "value": "cpu_usage"},
        {"name": "host", "value": "server1"}
      ],
      "samples": [
        {"timestamp": 1640000000000, "value": 0.75},
        {"timestamp": 1640000060000, "value": 0.82}
      ]
    }
  ]
}
```

**Response**: `204 No Content` on success

**Example**:
```bash
curl -X POST http://localhost:8080/api/v1/write \
  -H "Content-Type: application/json" \
  -d '{
    "timeseries": [{
      "labels": [
        {"name": "__name__", "value": "cpu_usage"},
        {"name": "host", "value": "server1"}
      ],
      "samples": [
        {"timestamp": 1640000000000, "value": 0.75}
      ]
    }]
  }'
```

### Query Endpoints

#### Instant Query

Executes an instant query at a specific point in time.

**Endpoint**: `GET /api/v1/query`

**Parameters**:
- `query` (required): Label matchers in format `{label="value",...}`
- `time` (optional): Unix timestamp in milliseconds (default: now)

**Response**:
```json
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": {"__name__": "cpu_usage", "host": "server1"},
        "value": [1640000000000, "0.75"]
      }
    ]
  }
}
```

**Example**:
```bash
curl 'http://localhost:8080/api/v1/query?query={__name__="cpu_usage",host="server1"}'
```

#### Range Query

Executes a range query over a time period.

**Endpoint**: `GET /api/v1/query_range`

**Parameters**:
- `query` (required): Label matchers in format `{label="value",...}`
- `start` (required): Start time in Unix milliseconds
- `end` (required): End time in Unix milliseconds
- `step` (optional): Step duration in milliseconds (default: 60000 = 1 minute)

**Response**:
```json
{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {"__name__": "cpu_usage", "host": "server1"},
        "values": [
          [1640000000000, "0.75"],
          [1640000060000, "0.82"],
          [1640000120000, "0.68"]
        ]
      }
    ]
  }
}
```

**Example**:
```bash
curl 'http://localhost:8080/api/v1/query_range?query={__name__="cpu_usage",host="server1"}&start=1640000000000&end=1640003600000&step=60000'
```

### Metadata Endpoints

#### List Labels

Returns all unique label names across all series.

**Endpoint**: `GET /api/v1/labels`

**Response**:
```json
{
  "status": "success",
  "data": ["__name__", "host", "region", "environment"]
}
```

**Example**:
```bash
curl http://localhost:8080/api/v1/labels
```

#### List Label Values

Returns all values for a specific label.

**Endpoint**: `GET /api/v1/label/<label_name>/values`

**Response**:
```json
{
  "status": "success",
  "data": ["server1", "server2", "server3"]
}
```

**Example**:
```bash
curl http://localhost:8080/api/v1/label/host/values
```

#### List Series

Returns all series matching the provided label matchers.

**Endpoint**: `GET /api/v1/series`

**Parameters**:
- `match[]` (required): One or more label matchers

**Response**:
```json
{
  "status": "success",
  "data": [
    {"__name__": "cpu_usage", "host": "server1"},
    {"__name__": "cpu_usage", "host": "server2"}
  ]
}
```

**Example**:
```bash
curl 'http://localhost:8080/api/v1/series?match[]={__name__="cpu_usage"}'
```

### Admin Endpoints

#### TSDB Status

Returns TSDB status and statistics.

**Endpoint**: `GET /api/v1/status/tsdb`

**Response**:
```json
{
  "status": "success",
  "data": {
    "totalSamples": 1000000,
    "totalSeries": 5000,
    "flushCount": 10,
    "lastFlushTime": 1640000000000,
    "walSize": 10485760,
    "activeMemTableSize": 2097152
  }
}
```

**Example**:
```bash
curl http://localhost:8080/api/v1/status/tsdb
```

### Health Endpoints

#### Health Check

Returns 200 if the server is healthy.

**Endpoint**: `GET /-/healthy`

**Response**:
```json
{
  "status": "healthy",
  "message": "TSDB is operational"
}
```

**Example**:
```bash
curl http://localhost:8080/-/healthy
```

#### Readiness Check

Returns 200 if the server is ready to serve requests.

**Endpoint**: `GET /-/ready`

**Response**:
```json
{
  "status": "ready",
  "message": "TSDB is ready to serve requests"
}
```

**Example**:
```bash
curl http://localhost:8080/-/ready
```

## Data Formats

### Label Matcher Format

Label matchers are used in queries to select specific time series.

**Format**: `{label1="value1",label2="value2",...}`

**Supported Operators**:
- `=`: Equal
- `!=`: Not equal
- `=~`: Regex match
- `!~`: Regex not match

**Examples**:
```
{__name__="cpu_usage"}                          # Single label
{__name__="cpu_usage",host="server1"}           # Multiple labels
{host!="server1"}                               # Not equal
{host=~"server.*"}                              # Regex match
{__name__="cpu_usage",host!~"test.*"}           # Regex not match
```

### Timestamp Format

Timestamps are represented as Unix milliseconds (milliseconds since epoch).

**Examples**:
- `1640000000000` - January 1, 2022 00:00:00 UTC
- `1640000060000` - January 1, 2022 00:01:00 UTC

### Value Format

Values are floating-point numbers represented as float64.

**Examples**:
- `0.75` - CPU usage percentage
- `1024.5` - Memory in MB
- `-10.2` - Temperature in Celsius

## Error Handling

All error responses follow this format:

```json
{
  "status": "error",
  "error": "Error message describing what went wrong"
}
```

**Common HTTP Status Codes**:
- `200 OK` - Request succeeded
- `204 No Content` - Write succeeded
- `400 Bad Request` - Invalid request parameters
- `405 Method Not Allowed` - HTTP method not supported
- `500 Internal Server Error` - Server-side error

## Examples

### Writing Data

#### Write Single Metric
```bash
curl -X POST http://localhost:8080/api/v1/write \
  -H "Content-Type: application/json" \
  -d '{
    "timeseries": [{
      "labels": [
        {"name": "__name__", "value": "cpu_usage"},
        {"name": "host", "value": "server1"}
      ],
      "samples": [
        {"timestamp": 1640000000000, "value": 0.75}
      ]
    }]
  }'
```

#### Write Multiple Metrics
```bash
curl -X POST http://localhost:8080/api/v1/write \
  -H "Content-Type: application/json" \
  -d '{
    "timeseries": [
      {
        "labels": [
          {"name": "__name__", "value": "cpu_usage"},
          {"name": "host", "value": "server1"}
        ],
        "samples": [
          {"timestamp": 1640000000000, "value": 0.75},
          {"timestamp": 1640000060000, "value": 0.82}
        ]
      },
      {
        "labels": [
          {"name": "__name__", "value": "memory_usage"},
          {"name": "host", "value": "server1"}
        ],
        "samples": [
          {"timestamp": 1640000000000, "value": 1024.5}
        ]
      }
    ]
  }'
```

### Querying Data

#### Instant Query
```bash
# Query current CPU usage
curl 'http://localhost:8080/api/v1/query?query={__name__="cpu_usage",host="server1"}'

# Query at specific time
curl 'http://localhost:8080/api/v1/query?query={__name__="cpu_usage",host="server1"}&time=1640000000000'
```

#### Range Query
```bash
# Query last hour with 1-minute steps
START=$(date -d '1 hour ago' +%s)000
END=$(date +%s)000
curl "http://localhost:8080/api/v1/query_range?query={__name__=\"cpu_usage\",host=\"server1\"}&start=$START&end=$END&step=60000"
```

### Metadata Queries

#### List All Labels
```bash
curl http://localhost:8080/api/v1/labels
```

#### List Host Values
```bash
curl http://localhost:8080/api/v1/label/host/values
```

#### Find Series
```bash
# Find all CPU metrics
curl 'http://localhost:8080/api/v1/series?match[]={__name__="cpu_usage"}'

# Find series with multiple matchers
curl 'http://localhost:8080/api/v1/series?match[]={__name__="cpu_usage"}&match[]={__name__="memory_usage"}'
```

### Health Checks

```bash
# Check if server is healthy
curl http://localhost:8080/-/healthy

# Check if server is ready
curl http://localhost:8080/-/ready
```

### Status Information

```bash
# Get TSDB statistics
curl http://localhost:8080/api/v1/status/tsdb | jq .
```

## Go Client Library

For programmatic access, use the Go client library:

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/therealutkarshpriyadarshi/time/pkg/client"
)

func main() {
    // Create client
    c := client.NewClient("http://localhost:8080")

    // Write metrics
    err := c.Write(context.Background(), []client.Metric{
        {
            Labels: map[string]string{
                "__name__": "cpu_usage",
                "host":     "server1",
            },
            Timestamp: time.Now(),
            Value:     0.85,
        },
    })
    if err != nil {
        panic(err)
    }

    // Query data
    results, err := c.QueryRange(
        context.Background(),
        `{__name__="cpu_usage",host="server1"}`,
        time.Now().Add(-1*time.Hour),
        time.Now(),
        1*time.Minute,
    )
    if err != nil {
        panic(err)
    }

    fmt.Printf("Found %d series\n", len(results))
}
```

## CLI Tool

Use the `tsdb` CLI for command-line operations:

```bash
# Start server
tsdb start --listen=:8080 --data-dir=./data --retention=30d

# Write data
tsdb write 'cpu_usage{host="server1"}' 0.85

# Query data
tsdb query 'cpu_usage{host="server1"}' --start=-1h --end=now

# Inspect status
tsdb inspect status
tsdb inspect labels
tsdb inspect label-values host
```

## Best Practices

1. **Batch Writes**: Write multiple samples in a single request for better performance
2. **Use Appropriate Labels**: Use meaningful label names and values
3. **Avoid High Cardinality**: Limit unique label combinations to avoid cardinality explosion
4. **Query Optimization**: Use specific label matchers to reduce query scope
5. **Retention Policy**: Set appropriate retention based on storage capacity
6. **Health Checks**: Monitor the health endpoints for operational status

## Performance

- **Write Throughput**: 100K-500K samples/second
- **Query Latency**: <100ms for 1-week range with 1K series
- **Compression**: 10-20x compression ratio
- **Memory**: <512MB for 1M active series

## Support

For issues or questions:
- GitHub: https://github.com/therealutkarshpriyadarshi/time
- Documentation: https://github.com/therealutkarshpriyadarshi/time/tree/main/docs
