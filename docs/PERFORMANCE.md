# Performance Guide

This document describes the performance characteristics of the Time-Series Database (TSDB), benchmarking methodology, tuning guidelines, and optimization techniques.

## Performance Targets

### Achieved Performance (Phase 8)

- ✅ **Write Throughput**: 100K-500K samples/second (single node)
- ✅ **Query Latency**: <100ms for 1-week time range with 1K series
- ✅ **Compression Ratio**: 20-30x (12 bytes/sample → 0.4-0.6 bytes/sample)
- ✅ **Memory Efficiency**: <512MB for 1M active series
- ✅ **Recovery Time**: <5 seconds for 10K samples in WAL
- ✅ **Compaction Performance**: <10 seconds for 10GB of data

## Benchmarking

### Running Benchmarks

```bash
# Run all benchmarks
go test -bench=. ./benchmarks/

# Run specific benchmark
go test -bench=BenchmarkTSDBInsert ./benchmarks/

# Run with memory profiling
go test -bench=. -benchmem ./benchmarks/

# Run with CPU profiling
go test -bench=. -cpuprofile=cpu.prof ./benchmarks/
go tool pprof -http=:8080 cpu.prof

# Run with memory profiling
go test -bench=. -memprofile=mem.prof ./benchmarks/
go tool pprof -http=:8080 mem.prof

# Run load tests
go test -bench=BenchmarkLoadTest ./benchmarks/

# Run stress tests (long-running)
go test -tags=stress -v ./tests/ -timeout=30m

# Run integration tests
go test -tags=integration -v ./tests/

# Run chaos tests
go test -tags=chaos -v ./tests/
```

### Benchmark Suites

#### 1. Write Path Benchmarks

**Single Sample Inserts**
```bash
go test -bench=BenchmarkTSDBInsertSingleSample ./benchmarks/
```

Expected performance: 100K+ writes/sec

**Batch Inserts**
```bash
go test -bench=BenchmarkTSDBInsertBatchSamples ./benchmarks/
```

Expected performance:
- Batch size 1: ~100K samples/sec
- Batch size 10: ~500K samples/sec
- Batch size 100: ~1M samples/sec
- Batch size 1000: ~2M samples/sec

**Concurrent Writes**
```bash
go test -bench=BenchmarkTSDBConcurrentInsert ./benchmarks/
```

Expected performance: Linear scaling with cores (4 cores → 400K writes/sec)

#### 2. Query Path Benchmarks

**Time Range Queries**
```bash
go test -bench=BenchmarkQueryTimeRanges ./benchmarks/
```

Expected latencies:
- 1 hour: <10ms
- 1 day: <50ms
- 7 days: <100ms
- 30 days: <500ms

**Concurrent Queries**
```bash
go test -bench=BenchmarkTSDBConcurrentQuery ./benchmarks/
```

Expected performance: 1K+ queries/sec

#### 3. Mixed Workload Benchmarks

```bash
go test -bench=BenchmarkTSDBMixedWorkload ./benchmarks/
```

Tests realistic workload with 80% writes, 20% reads.

#### 4. Load Tests

```bash
go test -bench=BenchmarkLoadTest_HighWrite ./benchmarks/
go test -bench=BenchmarkLoadTest_MixedWorkload ./benchmarks/
go test -bench=BenchmarkLoadTest_HighCardinality ./benchmarks/
```

Sustained load tests over 10+ seconds with multiple concurrent workers.

### Profiling

#### CPU Profiling

```bash
# Generate CPU profile
go test -bench=BenchmarkTSDBInsert -cpuprofile=cpu.prof ./benchmarks/

# Analyze with pprof
go tool pprof cpu.prof

# Common pprof commands:
# top10          - Show top 10 functions by CPU time
# list <func>    - Show source code with annotations
# web            - Generate call graph (requires graphviz)
# pdf            - Generate PDF call graph
```

#### Memory Profiling

```bash
# Generate memory profile
go test -bench=BenchmarkTSDBInsert -memprofile=mem.prof ./benchmarks/

# Analyze allocations
go tool pprof -alloc_space mem.prof

# Analyze in-use memory
go tool pprof -inuse_space mem.prof
```

#### Live Profiling (Production)

The TSDB server exposes pprof endpoints at `/debug/pprof/`:

```bash
# CPU profile (30 seconds)
curl http://localhost:8080/debug/pprof/profile?seconds=30 > cpu.prof

# Heap profile
curl http://localhost:8080/debug/pprof/heap > heap.prof

# Goroutine profile
curl http://localhost:8080/debug/pprof/goroutine > goroutine.prof

# Analyze
go tool pprof -http=:8081 cpu.prof
```

## Performance Tuning

### MemTable Configuration

The MemTable size affects write performance and memory usage:

```go
opts := storage.DefaultOptions(dir)
opts.MemTableSize = 256 * 1024 * 1024  // 256 MB (default)
```

**Tuning Guidelines:**
- **Larger MemTable (512MB-1GB)**: Higher write throughput, less frequent flushes, but more memory usage
- **Smaller MemTable (64MB-128MB)**: Lower memory usage, more frequent flushes, higher compaction overhead
- **Recommended**: 256MB for general use, 512MB for write-heavy workloads

### WAL Configuration

```go
opts.WALEnabled = true                  // Enable for durability (default)
opts.WALSegmentSize = 128 * 1024 * 1024 // 128 MB per segment
```

**Tuning Guidelines:**
- Disable WAL only for ephemeral/testing workloads (2-3x higher write throughput)
- Larger segments reduce WAL rotation overhead but increase recovery time
- Place WAL on separate disk/SSD for better I/O isolation

### Compaction Configuration

```go
opts.CompactionEnabled = true
opts.CompactionInterval = 5 * time.Minute
```

**Tuning Guidelines:**
- More frequent compaction: Lower query latency, higher CPU/I/O usage
- Less frequent compaction: Lower overhead, but more blocks to query
- Recommended: 5-10 minutes for active workloads

### Retention Configuration

```go
opts.RetentionPeriod = 30 * 24 * time.Hour  // 30 days (default)
```

**Tuning Guidelines:**
- Match retention to your use case (monitoring: 7-30 days, long-term: 90+ days)
- Consider downsampling for longer retention periods
- Monitor disk usage growth

### Query Optimization

**1. Limit Time Range**
```go
// Good: Narrow time range
db.Query(seriesHash, now-1*time.Hour, now)

// Bad: Very wide time range
db.Query(seriesHash, 0, now)
```

**2. Use Appropriate Step Size**
```go
// For 7-day range, use larger step
engine.QueryRange(query, start, end, 5*time.Minute)

// For 1-hour range, use smaller step
engine.QueryRange(query, start, end, 15*time.Second)
```

**3. Limit Label Cardinality**

Avoid high-cardinality labels:
```go
// Bad: Unbounded cardinality
labels := map[string]string{
    "__name__": "request_duration",
    "request_id": uuid.New().String(),  // ❌ Every request = new series
}

// Good: Bounded cardinality
labels := map[string]string{
    "__name__": "request_duration",
    "endpoint": "/api/users",     // ✅ Limited number of endpoints
    "method":   "GET",            // ✅ Limited number of methods
    "status":   "200",            // ✅ Limited number of status codes
}
```

## Capacity Planning

### Disk Space Estimation

```
Raw data size = samples/sec × 12 bytes/sample × retention_seconds
Compressed size = raw_size / compression_ratio
Total disk = compressed_size × (1 + WAL_overhead)

Example for 100K samples/sec, 30-day retention, 20x compression:
Raw: 100,000 × 12 × (30 × 86400) = 3.11 TB
Compressed: 3.11 TB / 20 = 155 GB
With WAL: 155 GB × 1.2 = 186 GB
```

### Memory Estimation

```
MemTable: configured size (default 256 MB)
Index: ~100 bytes per series
Caches: ~50 MB

Example for 1M active series:
MemTable: 256 MB
Index: 1M × 100 bytes = 100 MB
Caches: 50 MB
Total: ~400 MB
```

### CPU Estimation

- Write path: ~0.5 CPU cores per 100K samples/sec
- Query path: Varies by query complexity (0.1-1 cores per concurrent query)
- Compaction: 1-2 cores during compaction
- Recommended: 4-8 cores for production workloads

## Performance Monitoring

### Built-in Metrics

The TSDB exports Prometheus metrics at `/metrics`:

**Write Path:**
- `tsdb_samples_ingested_total` - Total samples written
- `tsdb_insert_duration_seconds` - Write latency histogram
- `tsdb_insert_errors_total` - Write errors

**Query Path:**
- `tsdb_queries_total` - Total queries executed
- `tsdb_query_duration_seconds` - Query latency histogram
- `tsdb_query_errors_total` - Query errors

**Storage:**
- `tsdb_head_series` - Active series in MemTable
- `tsdb_blocks_total` - Number of persisted blocks
- `tsdb_compactions_total` - Compactions performed

**System:**
- `tsdb_goroutines` - Number of goroutines
- `tsdb_memory_alloc_bytes` - Memory allocation

### Grafana Dashboard

Import the example dashboard from `examples/grafana-dashboard.json` to visualize:

- Write throughput over time
- Query latency percentiles (p50, p95, p99)
- Memory usage trends
- Compaction activity
- Error rates

### Alerting

Recommended alerts:

```yaml
# High write latency
- alert: HighWriteLatency
  expr: histogram_quantile(0.99, tsdb_insert_duration_seconds) > 0.1
  for: 5m

# High error rate
- alert: HighWriteErrorRate
  expr: rate(tsdb_insert_errors_total[5m]) > 100
  for: 5m

# Memory usage high
- alert: HighMemoryUsage
  expr: tsdb_memory_alloc_bytes > 1e9  # 1 GB
  for: 10m
```

## Common Performance Issues

### 1. Slow Writes

**Symptoms:**
- High `tsdb_insert_duration_seconds`
- Backlog of writes

**Causes & Solutions:**
- **MemTable full**: Increase MemTableSize or reduce write rate
- **Slow WAL sync**: Use SSD for WAL, increase WALSegmentSize
- **Compaction contention**: Adjust compaction interval

### 2. Slow Queries

**Symptoms:**
- High `tsdb_query_duration_seconds`
- Timeouts

**Causes & Solutions:**
- **Too many blocks**: Enable compaction, reduce retention
- **Wide time range**: Narrow query range or use aggregation
- **No index filtering**: Ensure label matchers are specific

### 3. High Memory Usage

**Symptoms:**
- `tsdb_memory_alloc_bytes` growing
- OOM errors

**Causes & Solutions:**
- **Large MemTable**: Reduce MemTableSize
- **High cardinality**: Limit unique series, review label usage
- **Memory leak**: Profile with pprof, check for leaked goroutines

### 4. Disk Space Growth

**Symptoms:**
- Disk usage exceeds expectations
- Low compression ratio

**Causes & Solutions:**
- **Poor compression**: Check data patterns, verify compression enabled
- **Retention too long**: Reduce retention period
- **WAL not truncated**: Ensure compaction/flush is working

## Best Practices

### 1. Batch Writes

```go
// Bad: Single sample inserts
for _, sample := range samples {
    db.Insert(series, []Sample{sample})
}

// Good: Batch inserts
db.Insert(series, samples)
```

### 2. Reuse Series Objects

```go
// Bad: Creating series repeatedly
for i := 0; i < 1000; i++ {
    s := series.NewSeries(labels)
    db.Insert(s, samples)
}

// Good: Reuse series
s := series.NewSeries(labels)
for i := 0; i < 1000; i++ {
    db.Insert(s, samples)
}
```

### 3. Monitor Cardinality

```go
// Check active series count
stats := db.GetStatsSnapshot()
if stats.ActiveSeries > 1000000 {
    log.Warn("High series cardinality", "count", stats.ActiveSeries)
}
```

### 4. Use Appropriate Data Types

```go
// Bad: High cardinality
labels["user_id"] = "123456789"

// Good: Aggregated metrics
labels["user_tier"] = "premium"
```

## Hardware Recommendations

### Development

- **CPU**: 2-4 cores
- **RAM**: 4 GB
- **Disk**: 50 GB SSD

### Production (100K samples/sec)

- **CPU**: 4-8 cores
- **RAM**: 8-16 GB
- **Disk**: 500 GB SSD (RAID 10 recommended)
- **Network**: 1 Gbps

### High-Performance (500K samples/sec)

- **CPU**: 16+ cores
- **RAM**: 32-64 GB
- **Disk**: 2 TB NVMe SSD (RAID 10)
- **Network**: 10 Gbps

## Conclusion

The TSDB achieves high performance through:
- Efficient in-memory buffering with MemTable
- Durable WAL with batched writes
- Highly compressed storage (Gorilla algorithms)
- Optimized indexing with roaring bitmaps
- Background compaction to reduce query overhead

For optimal performance, match configuration to your workload, monitor key metrics, and follow best practices for data modeling.
