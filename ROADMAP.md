# Time-Series Database in Go - Project Roadmap

## Overview
Build a production-grade time-series database optimized for monitoring metrics and observability data, inspired by Prometheus, InfluxDB, and VictoriaMetrics.

**Tech Stack**: Go, custom storage format or BadgerDB, delta-of-delta and XOR compression (Gorilla paper), PromQL-like query language

**Performance Targets**:
- Write throughput: 100K-500K data points/second
- Query latency: <100ms for 1-week time range
- Compression ratio: 10-20x
- Memory efficiency: <512MB for 1M active series

**Timeline**: 8-12 weeks

---

## Project Structure

```
tsdb/
├── cmd/
│   └── tsdb/           # Main binary & CLI
├── pkg/
│   ├── storage/        # Storage engine core
│   ├── wal/            # Write-ahead log
│   ├── series/         # Time-series management
│   ├── index/          # Label indexing (inverted index)
│   ├── compression/    # Delta-of-delta & XOR compression
│   ├── query/          # Query engine & aggregations
│   └── api/            # HTTP API server
├── internal/
│   ├── bitmap/         # Roaring bitmap utilities
│   └── util/           # Helper functions
├── testdata/           # Test datasets & fixtures
├── benchmarks/         # Performance benchmarks
├── docs/               # Documentation
│   ├── DESIGN.md       # Architecture & design decisions
│   ├── API.md          # HTTP API reference
│   └── COMPRESSION.md  # Compression algorithms explained
├── docker/             # Docker setup
├── examples/           # Usage examples
├── go.mod
├── go.sum
├── README.md
├── ROADMAP.md          # This file
└── LICENSE
```

---

## Phase 1: Foundation & Core Data Structures (Week 1-2)

### Milestone 1.1: Project Setup
**Goal**: Initialize project structure and dependencies

**Tasks**:
- [ ] Initialize Go module: `go mod init github.com/yourusername/tsdb`
- [ ] Create directory structure as shown above
- [ ] Setup `.gitignore` for Go projects
- [ ] Add initial dependencies:
  ```bash
  go get github.com/dgraph-io/badger/v3        # Optional KV store
  go get github.com/RoaringBitmap/roaring      # Bitmap indexes
  go get github.com/prometheus/prometheus/model/labels  # Label handling
  ```
- [ ] Setup testing framework with table-driven tests
- [ ] Configure GitHub Actions for CI/CD
- [ ] Create basic README with project goals

**Deliverables**:
- Project skeleton with proper Go structure
- Initial test suite passing
- CI pipeline running

---

### Milestone 1.2: Core Data Model
**Goal**: Define fundamental data structures for time-series data

**Tasks**:
- [ ] Implement `Sample` struct (timestamp + value)
- [ ] Implement `Series` struct with label support
- [ ] Create efficient series hashing (for label matching)
- [ ] Design `MemTable` for in-memory buffer
- [ ] Add unit tests for all data structures

**Code to implement**:

```go
// pkg/series/types.go
type Sample struct {
    Timestamp int64   // Unix milliseconds
    Value     float64
}

type Series struct {
    Labels map[string]string // e.g., {__name__: "cpu_usage", host: "server1"}
    Hash   uint64            // Fast lookup key
}

// pkg/storage/memtable.go
type MemTable struct {
    series map[uint64][]Sample // seriesHash -> samples
    size   int64                // bytes used
    mu     sync.RWMutex
}
```

**Deliverables**:
- `pkg/series/` with core types
- `pkg/storage/memtable.go` with thread-safe operations
- Unit tests with 80%+ coverage
- Benchmarks for series hashing

**Performance target**: Hash 1M series in <1 second

---

## Phase 2: Write Path - Ingestion Pipeline (Week 2-3)

### Milestone 2.1: Write-Ahead Log (WAL)
**Goal**: Implement durability for crash recovery

**Tasks**:
- [ ] Design WAL file format (header, entries, checksums)
- [ ] Implement append-only log writer
- [ ] Add segment rotation (128MB per segment)
- [ ] Implement WAL replay for recovery
- [ ] Add compression for WAL segments
- [ ] Implement WAL truncation after flush

**Code to implement**:

```go
// pkg/wal/wal.go
type WAL struct {
    dir        string
    file       *os.File
    buf        *bufio.Writer
    mu         sync.Mutex
    segmentNum int
}

func (w *WAL) Append(series Series, samples []Sample) error
func (w *WAL) Replay() ([]Entry, error)
func (w *WAL) Truncate(beforeTimestamp int64) error
func (w *WAL) Rotate() error
```

**WAL Entry Format**:
```
[4 bytes: length][8 bytes: checksum][N bytes: protobuf/msgpack data]
```

**Deliverables**:
- `pkg/wal/` with full WAL implementation
- Crash recovery tests (kill process mid-write)
- Performance tests (sequential write speed)
- Documentation on WAL format

**Performance target**: 50K+ WAL writes/second

---

### Milestone 2.2: In-Memory Buffer & Flush
**Goal**: Fast writes with background persistence

**Tasks**:
- [ ] Implement thread-safe `MemTable` insert
- [ ] Add memory usage tracking
- [ ] Implement flush trigger (size threshold: 256MB)
- [ ] Create background flusher goroutine
- [ ] Add double-buffering (active + flushing MemTable)
- [ ] Coordinate WAL writes with MemTable inserts

**Code to implement**:

```go
// pkg/storage/tsdb.go
type TSDB struct {
    activeMemTable   *MemTable
    flushingMemTable *MemTable
    walWriter        *WAL
    mu               sync.RWMutex
    flushChan        chan struct{}
}

func (db *TSDB) Insert(series Series, samples []Sample) error {
    // 1. Write to WAL (durability)
    // 2. Insert to activeMemTable
    // 3. Trigger flush if size > threshold
}

func (db *TSDB) backgroundFlusher() {
    // Periodically flush MemTable to disk blocks
}
```

**Deliverables**:
- `pkg/storage/tsdb.go` with TSDB orchestration
- Concurrent write benchmarks
- Memory usage tests
- Load testing with 100K writes/sec

**Performance target**: 100K+ inserts/second with WAL enabled

---

## Phase 3: Storage Engine - Persistence (Week 3-5)

### Milestone 3.1: Time-Partitioned Blocks
**Goal**: Organize data into immutable time windows

**Tasks**:
- [ ] Design block directory structure
- [ ] Implement block metadata (min/max time, series count)
- [ ] Create block writer (flush MemTable → block)
- [ ] Add ULID-based block naming
- [ ] Implement block reader
- [ ] Add block validation and repair

**Block Directory Layout**:
```
data/
├── 01H8XABC00000000/   # Block ID (ULID, sortable by time)
│   ├── meta.json       # Block metadata
│   ├── index           # Inverted index file
│   ├── chunks/         # Compressed chunks directory
│   │   ├── 000001
│   │   ├── 000002
│   │   └── ...
│   └── tombstones      # Deletion markers
└── 01H8XDEF00000000/
    └── ...
```

**meta.json format**:
```json
{
  "ulid": "01H8XABC00000000",
  "minTime": 1640000000000,
  "maxTime": 1640007200000,
  "stats": {
    "numSamples": 1000000,
    "numSeries": 5000,
    "numChunks": 10000
  },
  "version": 1
}
```

**Deliverables**:
- `pkg/storage/block.go` with block I/O
- Block validation tests
- Documentation on block format

**Configuration**: 2-hour blocks (configurable)

---

### Milestone 3.2: Compression Algorithms
**Goal**: Achieve 10-20x compression ratio using Gorilla paper algorithms

**Tasks**:
- [ ] Implement delta-of-delta encoding for timestamps
- [ ] Implement XOR floating-point compression for values
- [ ] Create bit-level writer/reader
- [ ] Add compression benchmarks
- [ ] Optimize for CPU cache efficiency

**Code to implement**:

```go
// pkg/compression/timestamp.go
type TimestampEncoder struct {
    t0        int64      // First timestamp
    tDelta    int64      // Previous delta
    buf       *BitWriter
}

func (e *TimestampEncoder) Encode(t int64) error {
    // Delta-of-delta encoding (Gorilla paper)
}

// pkg/compression/value.go
type ValueEncoder struct {
    prevValue    uint64
    prevLeading  uint8
    prevTrailing uint8
    buf          *BitWriter
}

func (e *ValueEncoder) Encode(v float64) error {
    // XOR compression with leading/trailing zeros
}
```

**Reference**: [Gorilla Paper](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf) - Facebook's time-series compression

**Deliverables**:
- `pkg/compression/` with encoder/decoder
- Unit tests verifying correctness
- Benchmarks showing compression ratios
- Performance tests (encode/decode speed)

**Performance targets**:
- Compression ratio: 12 bytes/sample → 1.5 bytes/sample (8x)
- Encode speed: 10M+ samples/second
- Decode speed: 50M+ samples/second

---

### Milestone 3.3: Chunk Storage Format
**Goal**: Efficient on-disk layout for compressed samples

**Tasks**:
- [ ] Design chunk format specification
- [ ] Implement chunk writer with compression
- [ ] Implement chunk reader with decompression
- [ ] Add chunk index for fast seeks
- [ ] Optimize chunk size (default: 120 samples, ~2 hours @ 1m interval)
- [ ] Add checksum verification

**Chunk Format**:
```
Chunk Header (24 bytes):
  [8 bytes: minTime]
  [8 bytes: maxTime]
  [2 bytes: numSamples]
  [4 bytes: dataLength]
  [2 bytes: encoding flags]

Chunk Data:
  [N bytes: compressed timestamps]
  [M bytes: compressed values]

Chunk Footer:
  [4 bytes: CRC32 checksum]
```

**Chunk File Layout**:
```
[ChunkHeader1][ChunkData1][ChunkFooter1]
[ChunkHeader2][ChunkData2][ChunkFooter2]
...
```

**Code to implement**:

```go
// pkg/storage/chunk.go
type Chunk struct {
    MinTime    int64
    MaxTime    int64
    NumSamples uint16
    Data       []byte  // Compressed
    Encoding   uint16
}

func (c *Chunk) Append(t int64, v float64) error
func (c *Chunk) Iterator() Iterator
func (c *Chunk) MarshalBinary() ([]byte, error)
func (c *Chunk) UnmarshalBinary(data []byte) error
```

**Deliverables**:
- `pkg/storage/chunk.go` with chunk implementation
- Flush MemTable → disk blocks working end-to-end
- Integration tests (write → flush → read)
- Corruption detection tests

---

## Phase 4: Indexing - Fast Series Lookup (Week 5-6) ✅ COMPLETED

### Milestone 4.1: Inverted Index
**Goal**: Enable fast label-based queries (e.g., `{host="server1", metric="cpu"}`)

**Tasks**:
- [x] Design inverted index structure
- [x] Implement posting list storage
- [x] Add roaring bitmap for set operations
- [x] Implement label matchers (=, !=, =~, !~)
- [x] Optimize for high-cardinality labels
- [x] Add index persistence to disk

**Index Structure**:
```
labelName -> labelValue -> PostingsList (roaring bitmap of seriesIDs)

Example:
"host" -> "server1" -> [seriesID: 1, 5, 42, 100, ...]
"host" -> "server2" -> [seriesID: 2, 6, 43, 101, ...]
"metric" -> "cpu" -> [seriesID: 1, 2, 3, 4, ...]
```

**Code to implement**:

```go
// pkg/index/inverted.go
type InvertedIndex struct {
    // labelName -> labelValue -> PostingsList
    postings map[string]map[string]*roaring.Bitmap

    // seriesID -> Labels (reverse lookup)
    series map[uint64]map[string]string

    mu sync.RWMutex
}

func (idx *InvertedIndex) Add(seriesID uint64, labels map[string]string) error
func (idx *InvertedIndex) Lookup(matchers []LabelMatcher) *roaring.Bitmap
```

**Query Examples**:
```go
// Find: {host="server1" AND metric="cpu"}
matcher1 := LabelMatcher{Name: "host", Value: "server1", Type: Equal}
matcher2 := LabelMatcher{Name: "metric", Value: "cpu", Type: Equal}
seriesIDs := idx.Lookup([]LabelMatcher{matcher1, matcher2})
// Uses bitmap intersection: postings["host"]["server1"] ∩ postings["metric"]["cpu"]
```

**Deliverables**:
- `pkg/index/` with inverted index
- Support for all matcher types (=, !=, =~, !~)
- Benchmarks with 1M+ series
- Documentation on index internals

**Performance target**: Query 10M series in <10ms

---

### Milestone 4.2: Series ID Management
**Goal**: Compact series references and efficient storage

**Tasks**:
- [x] Implement series ID allocator (monotonic counter)
- [x] Create series metadata storage
- [x] Add series hash → ID mapping
- [x] Implement series lookup cache (LRU)
- [x] Add series cardinality limits
- [x] Track series churn rate

**Code to implement**:

```go
// pkg/series/registry.go
type SeriesRef uint64

type SeriesRegistry struct {
    nextID    atomic.Uint64
    hashToID  map[uint64]SeriesRef
    idToMeta  map[SeriesRef]*SeriesMeta
    cache     *lru.Cache
    mu        sync.RWMutex
}

func (r *SeriesRegistry) GetOrCreate(labels map[string]string) (SeriesRef, error)
func (r *SeriesRegistry) Get(ref SeriesRef) (*SeriesMeta, error)
```

**Deliverables**:
- `pkg/series/registry.go`
- Cardinality explosion protection
- Series churn metrics

---

## Phase 5: Read Path - Query Engine (Week 6-8)

### Milestone 5.1: Time-Range Queries
**Goal**: Fetch raw data efficiently

**Tasks**:
- [ ] Implement query planner
- [ ] Create series iterator interface
- [ ] Add block overlap detection
- [ ] Implement chunk merging (MemTable + blocks)
- [ ] Add query result streaming
- [ ] Optimize with chunk skipping (min/max time)

**Code to implement**:

```go
// pkg/query/engine.go
type QueryEngine struct {
    db *storage.TSDB
}

type Query struct {
    Matchers []LabelMatcher
    MinTime  int64
    MaxTime  int64
}

func (qe *QueryEngine) Select(q *Query) ([]SeriesIterator, error) {
    // 1. Index lookup → series IDs
    // 2. Find overlapping blocks (MemTable + disk blocks)
    // 3. Create iterators for each series
    // 4. Return iterator set
}

type SeriesIterator interface {
    Next() bool
    At() (int64, float64)
    Err() error
    Labels() map[string]string
}
```

**Query Execution Plan**:
1. Parse query → label matchers
2. Index lookup → seriesID list
3. Find blocks: `[minTime, maxTime]` overlap
4. For each block: read chunks for selected series
5. Decompress chunks → samples
6. Merge results from multiple blocks
7. Return sorted iterator

**Deliverables**:
- `pkg/query/engine.go` with Select implementation
- Iterator implementations for MemTable and blocks
- Query tests with various time ranges
- Performance benchmarks

**Performance target**: <100ms for 1-week query with 1000 series

---

### Milestone 5.2: Aggregations & Downsampling
**Goal**: Implement sum, avg, max, min, count over time ranges

**Tasks**:
- [ ] Implement time-bucketing (step parameter)
- [ ] Add aggregation functions (sum, avg, max, min, count)
- [ ] Implement rate() calculation
- [ ] Add increase() function
- [ ] Optimize with pre-aggregated chunks (optional)
- [ ] Support group-by labels

**Code to implement**:

```go
// pkg/query/aggregation.go
type AggregateFunc string

const (
    Sum   AggregateFunc = "sum"
    Avg   AggregateFunc = "avg"
    Max   AggregateFunc = "max"
    Min   AggregateFunc = "min"
    Count AggregateFunc = "count"
)

func (qe *QueryEngine) Aggregate(
    q *Query,
    fn AggregateFunc,
    step int64, // e.g., 5 minutes (300000 ms)
    groupBy []string,
) ([]TimeSeries, error)

func (qe *QueryEngine) Rate(
    q *Query,
    range int64, // e.g., 5 minutes
) ([]TimeSeries, error)
```

**Example Queries**:
```
sum(http_requests_total)                      # Total across all series
avg(cpu_usage{host="server1"})               # Average for matching series
rate(http_requests_total[5m])                # Per-second rate over 5m windows
sum(rate(http_requests_total[5m])) by (code) # Group by status code
```

**Deliverables**:
- Aggregation functions implemented
- Downsampling for long-term storage
- Tests for correctness
- Performance benchmarks

---

### Milestone 5.3: PromQL-Like Query Language (Optional, Advanced)
**Goal**: Parse and execute string queries

**Tasks**:
- [ ] Define query grammar (EBNF)
- [ ] Implement lexer and parser
- [ ] Create AST (Abstract Syntax Tree)
- [ ] Build query executor
- [ ] Add function registry
- [ ] Support nested queries

**Simple Grammar**:
```ebnf
query          ::= selector | function_call
selector       ::= metric_name label_set? time_range?
label_set      ::= '{' label_matcher (',' label_matcher)* '}'
label_matcher  ::= label_name ('='|'!='|'=~'|'!~') label_value
time_range     ::= '[' duration ']'
function_call  ::= function_name '(' query (',' args)* ')'
```

**Example Queries**:
```
cpu_usage{host="server1"}[5m]
sum(rate(http_requests_total{code="200"}[5m]))
avg(cpu_usage) by (host)
```

**Use parser library**:
```bash
go get github.com/alecthomas/participle/v2
```

**Deliverables** (optional):
- `pkg/query/parser.go`
- AST evaluation
- Query validation
- Documentation on query syntax

---

## Phase 6: Background Operations (Week 8-9)

### Milestone 6.1: Compaction
**Goal**: Merge small blocks, reduce query overhead

**Tasks**:
- [ ] Design compaction strategy (tiered or leveled)
- [ ] Implement block merging algorithm
- [ ] Add vertical compaction (remove duplicates/deletes)
- [ ] Add horizontal compaction (merge time ranges)
- [ ] Implement background compactor goroutine
- [ ] Add compaction metrics and monitoring
- [ ] Handle concurrent reads during compaction

**Compaction Tiers**:
```
Level 0: 2-hour blocks (raw ingestion)
Level 1: 12-hour blocks (merge 6x L0 blocks)
Level 2: 7-day blocks (merge 14x L1 blocks)
```

**Code to implement**:

```go
// pkg/storage/compactor.go
type Compactor struct {
    db          *TSDB
    interval    time.Duration
    retention   time.Duration
    concurrency int
}

func (c *Compactor) Run(ctx context.Context) {
    ticker := time.NewTicker(c.interval)
    for {
        select {
        case <-ticker.C:
            c.compact()
        case <-ctx.Done():
            return
        }
    }
}

func (c *Compactor) compact() error {
    // 1. Find blocks eligible for compaction
    // 2. Merge blocks (rewrite chunks, rebuild index)
    // 3. Atomic replacement (write new, delete old)
}
```

**Deliverables**:
- `pkg/storage/compactor.go`
- Compaction strategy documentation
- Tests for block merging
- Performance metrics

**Performance target**: Compact 10GB of data in <5 minutes

---

### Milestone 6.2: Retention & Garbage Collection
**Goal**: Automatically delete old data

**Tasks**:
- [ ] Implement retention policy (e.g., keep 30 days)
- [ ] Add block deletion based on time
- [ ] Implement tombstones for series deletion
- [ ] Add garbage collection for unused series
- [ ] Create cleanup background task
- [ ] Add retention configuration

**Code to implement**:

```go
// pkg/storage/retention.go
type RetentionPolicy struct {
    MaxAge     time.Duration  // e.g., 30 days
    MinSamples int64          // Keep at least N samples per series
}

func (db *TSDB) ApplyRetentionPolicy(policy RetentionPolicy) error {
    cutoffTime := time.Now().Add(-policy.MaxAge).UnixMilli()

    // Find blocks older than cutoffTime
    oldBlocks := db.FindBlocksBefore(cutoffTime)

    // Delete blocks
    for _, block := range oldBlocks {
        db.DeleteBlock(block)
    }
}
```

**Deliverables**:
- Retention policy implementation
- Configurable retention settings
- Disk space reclamation tests
- Documentation on data lifecycle

---

### Milestone 6.3: Downsampling (Optional)
**Goal**: Reduce storage for old data

**Tasks**:
- [ ] Implement multi-resolution storage
- [ ] Create downsampling rules (e.g., 1m → 5m → 1h)
- [ ] Add background downsampler
- [ ] Store downsampled data in separate blocks
- [ ] Query optimizer: use downsampled data for long ranges

**Example**:
```
Raw data (1 minute):      Keep for 7 days
5-minute averages:        Keep for 30 days
1-hour averages:          Keep for 1 year
```

**Deliverables** (optional):
- Downsampling implementation
- Multi-resolution query routing
- Storage savings metrics

---

## Phase 7: HTTP API & Client (Week 9-10)

### Milestone 7.1: REST API Server
**Goal**: Expose TSDB functionality via HTTP

**Tasks**:
- [ ] Design API endpoints (Prometheus-compatible)
- [ ] Implement write endpoint (remote write protocol)
- [ ] Implement query endpoints
- [ ] Add metadata endpoints (labels, series)
- [ ] Implement health check endpoint
- [ ] Add API authentication (optional)
- [ ] Create OpenAPI/Swagger spec

**API Endpoints**:

```go
// pkg/api/server.go
func (s *Server) RegisterRoutes() {
    // Write
    s.router.POST("/api/v1/write", s.handleWrite)

    // Query
    s.router.GET("/api/v1/query", s.handleQuery)
    s.router.GET("/api/v1/query_range", s.handleQueryRange)

    // Metadata
    s.router.GET("/api/v1/labels", s.handleLabels)
    s.router.GET("/api/v1/label/:name/values", s.handleLabelValues)
    s.router.GET("/api/v1/series", s.handleSeries)

    // Admin
    s.router.GET("/api/v1/status/tsdb", s.handleStatus)
    s.router.POST("/api/v1/admin/snapshot", s.handleSnapshot)

    // Health
    s.router.GET("/-/healthy", s.handleHealthy)
    s.router.GET("/-/ready", s.handleReady)
}
```

**Write Endpoint Format** (Prometheus Remote Write):
```json
POST /api/v1/write
Content-Type: application/json

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

**Query Endpoint**:
```
GET /api/v1/query_range?query=cpu_usage{host="server1"}&start=1640000000&end=1640003600&step=60

Response:
{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {"__name__": "cpu_usage", "host": "server1"},
        "values": [
          [1640000000, "0.75"],
          [1640000060, "0.82"],
          ...
        ]
      }
    ]
  }
}
```

**Deliverables**:
- `pkg/api/` with HTTP server
- Prometheus remote write/read support
- API documentation
- Integration tests with curl/httpie

**Use**: `net/http` or `github.com/gorilla/mux`

---

### Milestone 7.2: Go Client Library
**Goal**: Provide programmatic access to TSDB

**Tasks**:
- [ ] Create client package
- [ ] Implement write client
- [ ] Implement query client
- [ ] Add connection pooling
- [ ] Support batch writes
- [ ] Add retry logic with backoff
- [ ] Create usage examples

**Code to implement**:

```go
// pkg/client/client.go
package client

type Client struct {
    baseURL string
    client  *http.Client
}

func NewClient(addr string, opts ...Option) *Client

func (c *Client) Write(ctx context.Context, metrics []Metric) error

func (c *Client) Query(ctx context.Context, query string, ts time.Time) (*QueryResult, error)

func (c *Client) QueryRange(ctx context.Context, query string, start, end time.Time, step time.Duration) (*QueryResult, error)

func (c *Client) Labels(ctx context.Context) ([]string, error)
```

**Example Usage**:
```go
package main

import "github.com/yourusername/tsdb/pkg/client"

func main() {
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

    // Query
    result, err := c.QueryRange(
        context.Background(),
        "cpu_usage{host=\"server1\"}",
        time.Now().Add(-1*time.Hour),
        time.Now(),
        1*time.Minute,
    )
}
```

**Deliverables**:
- `pkg/client/` with client library
- Example programs in `examples/`
- Client documentation
- Integration tests

---

### Milestone 7.3: CLI Tool
**Goal**: Command-line interface for TSDB management

**Tasks**:
- [ ] Create CLI with cobra/urfave-cli
- [ ] Add `tsdb start` command (start server)
- [ ] Add `tsdb write` command (write samples)
- [ ] Add `tsdb query` command (run queries)
- [ ] Add `tsdb inspect` command (debug blocks)
- [ ] Add `tsdb compact` command (manual compaction)
- [ ] Add `tsdb import` command (bulk import)

**CLI Structure**:
```bash
# Start server
tsdb start --listen=:8080 --data-dir=./data --retention=30d

# Write data
tsdb write --addr=localhost:8080 'cpu_usage{host="server1"} 0.85'

# Query
tsdb query --addr=localhost:8080 'cpu_usage{host="server1"}' --start=-1h

# Inspect blocks
tsdb inspect ./data/01H8XABC00000000/

# Manual compaction
tsdb compact --data-dir=./data

# Import CSV
tsdb import --addr=localhost:8080 --file=metrics.csv
```

**Deliverables**:
- `cmd/tsdb/` with CLI implementation
- User-friendly output formatting
- CLI documentation

**Use**: `github.com/spf13/cobra` or `github.com/urfave/cli`

---

## Phase 8: Performance & Production Readiness (Week 10-12)

### Milestone 8.1: Benchmarking & Optimization
**Goal**: Meet performance targets and identify bottlenecks

**Tasks**:
- [ ] Create comprehensive benchmark suite
- [ ] Measure write throughput (target: 100K-500K/sec)
- [ ] Measure query latency (target: <100ms)
- [ ] Profile CPU usage with pprof
- [ ] Profile memory allocation
- [ ] Optimize hot paths
- [ ] Add continuous benchmarking

**Benchmark Suite**:

```go
// benchmarks/write_test.go
func BenchmarkInsert(b *testing.B) {
    db := setupDB()
    samples := generateSamples(b.N)

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        db.Insert(samples[i].Series, samples[i].Samples)
    }
    b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "samples/sec")
}

// benchmarks/query_test.go
func BenchmarkQueryRange(b *testing.B) {
    db := setupDBWithData()

    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        db.Query(&Query{...})
    }
}
```

**Profiling Commands**:
```bash
# CPU profile
go test -bench=. -benchtime=10s -cpuprofile=cpu.prof
go tool pprof -http=:8080 cpu.prof

# Memory profile
go test -bench=. -benchtime=10s -memprofile=mem.prof
go tool pprof -http=:8080 mem.prof

# Trace
go test -bench=. -trace=trace.out
go tool trace trace.out
```

**Performance Checklist**:
- [ ] Write throughput: 100K+ samples/sec (single node)
- [ ] Query latency: <100ms for 1-week range, 1000 series
- [ ] Compression ratio: 10-20x (12 bytes → 1-1.5 bytes)
- [ ] Memory usage: <512MB for 1M active series
- [ ] CPU usage: <50% at 100K writes/sec
- [ ] Disk I/O: Efficient batching, sequential writes

**Deliverables**:
- Comprehensive benchmark suite
- Performance report with graphs
- Optimization documentation
- Continuous benchmarking in CI

---

### Milestone 8.2: Observability & Monitoring
**Goal**: Make TSDB observable and debuggable

**Tasks**:
- [ ] Instrument code with Prometheus metrics (meta!)
- [ ] Add structured logging (log/slog)
- [ ] Create debugging endpoints (pprof)
- [ ] Add distributed tracing (OpenTelemetry, optional)
- [ ] Build monitoring dashboard (Grafana)
- [ ] Add alerting rules

**Metrics to Export**:
```go
// Internal metrics
tsdb_samples_ingested_total
tsdb_samples_ingested_bytes_total
tsdb_compactions_total
tsdb_compactions_duration_seconds
tsdb_wal_size_bytes
tsdb_head_series
tsdb_head_chunks
tsdb_blocks_total
tsdb_queries_total
tsdb_query_duration_seconds
```

**Logging Structure**:
```go
import "log/slog"

slog.Info("starting TSDB",
    "listen_addr", cfg.ListenAddr,
    "data_dir", cfg.DataDir,
    "retention", cfg.Retention,
)

slog.Debug("flushing memtable",
    "series_count", memTable.SeriesCount(),
    "sample_count", memTable.SampleCount(),
    "duration_ms", duration.Milliseconds(),
)
```

**Debug Endpoints**:
```go
import _ "net/http/pprof"

// Automatically registers pprof handlers
http.ListenAndServe("localhost:6060", nil)

// Access via:
// http://localhost:6060/debug/pprof/
```

**Deliverables**:
- Prometheus metrics exported at `/metrics`
- Structured logging throughout codebase
- pprof endpoints for debugging
- Example Grafana dashboard JSON
- Documentation on observability

---

### Milestone 8.3: Testing & Quality
**Goal**: Ensure correctness and reliability

**Tasks**:
- [ ] Achieve 70%+ code coverage
- [ ] Add integration tests (end-to-end)
- [ ] Add stress tests (long-running)
- [ ] Add chaos tests (crash recovery, data corruption)
- [ ] Implement fuzz testing for parsers
- [ ] Add race detector tests
- [ ] Create test data generator

**Test Categories**:

1. **Unit Tests**: Test individual components
2. **Integration Tests**: Test component interactions
3. **Stress Tests**: High load, long duration
4. **Chaos Tests**: Simulated failures
5. **Regression Tests**: Prevent known bugs

**Example Chaos Test**:
```go
func TestCrashRecovery(t *testing.T) {
    db := startDB()

    // Write data
    writeData(db, 10000)

    // Simulate crash (kill process)
    db.Kill()

    // Restart and verify data
    db = restartDB()
    verifyData(db, 10000)
}
```

**Test Commands**:
```bash
# Unit tests with coverage
go test ./... -cover -coverprofile=coverage.out
go tool cover -html=coverage.out

# Race detector
go test ./... -race

# Integration tests
go test ./... -tags=integration -v

# Stress test
go test -run=TestStress -timeout=30m -v
```

**Deliverables**:
- 70%+ test coverage
- CI running all test suites
- Test documentation
- Coverage reports in CI

---

### Milestone 8.4: Documentation & Examples
**Goal**: Make project accessible and usable

**Tasks**:
- [ ] Write comprehensive README
- [ ] Create DESIGN.md (architecture)
- [ ] Document API endpoints (API.md)
- [ ] Explain compression algorithms (COMPRESSION.md)
- [ ] Add code examples
- [ ] Create tutorial blog post
- [ ] Record demo video (optional)
- [ ] Add architecture diagrams

**Documentation Structure**:

```
docs/
├── README.md              # Overview, quick start
├── DESIGN.md              # Architecture, design decisions
├── API.md                 # HTTP API reference
├── COMPRESSION.md         # Gorilla compression explained
├── QUERY_LANGUAGE.md      # Query syntax guide
├── PERFORMANCE.md         # Benchmarks, tuning guide
├── OPERATIONS.md          # Deployment, monitoring
└── diagrams/
    ├── architecture.png
    ├── write_path.png
    └── read_path.png
```

**README Sections**:
1. Project overview and goals
2. Features and performance characteristics
3. Quick start guide
4. Architecture overview
5. API documentation
6. Configuration reference
7. Development setup
8. Contributing guidelines
9. License

**Deliverables**:
- Complete documentation suite
- Code examples in `examples/`
- Architecture diagrams
- Blog post or tutorial

---

### Milestone 8.5: Deployment & Docker
**Goal**: Make TSDB easy to deploy

**Tasks**:
- [ ] Create Dockerfile
- [ ] Add docker-compose setup
- [ ] Create Kubernetes manifests (optional)
- [ ] Add systemd service file
- [ ] Create deployment guide
- [ ] Add configuration examples

**Dockerfile**:
```dockerfile
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY . .
RUN go build -o tsdb ./cmd/tsdb

FROM alpine:latest
RUN apk --no-cache add ca-certificates
COPY --from=builder /app/tsdb /usr/local/bin/
EXPOSE 8080
VOLUME ["/data"]
ENTRYPOINT ["tsdb"]
CMD ["start", "--data-dir=/data", "--listen=:8080"]
```

**docker-compose.yml**:
```yaml
version: '3.8'
services:
  tsdb:
    build: .
    ports:
      - "8080:8080"
    volumes:
      - tsdb-data:/data
    environment:
      - TSDB_RETENTION=30d
      - TSDB_LOG_LEVEL=info

volumes:
  tsdb-data:
```

**Deliverables**:
- Docker image building successfully
- docker-compose for local development
- Deployment documentation
- Configuration examples

---

## Phase 9: Advanced Features (Optional Extensions)

### 9.1: High Availability & Replication
- [ ] Implement Raft consensus for clustering
- [ ] Add replication protocol
- [ ] Leader election and failover
- [ ] Consistent hashing for sharding

### 9.2: Performance Optimizations
- [ ] Memory-mapped file I/O for blocks
- [ ] SIMD optimizations for compression
- [ ] Lock-free data structures
- [ ] Zero-copy data paths

### 9.3: Advanced Query Features
- [ ] Subqueries and nested aggregations
- [ ] Recording rules (pre-aggregation)
- [ ] Alerting rules engine
- [ ] Query cost estimation

### 9.4: Ecosystem Integration
- [ ] Prometheus remote storage adapter
- [ ] Grafana data source plugin
- [ ] Telegraf output plugin
- [ ] OpenTelemetry collector exporter

---

## Key Technical Challenges

| Challenge | Solution Approach |
|-----------|------------------|
| **High write throughput** | Batching, async WAL, lock-free queues, double-buffering |
| **Query performance** | Inverted index, chunk skipping, bloom filters, parallel queries |
| **Memory efficiency** | mmap for blocks, limited MemTable size, LRU caches |
| **Compression ratio** | Delta-of-delta + XOR (Gorilla), bit-level encoding |
| **Crash recovery** | WAL with checksums, idempotent replay, atomic block replacement |
| **Cardinality explosion** | Label limits, series churn detection, GC for inactive series |
| **Compaction overhead** | Background compaction, incremental merging, read/write isolation |
| **Time-range queries** | Time-partitioned blocks, chunk-level time filtering |

---

## Learning Resources

### Papers to Read
1. **Gorilla: A Fast, Scalable, In-Memory Time Series Database** (Facebook)
   - https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
   - Delta-of-delta and XOR compression algorithms

2. **Monarch: Google's Planet-Scale In-Memory Time Series Database**
   - Architecture and query optimization

3. **The Log-Structured Merge-Tree (LSM-Tree)**
   - Foundation for compaction strategies

4. **Roaring Bitmaps: Implementation of an Optimized Software Library**
   - Efficient bitmap operations for indexing

### Open-Source References
1. **Prometheus TSDB**: `github.com/prometheus/prometheus/tsdb`
   - Go implementation, excellent reference

2. **VictoriaMetrics**: `github.com/VictoriaMetrics/VictoriaMetrics`
   - High-performance Go TSDB

3. **InfluxDB**: `github.com/influxdata/influxdb`
   - Column-oriented storage engine

4. **M3DB**: `github.com/m3db/m3`
   - Uber's distributed TSDB

### Go Resources
1. **"The Go Programming Language"** by Donovan & Kernighan
2. **Go Concurrency Patterns**: golang.org/blog/pipelines
3. **Effective Go**: golang.org/doc/effective_go
4. **`testing` package**: Benchmarking and profiling
5. **`pprof` tool**: Performance analysis

### Community
- Prometheus mailing list
- CNCF Slack #prometheus-dev
- r/golang on Reddit
- Go Forum: forum.golangbridge.org

---

## Success Metrics

### Technical Metrics
- ✅ Write throughput: 100K-500K samples/second
- ✅ Query latency: <100ms for 1-week range
- ✅ Compression ratio: 10-20x
- ✅ Memory efficiency: <512MB for 1M series
- ✅ Test coverage: >70%
- ✅ Zero data loss with WAL enabled

### Portfolio Metrics
- ✅ GitHub stars: Target 50+
- ✅ Comprehensive documentation
- ✅ Working demo/video
- ✅ Blog post with 1000+ views
- ✅ LinkedIn post highlighting project

### Career Impact
**Resume bullet point**:
> "Built production-grade time-series database in Go handling 100K+ writes/sec with 15x compression using delta-of-delta and XOR encoding (Facebook's Gorilla paper). Implemented write-ahead logging, inverted indexing, and time-partitioned storage similar to Prometheus."

**Interview talking points**:
- Storage engine design trade-offs (LSM vs. time-partitioned)
- Compression algorithm implementation details
- Handling high-cardinality label sets
- Concurrency patterns in Go (goroutines, channels, mutexes)
- Performance profiling and optimization techniques
- Crash recovery and durability guarantees

---

## Timeline Summary

| Phase | Duration | Key Deliverables |
|-------|----------|------------------|
| 1. Foundation | Week 1-2 | Data structures, project setup |
| 2. Write Path | Week 2-3 | WAL, MemTable, ingestion |
| 3. Storage Engine | Week 3-5 | Blocks, compression, chunks |
| 4. Indexing | Week 5-6 | Inverted index, series registry |
| 5. Query Engine | Week 6-8 | Time-range queries, aggregations |
| 6. Background Ops | Week 8-9 | Compaction, retention, GC |
| 7. API & Client | Week 9-10 | HTTP API, client lib, CLI |
| 8. Production | Week 10-12 | Benchmarks, docs, deployment |

**Total**: 8-12 weeks (part-time) or 4-6 weeks (full-time)

---

## Getting Started

### Initial Setup
```bash
# Clone or create repository
mkdir tsdb && cd tsdb
git init

# Initialize Go module
go mod init github.com/yourusername/tsdb

# Create directory structure
mkdir -p cmd/tsdb pkg/{storage,wal,series,index,compression,query,api} \
         internal/{bitmap,util} testdata benchmarks docs examples

# Install initial dependencies
go get github.com/dgraph-io/badger/v3
go get github.com/RoaringBitmap/roaring
go get github.com/stretchr/testify

# Create this roadmap
cp ROADMAP.md docs/

# Initial commit
git add .
git commit -m "Initial commit: project structure and roadmap"
```

### Development Workflow
1. Start with Phase 1, Milestone 1.1
2. Create feature branch: `git checkout -b feat/data-structures`
3. Implement milestone tasks
4. Write tests (aim for >70% coverage)
5. Run benchmarks
6. Update documentation
7. Commit and push
8. Move to next milestone

### Questions to Ask Yourself
- ✅ Can my TSDB handle 100K writes/second?
- ✅ Is my compression ratio competitive (10x+)?
- ✅ Are my queries fast (<100ms)?
- ✅ Can I explain the Gorilla paper algorithms?
- ✅ Does my design handle crash recovery?
- ✅ Is my code well-tested and documented?

---

## Next Steps

Ready to begin? Start with **Phase 1, Milestone 1.1**:

1. Initialize Go module
2. Create directory structure
3. Setup CI/CD pipeline
4. Write initial README

**Let's build something amazing!** 🚀

---

## Appendix: References

- [Prometheus Documentation](https://prometheus.io/docs/)
- [Gorilla Paper (PDF)](https://www.vldb.org/pvldb/vol8/p1816-teller.pdf)
- [VictoriaMetrics Blog](https://victoriametrics.com/blog/)
- [InfluxDB Design Docs](https://github.com/influxdata/influxdb/tree/master/tsdb)
- [Go Concurrency Patterns](https://go.dev/blog/pipelines)
- [Effective Go](https://go.dev/doc/effective_go)

---

**Good luck building your time-series database!** 🎉

This project will demonstrate deep systems programming knowledge and position you as a storage engine expert—skills that command premium salaries at infrastructure companies.
