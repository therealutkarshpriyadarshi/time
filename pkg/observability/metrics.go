package observability

import (
	"sync"
	"sync/atomic"
	"time"
)

// Metrics collects and exposes TSDB operational metrics in Prometheus format
type Metrics struct {
	// Write path metrics
	samplesIngestedTotal     atomic.Int64
	samplesIngestedBytesTotal atomic.Int64
	insertErrorsTotal        atomic.Int64
	insertDurationSeconds    *Histogram

	// WAL metrics
	walSizeBytes          atomic.Int64
	walSegmentsTotal      atomic.Int64
	walSyncDurationSeconds *Histogram
	walCorruptionsTotal   atomic.Int64

	// MemTable metrics
	headSeries atomic.Int64
	headChunks atomic.Int64
	headSizeBytes atomic.Int64

	// Block/storage metrics
	blocksTotal      atomic.Int64
	blockSizeBytes   atomic.Int64
	oldestBlockTime  atomic.Int64
	newestBlockTime  atomic.Int64

	// Compaction metrics
	compactionsTotal         atomic.Int64
	compactionDurationSeconds *Histogram
	compactedBytesTotal      atomic.Int64
	compactionFailuresTotal  atomic.Int64

	// Query metrics
	queriesTotal            atomic.Int64
	queryDurationSeconds    *Histogram
	queryErrorsTotal        atomic.Int64
	queriedSamplesTotal     atomic.Int64

	// System metrics
	goroutinesCount atomic.Int64
	memoryAllocBytes atomic.Int64
	gcDurationSeconds *Histogram
}

var (
	globalMetrics     *Metrics
	globalMetricsOnce sync.Once
)

// GetGlobalMetrics returns the singleton metrics instance
func GetGlobalMetrics() *Metrics {
	globalMetricsOnce.Do(func() {
		globalMetrics = NewMetrics()
	})
	return globalMetrics
}

// NewMetrics creates a new Metrics instance
func NewMetrics() *Metrics {
	return &Metrics{
		insertDurationSeconds:     NewHistogram("insert_duration_seconds"),
		walSyncDurationSeconds:    NewHistogram("wal_sync_duration_seconds"),
		compactionDurationSeconds: NewHistogram("compaction_duration_seconds"),
		queryDurationSeconds:      NewHistogram("query_duration_seconds"),
		gcDurationSeconds:         NewHistogram("gc_duration_seconds"),
	}
}

// RecordSamplesIngested records samples written
func (m *Metrics) RecordSamplesIngested(count int64, bytes int64) {
	m.samplesIngestedTotal.Add(count)
	m.samplesIngestedBytesTotal.Add(bytes)
}

// RecordInsertError records an insert error
func (m *Metrics) RecordInsertError() {
	m.insertErrorsTotal.Add(1)
}

// RecordInsertDuration records insert latency
func (m *Metrics) RecordInsertDuration(d time.Duration) {
	m.insertDurationSeconds.Observe(d.Seconds())
}

// SetWALSize sets current WAL size
func (m *Metrics) SetWALSize(bytes int64) {
	m.walSizeBytes.Store(bytes)
}

// SetWALSegments sets number of WAL segments
func (m *Metrics) SetWALSegments(count int64) {
	m.walSegmentsTotal.Store(count)
}

// RecordWALSync records WAL sync duration
func (m *Metrics) RecordWALSync(d time.Duration) {
	m.walSyncDurationSeconds.Observe(d.Seconds())
}

// RecordWALCorruption records WAL corruption
func (m *Metrics) RecordWALCorruption() {
	m.walCorruptionsTotal.Add(1)
}

// SetHeadSeries sets number of series in head (MemTable)
func (m *Metrics) SetHeadSeries(count int64) {
	m.headSeries.Store(count)
}

// SetHeadChunks sets number of chunks in head
func (m *Metrics) SetHeadChunks(count int64) {
	m.headChunks.Store(count)
}

// SetHeadSize sets head (MemTable) size in bytes
func (m *Metrics) SetHeadSize(bytes int64) {
	m.headSizeBytes.Store(bytes)
}

// SetBlocksTotal sets total number of blocks
func (m *Metrics) SetBlocksTotal(count int64) {
	m.blocksTotal.Store(count)
}

// SetBlockSize sets total size of all blocks
func (m *Metrics) SetBlockSize(bytes int64) {
	m.blockSizeBytes.Store(bytes)
}

// SetOldestBlockTime sets timestamp of oldest block
func (m *Metrics) SetOldestBlockTime(timestamp int64) {
	m.oldestBlockTime.Store(timestamp)
}

// SetNewestBlockTime sets timestamp of newest block
func (m *Metrics) SetNewestBlockTime(timestamp int64) {
	m.newestBlockTime.Store(timestamp)
}

// RecordCompaction records a compaction event
func (m *Metrics) RecordCompaction(duration time.Duration, bytes int64) {
	m.compactionsTotal.Add(1)
	m.compactionDurationSeconds.Observe(duration.Seconds())
	m.compactedBytesTotal.Add(bytes)
}

// RecordCompactionFailure records a compaction failure
func (m *Metrics) RecordCompactionFailure() {
	m.compactionFailuresTotal.Add(1)
}

// RecordQuery records a query
func (m *Metrics) RecordQuery(duration time.Duration, samples int64) {
	m.queriesTotal.Add(1)
	m.queryDurationSeconds.Observe(duration.Seconds())
	m.queriedSamplesTotal.Add(samples)
}

// RecordQueryError records a query error
func (m *Metrics) RecordQueryError() {
	m.queryErrorsTotal.Add(1)
}

// SetGoroutinesCount sets current goroutine count
func (m *Metrics) SetGoroutinesCount(count int64) {
	m.goroutinesCount.Store(count)
}

// SetMemoryAlloc sets current memory allocation
func (m *Metrics) SetMemoryAlloc(bytes int64) {
	m.memoryAllocBytes.Store(bytes)
}

// RecordGC records garbage collection duration
func (m *Metrics) RecordGC(d time.Duration) {
	m.gcDurationSeconds.Observe(d.Seconds())
}

// Snapshot returns a snapshot of all metrics
type MetricsSnapshot struct {
	SamplesIngestedTotal      int64
	SamplesIngestedBytesTotal int64
	InsertErrorsTotal         int64

	WALSizeBytes        int64
	WALSegmentsTotal    int64
	WALCorruptionsTotal int64

	HeadSeries    int64
	HeadChunks    int64
	HeadSizeBytes int64

	BlocksTotal     int64
	BlockSizeBytes  int64
	OldestBlockTime int64
	NewestBlockTime int64

	CompactionsTotal        int64
	CompactedBytesTotal     int64
	CompactionFailuresTotal int64

	QueriesTotal        int64
	QueryErrorsTotal    int64
	QueriedSamplesTotal int64

	GoroutinesCount  int64
	MemoryAllocBytes int64
}

// Snapshot returns a point-in-time snapshot of all metrics
func (m *Metrics) Snapshot() *MetricsSnapshot {
	return &MetricsSnapshot{
		SamplesIngestedTotal:      m.samplesIngestedTotal.Load(),
		SamplesIngestedBytesTotal: m.samplesIngestedBytesTotal.Load(),
		InsertErrorsTotal:         m.insertErrorsTotal.Load(),

		WALSizeBytes:        m.walSizeBytes.Load(),
		WALSegmentsTotal:    m.walSegmentsTotal.Load(),
		WALCorruptionsTotal: m.walCorruptionsTotal.Load(),

		HeadSeries:    m.headSeries.Load(),
		HeadChunks:    m.headChunks.Load(),
		HeadSizeBytes: m.headSizeBytes.Load(),

		BlocksTotal:     m.blocksTotal.Load(),
		BlockSizeBytes:  m.blockSizeBytes.Load(),
		OldestBlockTime: m.oldestBlockTime.Load(),
		NewestBlockTime: m.newestBlockTime.Load(),

		CompactionsTotal:        m.compactionsTotal.Load(),
		CompactedBytesTotal:     m.compactedBytesTotal.Load(),
		CompactionFailuresTotal: m.compactionFailuresTotal.Load(),

		QueriesTotal:        m.queriesTotal.Load(),
		QueryErrorsTotal:    m.queryErrorsTotal.Load(),
		QueriedSamplesTotal: m.queriedSamplesTotal.Load(),

		GoroutinesCount:  m.goroutinesCount.Load(),
		MemoryAllocBytes: m.memoryAllocBytes.Load(),
	}
}
