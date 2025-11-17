package observability

import (
	"fmt"
	"io"
	"runtime"
	"sort"
	"strings"
)

// WritePrometheusMetrics writes all metrics in Prometheus exposition format
func WritePrometheusMetrics(w io.Writer, m *Metrics) error {
	snapshot := m.Snapshot()

	var sb strings.Builder

	// Write path metrics
	writeCounter(&sb, "tsdb_samples_ingested_total", "Total number of samples ingested", snapshot.SamplesIngestedTotal)
	writeCounter(&sb, "tsdb_samples_ingested_bytes_total", "Total bytes of samples ingested", snapshot.SamplesIngestedBytesTotal)
	writeCounter(&sb, "tsdb_insert_errors_total", "Total number of insert errors", snapshot.InsertErrorsTotal)
	writeHistogramStats(&sb, "tsdb_insert_duration_seconds", "Insert operation duration", m.insertDurationSeconds)

	// WAL metrics
	writeGauge(&sb, "tsdb_wal_size_bytes", "Current WAL size in bytes", snapshot.WALSizeBytes)
	writeGauge(&sb, "tsdb_wal_segments_total", "Number of WAL segments", snapshot.WALSegmentsTotal)
	writeCounter(&sb, "tsdb_wal_corruptions_total", "Total WAL corruptions detected", snapshot.WALCorruptionsTotal)
	writeHistogramStats(&sb, "tsdb_wal_sync_duration_seconds", "WAL sync duration", m.walSyncDurationSeconds)

	// MemTable/Head metrics
	writeGauge(&sb, "tsdb_head_series", "Number of series in head (MemTable)", snapshot.HeadSeries)
	writeGauge(&sb, "tsdb_head_chunks", "Number of chunks in head", snapshot.HeadChunks)
	writeGauge(&sb, "tsdb_head_size_bytes", "Head (MemTable) size in bytes", snapshot.HeadSizeBytes)

	// Block/storage metrics
	writeGauge(&sb, "tsdb_blocks_total", "Total number of persisted blocks", snapshot.BlocksTotal)
	writeGauge(&sb, "tsdb_block_size_bytes", "Total size of all blocks in bytes", snapshot.BlockSizeBytes)
	writeGauge(&sb, "tsdb_oldest_block_timestamp_ms", "Timestamp of oldest block", snapshot.OldestBlockTime)
	writeGauge(&sb, "tsdb_newest_block_timestamp_ms", "Timestamp of newest block", snapshot.NewestBlockTime)

	// Compaction metrics
	writeCounter(&sb, "tsdb_compactions_total", "Total number of compactions performed", snapshot.CompactionsTotal)
	writeCounter(&sb, "tsdb_compacted_bytes_total", "Total bytes compacted", snapshot.CompactedBytesTotal)
	writeCounter(&sb, "tsdb_compaction_failures_total", "Total compaction failures", snapshot.CompactionFailuresTotal)
	writeHistogramStats(&sb, "tsdb_compaction_duration_seconds", "Compaction duration", m.compactionDurationSeconds)

	// Query metrics
	writeCounter(&sb, "tsdb_queries_total", "Total number of queries executed", snapshot.QueriesTotal)
	writeCounter(&sb, "tsdb_query_errors_total", "Total query errors", snapshot.QueryErrorsTotal)
	writeCounter(&sb, "tsdb_queried_samples_total", "Total samples returned by queries", snapshot.QueriedSamplesTotal)
	writeHistogramStats(&sb, "tsdb_query_duration_seconds", "Query duration", m.queryDurationSeconds)

	// System/runtime metrics
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	writeGauge(&sb, "tsdb_goroutines", "Number of goroutines", int64(runtime.NumGoroutine()))
	writeGauge(&sb, "tsdb_memory_alloc_bytes", "Bytes allocated and still in use", int64(memStats.Alloc))
	writeGauge(&sb, "tsdb_memory_sys_bytes", "Bytes obtained from system", int64(memStats.Sys))
	writeCounter(&sb, "tsdb_gc_runs_total", "Total number of GC runs", int64(memStats.NumGC))
	writeHistogramStats(&sb, "tsdb_gc_duration_seconds", "GC duration", m.gcDurationSeconds)

	_, err := w.Write([]byte(sb.String()))
	return err
}

func writeCounter(sb *strings.Builder, name, help string, value int64) {
	sb.WriteString(fmt.Sprintf("# HELP %s %s\n", name, help))
	sb.WriteString(fmt.Sprintf("# TYPE %s counter\n", name))
	sb.WriteString(fmt.Sprintf("%s %d\n", name, value))
	sb.WriteString("\n")
}

func writeGauge(sb *strings.Builder, name, help string, value int64) {
	sb.WriteString(fmt.Sprintf("# HELP %s %s\n", name, help))
	sb.WriteString(fmt.Sprintf("# TYPE %s gauge\n", name))
	sb.WriteString(fmt.Sprintf("%s %d\n", name, value))
	sb.WriteString("\n")
}

func writeHistogramStats(sb *strings.Builder, name, help string, hist *Histogram) {
	stats := hist.GetStats()

	sb.WriteString(fmt.Sprintf("# HELP %s %s\n", name, help))
	sb.WriteString(fmt.Sprintf("# TYPE %s summary\n", name))

	if stats.Count > 0 {
		sb.WriteString(fmt.Sprintf("%s{quantile=\"0.5\"} %f\n", name, stats.P50))
		sb.WriteString(fmt.Sprintf("%s{quantile=\"0.9\"} %f\n", name, stats.P90))
		sb.WriteString(fmt.Sprintf("%s{quantile=\"0.95\"} %f\n", name, stats.P95))
		sb.WriteString(fmt.Sprintf("%s{quantile=\"0.99\"} %f\n", name, stats.P99))
		sb.WriteString(fmt.Sprintf("%s_sum %f\n", name, stats.Sum))
		sb.WriteString(fmt.Sprintf("%s_count %d\n", name, stats.Count))
	} else {
		sb.WriteString(fmt.Sprintf("%s_sum 0\n", name))
		sb.WriteString(fmt.Sprintf("%s_count 0\n", name))
	}
	sb.WriteString("\n")
}

// GetMetricsSummary returns a human-readable summary of all metrics
func GetMetricsSummary(m *Metrics) string {
	snapshot := m.Snapshot()
	var sb strings.Builder

	sb.WriteString("=== TSDB Metrics Summary ===\n\n")

	// Write path
	sb.WriteString("Write Path:\n")
	sb.WriteString(fmt.Sprintf("  Samples Ingested: %d (%.2f MB)\n",
		snapshot.SamplesIngestedTotal,
		float64(snapshot.SamplesIngestedBytesTotal)/(1024*1024)))
	sb.WriteString(fmt.Sprintf("  Insert Errors: %d\n", snapshot.InsertErrorsTotal))

	if insertStats := m.insertDurationSeconds.GetStats(); insertStats.Count > 0 {
		sb.WriteString(fmt.Sprintf("  Insert Latency: p50=%.3fms p95=%.3fms p99=%.3fms\n",
			insertStats.P50*1000, insertStats.P95*1000, insertStats.P99*1000))
	}

	// WAL
	sb.WriteString("\nWAL:\n")
	sb.WriteString(fmt.Sprintf("  Size: %.2f MB\n", float64(snapshot.WALSizeBytes)/(1024*1024)))
	sb.WriteString(fmt.Sprintf("  Segments: %d\n", snapshot.WALSegmentsTotal))
	sb.WriteString(fmt.Sprintf("  Corruptions: %d\n", snapshot.WALCorruptionsTotal))

	// MemTable/Head
	sb.WriteString("\nHead (MemTable):\n")
	sb.WriteString(fmt.Sprintf("  Series: %d\n", snapshot.HeadSeries))
	sb.WriteString(fmt.Sprintf("  Chunks: %d\n", snapshot.HeadChunks))
	sb.WriteString(fmt.Sprintf("  Size: %.2f MB\n", float64(snapshot.HeadSizeBytes)/(1024*1024)))

	// Blocks
	sb.WriteString("\nBlocks:\n")
	sb.WriteString(fmt.Sprintf("  Count: %d\n", snapshot.BlocksTotal))
	sb.WriteString(fmt.Sprintf("  Total Size: %.2f MB\n", float64(snapshot.BlockSizeBytes)/(1024*1024)))

	// Compaction
	sb.WriteString("\nCompaction:\n")
	sb.WriteString(fmt.Sprintf("  Total Compactions: %d\n", snapshot.CompactionsTotal))
	sb.WriteString(fmt.Sprintf("  Bytes Compacted: %.2f MB\n", float64(snapshot.CompactedBytesTotal)/(1024*1024)))
	sb.WriteString(fmt.Sprintf("  Failures: %d\n", snapshot.CompactionFailuresTotal))

	// Queries
	sb.WriteString("\nQueries:\n")
	sb.WriteString(fmt.Sprintf("  Total Queries: %d\n", snapshot.QueriesTotal))
	sb.WriteString(fmt.Sprintf("  Errors: %d\n", snapshot.QueryErrorsTotal))
	sb.WriteString(fmt.Sprintf("  Samples Returned: %d\n", snapshot.QueriedSamplesTotal))

	if queryStats := m.queryDurationSeconds.GetStats(); queryStats.Count > 0 {
		sb.WriteString(fmt.Sprintf("  Query Latency: p50=%.3fms p95=%.3fms p99=%.3fms\n",
			queryStats.P50*1000, queryStats.P95*1000, queryStats.P99*1000))
	}

	// System
	sb.WriteString("\nSystem:\n")
	sb.WriteString(fmt.Sprintf("  Goroutines: %d\n", snapshot.GoroutinesCount))
	sb.WriteString(fmt.Sprintf("  Memory Allocated: %.2f MB\n", float64(snapshot.MemoryAllocBytes)/(1024*1024)))

	return sb.String()
}

// MetricsList returns a list of all available metrics
func MetricsList() []string {
	metrics := []string{
		"tsdb_samples_ingested_total",
		"tsdb_samples_ingested_bytes_total",
		"tsdb_insert_errors_total",
		"tsdb_insert_duration_seconds",
		"tsdb_wal_size_bytes",
		"tsdb_wal_segments_total",
		"tsdb_wal_corruptions_total",
		"tsdb_wal_sync_duration_seconds",
		"tsdb_head_series",
		"tsdb_head_chunks",
		"tsdb_head_size_bytes",
		"tsdb_blocks_total",
		"tsdb_block_size_bytes",
		"tsdb_oldest_block_timestamp_ms",
		"tsdb_newest_block_timestamp_ms",
		"tsdb_compactions_total",
		"tsdb_compacted_bytes_total",
		"tsdb_compaction_failures_total",
		"tsdb_compaction_duration_seconds",
		"tsdb_queries_total",
		"tsdb_query_errors_total",
		"tsdb_queried_samples_total",
		"tsdb_query_duration_seconds",
		"tsdb_goroutines",
		"tsdb_memory_alloc_bytes",
		"tsdb_memory_sys_bytes",
		"tsdb_gc_runs_total",
		"tsdb_gc_duration_seconds",
	}
	sort.Strings(metrics)
	return metrics
}
