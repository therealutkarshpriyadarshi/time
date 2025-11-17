package observability

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestMetrics_RecordOperations(t *testing.T) {
	m := NewMetrics()

	// Test write metrics
	m.RecordSamplesIngested(100, 1200)
	m.RecordInsertDuration(10 * time.Millisecond)
	m.RecordInsertError()

	// Test WAL metrics
	m.SetWALSize(1024 * 1024)
	m.SetWALSegments(5)
	m.RecordWALSync(5 * time.Millisecond)
	m.RecordWALCorruption()

	// Test head metrics
	m.SetHeadSeries(500)
	m.SetHeadChunks(1000)
	m.SetHeadSize(256 * 1024)

	// Test block metrics
	m.SetBlocksTotal(10)
	m.SetBlockSize(10 * 1024 * 1024)

	// Test compaction metrics
	m.RecordCompaction(30*time.Second, 5*1024*1024)
	m.RecordCompactionFailure()

	// Test query metrics
	m.RecordQuery(50*time.Millisecond, 1000)
	m.RecordQueryError()

	snapshot := m.Snapshot()

	// Verify all metrics were recorded
	if snapshot.SamplesIngestedTotal != 100 {
		t.Errorf("expected 100 samples ingested, got %d", snapshot.SamplesIngestedTotal)
	}

	if snapshot.SamplesIngestedBytesTotal != 1200 {
		t.Errorf("expected 1200 bytes ingested, got %d", snapshot.SamplesIngestedBytesTotal)
	}

	if snapshot.InsertErrorsTotal != 1 {
		t.Errorf("expected 1 insert error, got %d", snapshot.InsertErrorsTotal)
	}

	if snapshot.WALSizeBytes != 1024*1024 {
		t.Errorf("expected WAL size 1MB, got %d", snapshot.WALSizeBytes)
	}

	if snapshot.WALSegmentsTotal != 5 {
		t.Errorf("expected 5 WAL segments, got %d", snapshot.WALSegmentsTotal)
	}

	if snapshot.WALCorruptionsTotal != 1 {
		t.Errorf("expected 1 WAL corruption, got %d", snapshot.WALCorruptionsTotal)
	}

	if snapshot.HeadSeries != 500 {
		t.Errorf("expected 500 head series, got %d", snapshot.HeadSeries)
	}

	if snapshot.CompactionsTotal != 1 {
		t.Errorf("expected 1 compaction, got %d", snapshot.CompactionsTotal)
	}

	if snapshot.QueriesTotal != 1 {
		t.Errorf("expected 1 query, got %d", snapshot.QueriesTotal)
	}
}

func TestPrometheusExport(t *testing.T) {
	m := NewMetrics()

	m.RecordSamplesIngested(1000, 12000)
	m.RecordInsertDuration(10 * time.Millisecond)
	m.SetHeadSeries(100)
	m.RecordQuery(50*time.Millisecond, 500)

	var buf bytes.Buffer
	err := WritePrometheusMetrics(&buf, m)
	if err != nil {
		t.Fatalf("failed to write Prometheus metrics: %v", err)
	}

	output := buf.String()

	// Verify some expected metrics are present
	expectedMetrics := []string{
		"tsdb_samples_ingested_total",
		"tsdb_head_series",
		"tsdb_queries_total",
		"tsdb_insert_duration_seconds",
		"tsdb_query_duration_seconds",
	}

	for _, metric := range expectedMetrics {
		if !strings.Contains(output, metric) {
			t.Errorf("expected metric %s not found in output", metric)
		}
	}

	// Verify HELP and TYPE comments are present
	if !strings.Contains(output, "# HELP") {
		t.Error("expected HELP comments in output")
	}

	if !strings.Contains(output, "# TYPE") {
		t.Error("expected TYPE comments in output")
	}
}

func TestHistogram(t *testing.T) {
	h := NewHistogram("test_histogram")

	// Add observations
	observations := []float64{1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0}
	for _, v := range observations {
		h.Observe(v)
	}

	stats := h.GetStats()

	if stats.Count != 10 {
		t.Errorf("expected count 10, got %d", stats.Count)
	}

	if stats.Sum != 55.0 {
		t.Errorf("expected sum 55.0, got %f", stats.Sum)
	}

	if stats.Mean != 5.5 {
		t.Errorf("expected mean 5.5, got %f", stats.Mean)
	}

	if stats.Min != 1.0 {
		t.Errorf("expected min 1.0, got %f", stats.Min)
	}

	if stats.Max != 10.0 {
		t.Errorf("expected max 10.0, got %f", stats.Max)
	}

	// P50 should be around 5-6
	if stats.P50 < 4.0 || stats.P50 > 7.0 {
		t.Errorf("expected P50 around 5-6, got %f", stats.P50)
	}

	// P99 should be around 10
	if stats.P99 < 9.0 || stats.P99 > 10.0 {
		t.Errorf("expected P99 around 10, got %f", stats.P99)
	}
}

func TestHistogram_Reset(t *testing.T) {
	h := NewHistogram("test_histogram")

	h.Observe(1.0)
	h.Observe(2.0)
	h.Observe(3.0)

	stats := h.GetStats()
	if stats.Count != 3 {
		t.Errorf("expected count 3 before reset, got %d", stats.Count)
	}

	h.Reset()

	stats = h.GetStats()
	if stats.Count != 0 {
		t.Errorf("expected count 0 after reset, got %d", stats.Count)
	}

	if stats.Sum != 0 {
		t.Errorf("expected sum 0 after reset, got %f", stats.Sum)
	}
}

func TestMetricsSummary(t *testing.T) {
	m := NewMetrics()

	m.RecordSamplesIngested(10000, 120000)
	m.SetHeadSeries(500)
	m.RecordQuery(25*time.Millisecond, 1000)

	summary := GetMetricsSummary(m)

	// Verify summary contains expected sections
	expectedSections := []string{
		"Write Path:",
		"WAL:",
		"Head (MemTable):",
		"Blocks:",
		"Compaction:",
		"Queries:",
		"System:",
	}

	for _, section := range expectedSections {
		if !strings.Contains(summary, section) {
			t.Errorf("expected section %q not found in summary", section)
		}
	}
}

func TestMetricsList(t *testing.T) {
	metrics := MetricsList()

	if len(metrics) == 0 {
		t.Error("expected non-empty metrics list")
	}

	// Verify some key metrics are in the list
	expectedMetrics := []string{
		"tsdb_samples_ingested_total",
		"tsdb_queries_total",
		"tsdb_compactions_total",
	}

	for _, expected := range expectedMetrics {
		found := false
		for _, metric := range metrics {
			if metric == expected {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected metric %s not found in list", expected)
		}
	}

	// Verify list is sorted
	for i := 1; i < len(metrics); i++ {
		if metrics[i-1] > metrics[i] {
			t.Error("metrics list is not sorted")
			break
		}
	}
}

func BenchmarkMetrics_RecordSamplesIngested(b *testing.B) {
	m := NewMetrics()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			m.RecordSamplesIngested(1, 12)
		}
	})
}

func BenchmarkHistogram_Observe(b *testing.B) {
	h := NewHistogram("bench")

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			h.Observe(1.234)
		}
	})
}
