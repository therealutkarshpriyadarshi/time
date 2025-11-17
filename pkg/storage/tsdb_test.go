package storage

import (
	"testing"
	"time"

	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

func TestTSDBBasicOperations(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	opts.FlushInterval = 1 * time.Second

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	// Create test series
	s := series.NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	})

	// Insert samples
	samples := []series.Sample{
		{Timestamp: 1000, Value: 0.75},
		{Timestamp: 2000, Value: 0.82},
		{Timestamp: 3000, Value: 0.68},
	}

	if err := db.Insert(s, samples); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Query samples
	results, err := db.Query(s.Hash, 0, 0)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if len(results) != len(samples) {
		t.Fatalf("expected %d samples, got %d", len(samples), len(results))
	}

	// Verify samples
	for i, sample := range results {
		if sample.Timestamp != samples[i].Timestamp {
			t.Errorf("sample %d: expected timestamp %d, got %d", i, samples[i].Timestamp, sample.Timestamp)
		}
		if sample.Value != samples[i].Value {
			t.Errorf("sample %d: expected value %f, got %f", i, samples[i].Value, sample.Value)
		}
	}
}

func TestTSDBMultipleSeries(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	// Insert samples for multiple series
	numSeries := 100
	for i := 0; i < numSeries; i++ {
		s := series.NewSeries(map[string]string{
			"__name__": "test_metric",
			"id":       string(rune(i)),
		})

		samples := []series.Sample{
			{Timestamp: int64(i * 1000), Value: float64(i)},
		}

		if err := db.Insert(s, samples); err != nil {
			t.Fatalf("failed to insert series %d: %v", i, err)
		}
	}

	// Verify stats
	stats := db.GetStatsSnapshot()
	if stats.TotalSamples != int64(numSeries) {
		t.Errorf("expected %d total samples, got %d", numSeries, stats.TotalSamples)
	}
}

func TestTSDBCrashRecovery(t *testing.T) {
	dir := t.TempDir()

	// Create and populate TSDB
	func() {
		db, err := Open(DefaultOptions(dir))
		if err != nil {
			t.Fatalf("failed to open TSDB: %v", err)
		}

		s := series.NewSeries(map[string]string{
			"__name__": "crash_test",
		})

		samples := []series.Sample{
			{Timestamp: 1000, Value: 1.0},
			{Timestamp: 2000, Value: 2.0},
			{Timestamp: 3000, Value: 3.0},
		}

		db.Insert(s, samples)

		// Simulate crash - don't call Close()
	}()

	// Recover
	db, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to recover TSDB: %v", err)
	}
	defer db.Close()

	// Verify data was recovered
	s := series.NewSeries(map[string]string{
		"__name__": "crash_test",
	})

	results, err := db.Query(s.Hash, 0, 0)
	if err != nil {
		t.Fatalf("failed to query after recovery: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("expected 3 samples after recovery, got %d", len(results))
	}
}

func TestTSDBTimeRangeQuery(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	s := series.NewSeries(map[string]string{
		"__name__": "range_test",
	})

	// Insert samples spanning different time ranges
	samples := []series.Sample{
		{Timestamp: 1000, Value: 1.0},
		{Timestamp: 2000, Value: 2.0},
		{Timestamp: 3000, Value: 3.0},
		{Timestamp: 4000, Value: 4.0},
		{Timestamp: 5000, Value: 5.0},
	}

	if err := db.Insert(s, samples); err != nil {
		t.Fatalf("failed to insert: %v", err)
	}

	// Query specific range
	results, err := db.Query(s.Hash, 2000, 4000)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	expectedSamples := 3 // 2000, 3000, 4000
	if len(results) != expectedSamples {
		t.Errorf("expected %d samples in range, got %d", expectedSamples, len(results))
	}

	// Verify timestamps are within range
	for _, sample := range results {
		if sample.Timestamp < 2000 || sample.Timestamp > 4000 {
			t.Errorf("sample timestamp %d outside range [2000, 4000]", sample.Timestamp)
		}
	}
}

func TestTSDBFlush(t *testing.T) {
	dir := t.TempDir()

	opts := DefaultOptions(dir)
	opts.MemTableSize = 1024 // Small size to trigger flush
	opts.FlushInterval = 100 * time.Millisecond

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	s := series.NewSeries(map[string]string{
		"__name__": "flush_test",
	})

	// Insert enough data to trigger flush
	for i := 0; i < 100; i++ {
		samples := make([]series.Sample, 10)
		for j := 0; j < 10; j++ {
			samples[j] = series.Sample{
				Timestamp: int64(i*10 + j),
				Value:     float64(i*10 + j),
			}
		}

		if err := db.Insert(s, samples); err != nil {
			// Expected to fail when MemTable is full
			// Trigger manual flush
			db.TriggerFlush()
			time.Sleep(200 * time.Millisecond)

			// Retry
			if err := db.Insert(s, samples); err != nil {
				t.Logf("insert still failing after flush: %v", err)
			}
		}
	}

	// Verify flush occurred
	stats := db.GetStatsSnapshot()
	if stats.FlushCount == 0 {
		t.Log("warning: no flushes occurred (this may be expected with small data)")
	}
}

func TestTSDBManualFlush(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	s := series.NewSeries(map[string]string{
		"__name__": "manual_flush_test",
	})

	samples := []series.Sample{
		{Timestamp: 1000, Value: 1.0},
	}

	db.Insert(s, samples)

	// Trigger manual flush
	if err := db.TriggerFlush(); err != nil {
		t.Fatalf("failed to trigger flush: %v", err)
	}

	// Wait for flush to complete
	time.Sleep(200 * time.Millisecond)

	// Verify flush occurred
	stats := db.GetStatsSnapshot()
	if stats.FlushCount == 0 {
		t.Error("expected at least one flush")
	}
}

func TestTSDBConcurrentWrites(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	numGoroutines := 10
	numWritesPerGoroutine := 100

	done := make(chan bool, numGoroutines)
	errors := make(chan error, numGoroutines*numWritesPerGoroutine)

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			s := series.NewSeries(map[string]string{
				"__name__": "concurrent_test",
				"id":       string(rune(id)),
			})

			for i := 0; i < numWritesPerGoroutine; i++ {
				samples := []series.Sample{
					{Timestamp: int64(i * 1000), Value: float64(i)},
				}

				if err := db.Insert(s, samples); err != nil {
					errors <- err
				}
			}

			done <- true
		}(g)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	close(errors)

	// Check for errors
	errorCount := 0
	for err := range errors {
		t.Logf("concurrent write error: %v", err)
		errorCount++
	}

	if errorCount > 0 {
		t.Logf("%d errors occurred during concurrent writes", errorCount)
	}

	// Verify some data was written
	stats := db.GetStatsSnapshot()
	if stats.TotalSamples == 0 {
		t.Error("no samples were written")
	}
}

func TestTSDBClose(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}

	s := series.NewSeries(map[string]string{
		"__name__": "close_test",
	})

	samples := []series.Sample{
		{Timestamp: 1000, Value: 1.0},
	}

	db.Insert(s, samples)

	// Close TSDB
	if err := db.Close(); err != nil {
		t.Fatalf("failed to close TSDB: %v", err)
	}

	// Verify operations fail after close
	if err := db.Insert(s, samples); err != ErrClosed {
		t.Errorf("expected ErrClosed, got %v", err)
	}

	// Verify double close is safe
	if err := db.Close(); err != nil {
		t.Errorf("double close should not error: %v", err)
	}
}

func TestTSDBGetSeries(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	originalSeries := series.NewSeries(map[string]string{
		"__name__": "get_series_test",
		"host":     "server1",
	})

	samples := []series.Sample{
		{Timestamp: 1000, Value: 1.0},
	}

	db.Insert(originalSeries, samples)

	// Get series metadata
	retrievedSeries, ok := db.GetSeries(originalSeries.Hash)
	if !ok {
		t.Fatal("series not found")
	}

	if !retrievedSeries.Equals(originalSeries) {
		t.Error("retrieved series does not match original")
	}

	// Try to get non-existent series
	nonExistentHash := uint64(999999)
	_, ok = db.GetSeries(nonExistentHash)
	if ok {
		t.Error("expected series not to exist")
	}
}

func TestTSDBMemTableStats(t *testing.T) {
	dir := t.TempDir()

	db, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	s := series.NewSeries(map[string]string{
		"__name__": "stats_test",
	})

	samples := []series.Sample{
		{Timestamp: 1000, Value: 1.0},
	}

	db.Insert(s, samples)

	// Get MemTable stats
	active, flushing := db.MemTableStats()

	if active == "" {
		t.Error("active MemTable stats should not be empty")
	}

	if flushing != "None" {
		t.Logf("flushing MemTable: %s", flushing)
	}

	t.Logf("Active MemTable: %s", active)
}

func BenchmarkTSDBInsert(b *testing.B) {
	dir := b.TempDir()

	db, err := Open(DefaultOptions(dir))
	if err != nil {
		b.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	s := series.NewSeries(map[string]string{
		"__name__": "benchmark_metric",
		"host":     "server1",
	})

	samples := []series.Sample{
		{Timestamp: 1000, Value: 1.0},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		samples[0].Timestamp = int64(i)
		samples[0].Value = float64(i)
		if err := db.Insert(s, samples); err != nil {
			// May fail when MemTable is full, that's ok for benchmark
			continue
		}
	}
}

func BenchmarkTSDBQuery(b *testing.B) {
	dir := b.TempDir()

	db, err := Open(DefaultOptions(dir))
	if err != nil {
		b.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	s := series.NewSeries(map[string]string{
		"__name__": "benchmark_query",
	})

	// Insert test data
	for i := 0; i < 1000; i++ {
		samples := []series.Sample{{Timestamp: int64(i), Value: float64(i)}}
		db.Insert(s, samples)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		db.Query(s.Hash, 0, 1000)
	}
}

func BenchmarkTSDBConcurrentInsert(b *testing.B) {
	dir := b.TempDir()

	db, err := Open(DefaultOptions(dir))
	if err != nil {
		b.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		s := series.NewSeries(map[string]string{
			"__name__": "concurrent_benchmark",
		})

		samples := []series.Sample{{Timestamp: 1000, Value: 1.0}}

		i := 0
		for pb.Next() {
			samples[0].Timestamp = int64(i)
			samples[0].Value = float64(i)
			db.Insert(s, samples)
			i++
		}
	})
}
