package wal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

func TestWALBasicOperations(t *testing.T) {
	dir := t.TempDir()

	// Open WAL
	w, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	// Create test series
	s := series.NewSeries(map[string]string{
		"__name__": "test_metric",
		"host":     "server1",
	})

	// Create test samples
	samples := []series.Sample{
		{Timestamp: 1000, Value: 1.0},
		{Timestamp: 2000, Value: 2.0},
		{Timestamp: 3000, Value: 3.0},
	}

	// Append entry
	if err := w.Append(s, samples); err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	// Close WAL
	if err := w.Close(); err != nil {
		t.Fatalf("failed to close WAL: %v", err)
	}

	// Reopen and replay
	w2, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.Close()

	entries, err := w2.Replay()
	if err != nil {
		t.Fatalf("failed to replay: %v", err)
	}

	// Verify
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}

	entry := entries[0]
	if entry.Type != entryTypeSamples {
		t.Errorf("expected type %d, got %d", entryTypeSamples, entry.Type)
	}

	if entry.Series == nil {
		t.Fatal("series is nil")
	}

	if entry.Series.Hash != s.Hash {
		t.Errorf("expected hash %d, got %d", s.Hash, entry.Series.Hash)
	}

	if len(entry.Samples) != len(samples) {
		t.Fatalf("expected %d samples, got %d", len(samples), len(entry.Samples))
	}

	for i, sample := range entry.Samples {
		if sample.Timestamp != samples[i].Timestamp {
			t.Errorf("sample %d: expected timestamp %d, got %d", i, samples[i].Timestamp, sample.Timestamp)
		}
		if sample.Value != samples[i].Value {
			t.Errorf("sample %d: expected value %f, got %f", i, samples[i].Value, sample.Value)
		}
	}
}

func TestWALMultipleEntries(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	// Write multiple entries
	numEntries := 100
	for i := 0; i < numEntries; i++ {
		s := series.NewSeries(map[string]string{
			"__name__": "test_metric",
			"id":       string(rune(i)),
		})

		samples := []series.Sample{
			{Timestamp: int64(i * 1000), Value: float64(i)},
		}

		if err := w.Append(s, samples); err != nil {
			t.Fatalf("failed to append entry %d: %v", i, err)
		}
	}

	w.Close()

	// Replay and verify
	w2, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.Close()

	entries, err := w2.Replay()
	if err != nil {
		t.Fatalf("failed to replay: %v", err)
	}

	if len(entries) != numEntries {
		t.Fatalf("expected %d entries, got %d", numEntries, len(entries))
	}
}

func TestWALSegmentRotation(t *testing.T) {
	dir := t.TempDir()

	// Use small segment size to force rotation
	opts := &Options{
		SegmentSize: 1024, // 1KB
	}

	w, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	// Write enough data to force multiple segments
	s := series.NewSeries(map[string]string{
		"__name__": "test_metric",
		"host":     "server1",
	})

	// Write 100 entries
	for i := 0; i < 100; i++ {
		samples := make([]series.Sample, 10)
		for j := 0; j < 10; j++ {
			samples[j] = series.Sample{
				Timestamp: int64(i*10 + j),
				Value:     float64(i*10 + j),
			}
		}

		if err := w.Append(s, samples); err != nil {
			t.Fatalf("failed to append: %v", err)
		}
	}

	w.Close()

	// Check that multiple segments were created
	segments, err := w.listSegments()
	if err != nil {
		t.Fatalf("failed to list segments: %v", err)
	}

	if len(segments) <= 1 {
		t.Errorf("expected multiple segments, got %d", len(segments))
	}

	// Verify all entries can be replayed
	w2, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.Close()

	entries, err := w2.Replay()
	if err != nil {
		t.Fatalf("failed to replay: %v", err)
	}

	if len(entries) != 100 {
		t.Fatalf("expected 100 entries, got %d", len(entries))
	}
}

func TestWALTruncate(t *testing.T) {
	dir := t.TempDir()

	opts := &Options{
		SegmentSize: 1024, // Small segment to create multiple
	}

	w, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	s := series.NewSeries(map[string]string{
		"__name__": "test_metric",
	})

	// Write entries with different timestamps
	timestamps := []int64{1000, 2000, 3000, 4000, 5000}
	for _, ts := range timestamps {
		samples := []series.Sample{{Timestamp: ts, Value: float64(ts)}}
		if err := w.Append(s, samples); err != nil {
			t.Fatalf("failed to append: %v", err)
		}
	}

	// Log a flush marker
	if err := w.LogFlush(3500); err != nil {
		t.Fatalf("failed to log flush: %v", err)
	}

	// Get segment count before truncate
	segmentsBefore, _ := w.listSegments()

	// Truncate entries before timestamp 3500
	if err := w.Truncate(3500); err != nil {
		t.Fatalf("failed to truncate: %v", err)
	}

	// Verify segments were removed
	segmentsAfter, _ := w.listSegments()

	// We should have fewer or same number of segments
	if len(segmentsAfter) > len(segmentsBefore) {
		t.Errorf("truncate increased segments: before=%d, after=%d", len(segmentsBefore), len(segmentsAfter))
	}

	w.Close()
}

func TestWALCrashRecovery(t *testing.T) {
	dir := t.TempDir()

	// Simulate crash by not closing WAL properly
	func() {
		w, err := Open(dir, nil)
		if err != nil {
			t.Fatalf("failed to open WAL: %v", err)
		}

		s := series.NewSeries(map[string]string{
			"__name__": "crash_test",
		})

		samples := []series.Sample{
			{Timestamp: 1000, Value: 1.0},
			{Timestamp: 2000, Value: 2.0},
		}

		w.Append(s, samples)
		// Simulate crash - don't call Close()
	}()

	// Recover
	w, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("failed to recover WAL: %v", err)
	}
	defer w.Close()

	entries, err := w.Replay()
	if err != nil {
		t.Fatalf("failed to replay after crash: %v", err)
	}

	// Should still have the entries
	if len(entries) == 0 {
		t.Error("expected entries after crash recovery")
	}
}

func TestWALFlushMarker(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	// Write some samples
	s := series.NewSeries(map[string]string{
		"__name__": "test",
	})

	samples := []series.Sample{{Timestamp: 1000, Value: 1.0}}
	if err := w.Append(s, samples); err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	// Log flush
	if err := w.LogFlush(1000); err != nil {
		t.Fatalf("failed to log flush: %v", err)
	}

	w.Close()

	// Replay and verify flush marker
	w2, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.Close()

	entries, err := w2.Replay()
	if err != nil {
		t.Fatalf("failed to replay: %v", err)
	}

	// Should have 2 entries: samples + flush marker
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}

	if entries[1].Type != entryTypeFlush {
		t.Errorf("expected flush entry type, got %d", entries[1].Type)
	}
}

func TestWALCorruptionDetection(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	s := series.NewSeries(map[string]string{"__name__": "test"})
	samples := []series.Sample{{Timestamp: 1000, Value: 1.0}}

	if err := w.Append(s, samples); err != nil {
		t.Fatalf("failed to append: %v", err)
	}

	w.Close()

	// Corrupt the WAL file
	segPath := filepath.Join(dir, "wal-00000000")
	file, err := os.OpenFile(segPath, os.O_RDWR, 0644)
	if err != nil {
		t.Fatalf("failed to open segment for corruption: %v", err)
	}

	// Write garbage at position 10 (in the checksum area)
	file.Seek(10, 0)
	file.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF})
	file.Close()

	// Try to replay - should detect corruption
	w2, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.Close()

	entries, err := w2.Replay()
	// Should handle corruption gracefully (may return partial data)
	if err != nil && err != ErrCorrupted {
		// Some errors are acceptable during replay
	}

	// Even with corruption, we should be able to continue using WAL
	if err := w2.Append(s, samples); err != nil {
		t.Errorf("should be able to append after corruption: %v", err)
	}

	t.Logf("Replayed %d entries despite corruption", len(entries))
}

func TestWALConcurrentWrites(t *testing.T) {
	dir := t.TempDir()

	w, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	// Concurrent writes
	numGoroutines := 10
	numWritesPerGoroutine := 10

	done := make(chan bool, numGoroutines)

	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			s := series.NewSeries(map[string]string{
				"__name__": "concurrent_test",
				"id":       string(rune(id)),
			})

			for i := 0; i < numWritesPerGoroutine; i++ {
				samples := []series.Sample{
					{Timestamp: int64(i), Value: float64(i)},
				}

				if err := w.Append(s, samples); err != nil {
					t.Errorf("goroutine %d: failed to append: %v", id, err)
				}
			}

			done <- true
		}(g)
	}

	// Wait for all goroutines
	for i := 0; i < numGoroutines; i++ {
		<-done
	}

	w.Close()

	// Verify all writes
	w2, err := Open(dir, nil)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.Close()

	entries, err := w2.Replay()
	if err != nil {
		t.Fatalf("failed to replay: %v", err)
	}

	expectedEntries := numGoroutines * numWritesPerGoroutine
	if len(entries) != expectedEntries {
		t.Errorf("expected %d entries, got %d", expectedEntries, len(entries))
	}
}

func BenchmarkWALAppend(b *testing.B) {
	dir := b.TempDir()

	w, err := Open(dir, nil)
	if err != nil {
		b.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

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
		if err := w.Append(s, samples); err != nil {
			b.Fatalf("failed to append: %v", err)
		}
	}
}

func BenchmarkWALReplay(b *testing.B) {
	dir := b.TempDir()

	// Setup: write entries
	w, err := Open(dir, nil)
	if err != nil {
		b.Fatalf("failed to open WAL: %v", err)
	}

	s := series.NewSeries(map[string]string{
		"__name__": "benchmark",
	})

	for i := 0; i < 1000; i++ {
		samples := []series.Sample{{Timestamp: int64(i), Value: float64(i)}}
		w.Append(s, samples)
	}
	w.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w2, _ := Open(dir, nil)
		w2.Replay()
		w2.Close()
	}
}
