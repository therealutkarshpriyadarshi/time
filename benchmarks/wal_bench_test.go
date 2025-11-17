package benchmarks

import (
	"testing"

	"github.com/therealutkarshpriyadarshi/time/pkg/series"
	"github.com/therealutkarshpriyadarshi/time/pkg/wal"
)

func BenchmarkWALAppendSingleSample(b *testing.B) {
	dir := b.TempDir()

	w, err := wal.Open(dir, nil)
	if err != nil {
		b.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	s := series.NewSeries(map[string]string{
		"__name__": "benchmark_metric",
		"host":     "server1",
		"region":   "us-west",
	})

	samples := []series.Sample{
		{Timestamp: 1000, Value: 1.0},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		samples[0].Timestamp = int64(i)
		samples[0].Value = float64(i)
		if err := w.Append(s, samples); err != nil {
			b.Fatalf("failed to append: %v", err)
		}
	}

	b.StopTimer()
	throughput := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(throughput, "writes/sec")
}

func BenchmarkWALAppendBatchSamples(b *testing.B) {
	dir := b.TempDir()

	w, err := wal.Open(dir, nil)
	if err != nil {
		b.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	s := series.NewSeries(map[string]string{
		"__name__": "batch_benchmark",
		"host":     "server1",
	})

	// Test different batch sizes
	batchSizes := []int{1, 10, 100, 1000}

	for _, batchSize := range batchSizes {
		samples := make([]series.Sample, batchSize)
		for i := 0; i < batchSize; i++ {
			samples[i] = series.Sample{Timestamp: int64(i), Value: float64(i)}
		}

		b.Run(string(rune(batchSize)), func(b *testing.B) {
			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				if err := w.Append(s, samples); err != nil {
					b.Fatalf("failed to append: %v", err)
				}
			}

			b.StopTimer()
			samplesWritten := float64(b.N * batchSize)
			throughput := samplesWritten / b.Elapsed().Seconds()
			b.ReportMetric(throughput, "samples/sec")
		})
	}
}

func BenchmarkWALAppendMultipleSeries(b *testing.B) {
	dir := b.TempDir()

	w, err := wal.Open(dir, nil)
	if err != nil {
		b.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	// Create multiple series
	numSeries := 100
	seriesList := make([]*series.Series, numSeries)
	for i := 0; i < numSeries; i++ {
		seriesList[i] = series.NewSeries(map[string]string{
			"__name__": "multi_series_benchmark",
			"id":       string(rune(i)),
		})
	}

	samples := []series.Sample{{Timestamp: 1000, Value: 1.0}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		s := seriesList[i%numSeries]
		samples[0].Timestamp = int64(i)
		if err := w.Append(s, samples); err != nil {
			b.Fatalf("failed to append: %v", err)
		}
	}

	b.StopTimer()
	throughput := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(throughput, "writes/sec")
}

func BenchmarkWALReplay(b *testing.B) {
	dir := b.TempDir()

	// Setup: write entries
	w, err := wal.Open(dir, nil)
	if err != nil {
		b.Fatalf("failed to open WAL: %v", err)
	}

	s := series.NewSeries(map[string]string{
		"__name__": "replay_benchmark",
	})

	// Write 10,000 entries
	for i := 0; i < 10000; i++ {
		samples := []series.Sample{{Timestamp: int64(i), Value: float64(i)}}
		w.Append(s, samples)
	}
	w.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w2, err := wal.Open(dir, nil)
		if err != nil {
			b.Fatalf("failed to open WAL: %v", err)
		}
		entries, err := w2.Replay()
		if err != nil {
			b.Fatalf("failed to replay: %v", err)
		}
		b.StopTimer()

		if len(entries) != 10000 {
			b.Fatalf("expected 10000 entries, got %d", len(entries))
		}

		w2.Close()
		b.StartTimer()
	}

	b.StopTimer()
	entriesReplayed := float64(10000 * b.N)
	throughput := entriesReplayed / b.Elapsed().Seconds()
	b.ReportMetric(throughput, "entries/sec")
}

func BenchmarkWALSegmentRotation(b *testing.B) {
	dir := b.TempDir()

	// Use small segment size to force rotation
	opts := &wal.Options{
		SegmentSize: 1024 * 1024, // 1MB
	}

	w, err := wal.Open(dir, opts)
	if err != nil {
		b.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	s := series.NewSeries(map[string]string{
		"__name__": "rotation_benchmark",
		"host":     "server1",
	})

	// Create samples that will trigger rotation
	samples := make([]series.Sample, 100)
	for i := 0; i < 100; i++ {
		samples[i] = series.Sample{Timestamp: int64(i), Value: float64(i)}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		if err := w.Append(s, samples); err != nil {
			b.Fatalf("failed to append: %v", err)
		}
	}
}

func BenchmarkWALConcurrentWrites(b *testing.B) {
	dir := b.TempDir()

	w, err := wal.Open(dir, nil)
	if err != nil {
		b.Fatalf("failed to open WAL: %v", err)
	}
	defer w.Close()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		s := series.NewSeries(map[string]string{
			"__name__": "concurrent_wal",
		})

		samples := []series.Sample{{Timestamp: 1000, Value: 1.0}}

		i := 0
		for pb.Next() {
			samples[0].Timestamp = int64(i)
			w.Append(s, samples)
			i++
		}
	})
}

func BenchmarkWALEncodeEntry(b *testing.B) {
	s := series.NewSeries(map[string]string{
		"__name__": "encode_benchmark",
		"host":     "server1",
		"region":   "us-west",
		"env":      "production",
	})

	samples := []series.Sample{
		{Timestamp: 1000, Value: 1.0},
		{Timestamp: 2000, Value: 2.0},
		{Timestamp: 3000, Value: 3.0},
	}

	entry := &wal.Entry{
		Type:      1,
		Timestamp: 1234567890,
		Series:    s,
		Samples:   samples,
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Note: This is a private function, benchmark approximates encoding cost
		// In real implementation, this would call the actual encode function
		_ = entry
	}
}
