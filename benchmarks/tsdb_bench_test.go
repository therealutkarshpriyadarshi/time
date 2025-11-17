package benchmarks

import (
	"testing"

	"github.com/therealutkarshpriyadarshi/time/pkg/series"
	"github.com/therealutkarshpriyadarshi/time/pkg/storage"
)

func BenchmarkTSDBInsertSingleSample(b *testing.B) {
	dir := b.TempDir()

	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		b.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

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
		if err := db.Insert(s, samples); err != nil {
			// May fail when MemTable is full
			continue
		}
	}

	b.StopTimer()
	throughput := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(throughput, "writes/sec")
}

func BenchmarkTSDBInsertBatchSamples(b *testing.B) {
	dir := b.TempDir()

	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		b.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

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
				if err := db.Insert(s, samples); err != nil {
					continue
				}
			}

			b.StopTimer()
			samplesWritten := float64(b.N * batchSize)
			throughput := samplesWritten / b.Elapsed().Seconds()
			b.ReportMetric(throughput, "samples/sec")
		})
	}
}

func BenchmarkTSDBInsertMultipleSeries(b *testing.B) {
	dir := b.TempDir()

	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		b.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	// Create multiple series
	numSeries := 1000
	seriesList := make([]*series.Series, numSeries)
	for i := 0; i < numSeries; i++ {
		seriesList[i] = series.NewSeries(map[string]string{
			"__name__": "multi_series_benchmark",
			"id":       string(rune(i % 256)),
			"shard":    string(rune((i / 256) % 256)),
		})
	}

	samples := []series.Sample{{Timestamp: 1000, Value: 1.0}}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		s := seriesList[i%numSeries]
		samples[0].Timestamp = int64(i)
		if err := db.Insert(s, samples); err != nil {
			continue
		}
	}

	b.StopTimer()
	throughput := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(throughput, "writes/sec")
}

func BenchmarkTSDBQuery(b *testing.B) {
	dir := b.TempDir()

	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		b.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	s := series.NewSeries(map[string]string{
		"__name__": "query_benchmark",
		"host":     "server1",
	})

	// Insert test data
	for i := 0; i < 10000; i++ {
		samples := []series.Sample{{Timestamp: int64(i * 1000), Value: float64(i)}}
		db.Insert(s, samples)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		results, err := db.Query(s.Hash, 0, 10000000)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
		_ = results
	}

	b.StopTimer()
	throughput := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(throughput, "queries/sec")
}

func BenchmarkTSDBQueryTimeRange(b *testing.B) {
	dir := b.TempDir()

	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		b.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	s := series.NewSeries(map[string]string{
		"__name__": "range_query_benchmark",
	})

	// Insert 1 week of data at 1-minute intervals
	oneWeek := int64(7 * 24 * 60)
	for i := int64(0); i < oneWeek; i++ {
		samples := []series.Sample{{Timestamp: i * 60000, Value: float64(i)}}
		db.Insert(s, samples)
	}

	// Query 1 day range
	oneDayMs := int64(24 * 60 * 60000)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		start := int64(i%6) * oneDayMs
		end := start + oneDayMs

		results, err := db.Query(s.Hash, start, end)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
		_ = results
	}
}

func BenchmarkTSDBConcurrentInsert(b *testing.B) {
	dir := b.TempDir()

	db, err := storage.Open(storage.DefaultOptions(dir))
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

func BenchmarkTSDBConcurrentQuery(b *testing.B) {
	dir := b.TempDir()

	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		b.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	s := series.NewSeries(map[string]string{
		"__name__": "concurrent_query_benchmark",
	})

	// Insert test data
	for i := 0; i < 1000; i++ {
		samples := []series.Sample{{Timestamp: int64(i), Value: float64(i)}}
		db.Insert(s, samples)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			db.Query(s.Hash, 0, 1000)
		}
	})
}

func BenchmarkTSDBMixedWorkload(b *testing.B) {
	dir := b.TempDir()

	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		b.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	s := series.NewSeries(map[string]string{
		"__name__": "mixed_workload",
	})

	// Pre-populate some data
	for i := 0; i < 1000; i++ {
		samples := []series.Sample{{Timestamp: int64(i), Value: float64(i)}}
		db.Insert(s, samples)
	}

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		samples := []series.Sample{{Timestamp: 1000, Value: 1.0}}
		i := 0

		for pb.Next() {
			// 80% writes, 20% reads
			if i%5 == 0 {
				db.Query(s.Hash, 0, 1000)
			} else {
				samples[0].Timestamp = int64(i)
				db.Insert(s, samples)
			}
			i++
		}
	})
}

func BenchmarkTSDBRecovery(b *testing.B) {
	dir := b.TempDir()

	// Setup: create and populate a TSDB
	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		b.Fatalf("failed to open TSDB: %v", err)
	}

	s := series.NewSeries(map[string]string{
		"__name__": "recovery_benchmark",
	})

	// Insert 1000 samples
	for i := 0; i < 1000; i++ {
		samples := []series.Sample{{Timestamp: int64(i), Value: float64(i)}}
		db.Insert(s, samples)
	}

	db.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Reopen and recover
		db2, err := storage.Open(storage.DefaultOptions(dir))
		if err != nil {
			b.Fatalf("failed to recover: %v", err)
		}
		b.StopTimer()
		db2.Close()
		b.StartTimer()
	}
}
