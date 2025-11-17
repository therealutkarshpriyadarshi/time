package benchmarks

import (
	"fmt"
	"testing"

	"github.com/therealutkarshpriyadarshi/time/pkg/series"
	"github.com/therealutkarshpriyadarshi/time/pkg/storage"
)

// BenchmarkMemTableInsert measures insert performance
func BenchmarkMemTableInsert(b *testing.B) {
	mt := storage.NewMemTable()
	s := series.NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	})

	samples := []series.Sample{{Timestamp: 1000, Value: 0.85}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Clear periodically to avoid filling up
		if i%10000 == 0 && i > 0 {
			mt.Clear()
		}
		mt.Insert(s, samples)
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "inserts/sec")
}

// BenchmarkMemTableInsertBatch measures batch insert performance
func BenchmarkMemTableInsertBatch(b *testing.B) {
	mt := storage.NewMemTable()
	s := series.NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	})

	// Batch of 100 samples
	samples := make([]series.Sample, 100)
	for i := range samples {
		samples[i] = series.Sample{
			Timestamp: int64(i * 1000),
			Value:     float64(i),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if i%100 == 0 && i > 0 {
			mt.Clear()
		}
		mt.Insert(s, samples)
	}

	b.ReportMetric(float64(b.N*100)/b.Elapsed().Seconds(), "samples/sec")
}

// BenchmarkMemTableInsertMultipleSeries measures insert with many different series
func BenchmarkMemTableInsertMultipleSeries(b *testing.B) {
	mt := storage.NewMemTable()

	numSeries := 1000
	seriesList := make([]*series.Series, numSeries)
	for i := 0; i < numSeries; i++ {
		seriesList[i] = series.NewSeries(map[string]string{
			"__name__": "metric",
			"host":     fmt.Sprintf("server%d", i),
		})
	}

	samples := []series.Sample{{Timestamp: 1000, Value: 0.85}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := seriesList[i%numSeries]
		if i%100000 == 0 && i > 0 {
			mt.Clear()
		}
		mt.Insert(s, samples)
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "inserts/sec")
}

// BenchmarkMemTableQuery measures query performance
func BenchmarkMemTableQuery(b *testing.B) {
	mt := storage.NewMemTable()
	s := series.NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	})

	// Insert 1000 samples
	samples := make([]series.Sample, 1000)
	for i := range samples {
		samples[i] = series.Sample{
			Timestamp: int64(i * 1000),
			Value:     float64(i),
		}
	}
	mt.Insert(s, samples)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := mt.Query(s.Hash, 0, 0)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

// BenchmarkMemTableQueryTimeRange measures time range query performance
func BenchmarkMemTableQueryTimeRange(b *testing.B) {
	mt := storage.NewMemTable()
	s := series.NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	})

	// Insert 10000 samples
	samples := make([]series.Sample, 10000)
	for i := range samples {
		samples[i] = series.Sample{
			Timestamp: int64(i * 1000),
			Value:     float64(i),
		}
	}
	mt.Insert(s, samples)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Query a 1000-sample window
		start := int64((i % 9000) * 1000)
		end := start + 1000000
		_, err := mt.Query(s.Hash, start, end)
		if err != nil {
			b.Fatal(err)
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

// BenchmarkMemTableConcurrentInsert measures concurrent insert performance
func BenchmarkMemTableConcurrentInsert(b *testing.B) {
	mt := storage.NewMemTable()
	s := series.NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	})

	samples := []series.Sample{{Timestamp: 1000, Value: 0.85}}

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10000 == 0 && i > 0 {
				// Note: Clearing in parallel benchmark is tricky,
				// in real usage you'd swap to a new memtable
			}
			mt.Insert(s, samples)
			i++
		}
	})

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "inserts/sec")
}

// BenchmarkMemTableConcurrentRead measures concurrent read performance
func BenchmarkMemTableConcurrentRead(b *testing.B) {
	mt := storage.NewMemTable()
	s := series.NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	})

	samples := make([]series.Sample, 1000)
	for i := range samples {
		samples[i] = series.Sample{
			Timestamp: int64(i * 1000),
			Value:     float64(i),
		}
	}
	mt.Insert(s, samples)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mt.Query(s.Hash, 0, 0)
		}
	})

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

// BenchmarkMemTableMixedWorkload measures mixed read/write performance
func BenchmarkMemTableMixedWorkload(b *testing.B) {
	mt := storage.NewMemTable()
	s := series.NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	})

	samples := []series.Sample{{Timestamp: 1000, Value: 0.85}}

	// Pre-populate
	for i := 0; i < 1000; i++ {
		mt.Insert(s, samples)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				mt.Insert(s, samples)
			} else {
				mt.Query(s.Hash, 0, 0)
			}
			i++
		}
	})

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

// BenchmarkMemTable100KWritesPerSec tests if we can achieve target write throughput
func BenchmarkMemTable100KWritesPerSec(b *testing.B) {
	mt := storage.NewMemTable()

	numSeries := 100
	seriesList := make([]*series.Series, numSeries)
	for i := 0; i < numSeries; i++ {
		seriesList[i] = series.NewSeries(map[string]string{
			"__name__": "metric",
			"host":     fmt.Sprintf("server%d", i),
		})
	}

	samples := []series.Sample{{Timestamp: 1000, Value: 0.85}}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := seriesList[i%numSeries]
		if i%100000 == 0 && i > 0 {
			mt.Clear()
		}
		err := mt.Insert(s, samples)
		if err != nil && err != storage.ErrMemTableFull {
			b.Fatal(err)
		}
	}

	writesPerSec := float64(b.N) / b.Elapsed().Seconds()
	b.ReportMetric(writesPerSec, "writes/sec")

	if writesPerSec < 100000 {
		b.Logf("Warning: Write throughput is %0.f writes/sec, target is 100K+", writesPerSec)
	}
}
