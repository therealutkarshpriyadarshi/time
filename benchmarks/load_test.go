package benchmarks

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/therealutkarshpriyadarshi/time/pkg/series"
	"github.com/therealutkarshpriyadarshi/time/pkg/storage"
)

// LoadTestConfig defines parameters for load testing
type LoadTestConfig struct {
	Duration       time.Duration
	NumSeries      int
	WritersPerCore int
	ReadersPerCore int
	SamplesPerBatch int
}

// LoadTestResult contains metrics from a load test
type LoadTestResult struct {
	Duration        time.Duration
	TotalWrites     int64
	TotalReads      int64
	WriteErrors     int64
	ReadErrors      int64
	WriteThroughput float64
	ReadThroughput  float64
	AvgWriteLatency time.Duration
	AvgReadLatency  time.Duration
	P95WriteLatency time.Duration
	P99WriteLatency time.Duration
}

// RunLoadTest executes a comprehensive load test
func RunLoadTest(b *testing.B, cfg LoadTestConfig) *LoadTestResult {
	dir := b.TempDir()

	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		b.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	// Create series pool
	seriesPool := make([]*series.Series, cfg.NumSeries)
	for i := 0; i < cfg.NumSeries; i++ {
		seriesPool[i] = series.NewSeries(map[string]string{
			"__name__": fmt.Sprintf("metric_%d", i%100),
			"host":     fmt.Sprintf("host_%d", i%10),
			"instance": fmt.Sprintf("instance_%d", i),
			"job":      fmt.Sprintf("job_%d", i%5),
		})
	}

	result := &LoadTestResult{}
	ctx, cancel := context.WithTimeout(context.Background(), cfg.Duration)
	defer cancel()

	var wg sync.WaitGroup

	// Start write workers
	numWriters := cfg.WritersPerCore * 4 // Assume 4 cores
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			writeWorker(ctx, db, seriesPool, cfg.SamplesPerBatch, result)
		}(i)
	}

	// Start read workers
	numReaders := cfg.ReadersPerCore * 4
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			readWorker(ctx, db, seriesPool, result)
		}(i)
	}

	wg.Wait()

	// Calculate metrics
	elapsed := cfg.Duration.Seconds()
	result.Duration = cfg.Duration
	result.WriteThroughput = float64(result.TotalWrites) / elapsed
	result.ReadThroughput = float64(result.TotalReads) / elapsed

	return result
}

func writeWorker(ctx context.Context, db *storage.TSDB, seriesPool []*series.Series, batchSize int, result *LoadTestResult) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	timestamp := time.Now().UnixMilli()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Pick random series
			s := seriesPool[r.Intn(len(seriesPool))]

			// Create batch of samples
			samples := make([]series.Sample, batchSize)
			for i := 0; i < batchSize; i++ {
				samples[i] = series.Sample{
					Timestamp: timestamp + int64(i*1000),
					Value:     r.Float64() * 100,
				}
			}

			// Write
			start := time.Now()
			err := db.Insert(s, samples)
			_ = time.Since(start)

			if err != nil {
				atomic.AddInt64(&result.WriteErrors, 1)
			} else {
				atomic.AddInt64(&result.TotalWrites, int64(batchSize))
			}

			timestamp += int64(batchSize * 1000)
		}
	}
}

func readWorker(ctx context.Context, db *storage.TSDB, seriesPool []*series.Series, result *LoadTestResult) {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Pick random series
			s := seriesPool[r.Intn(len(seriesPool))]

			// Random time range (last 1 hour)
			now := time.Now().UnixMilli()
			start := now - int64(r.Intn(3600000)) // Random point in last hour
			end := start + int64(r.Intn(600000))   // Up to 10 min range

			// Query
			startTime := time.Now()
			_, err := db.Query(s.Hash, start, end)
			_ = time.Since(startTime)

			if err != nil {
				atomic.AddInt64(&result.ReadErrors, 1)
			} else {
				atomic.AddInt64(&result.TotalReads, 1)
			}

			// Small delay to avoid overwhelming the system
			time.Sleep(time.Millisecond * 10)
		}
	}
}

// Benchmark high-throughput write scenario
func BenchmarkLoadTest_HighWrite(b *testing.B) {
	cfg := LoadTestConfig{
		Duration:       10 * time.Second,
		NumSeries:      1000,
		WritersPerCore: 2,
		ReadersPerCore: 0,
		SamplesPerBatch: 10,
	}

	result := RunLoadTest(b, cfg)

	b.ReportMetric(result.WriteThroughput, "writes/sec")
	b.ReportMetric(float64(result.WriteErrors), "write_errors")

	b.Logf("Load Test Results:")
	b.Logf("  Duration: %v", result.Duration)
	b.Logf("  Total Writes: %d", result.TotalWrites)
	b.Logf("  Write Throughput: %.0f samples/sec", result.WriteThroughput)
	b.Logf("  Write Errors: %d", result.WriteErrors)
}

// Benchmark mixed read/write workload
func BenchmarkLoadTest_MixedWorkload(b *testing.B) {
	cfg := LoadTestConfig{
		Duration:       10 * time.Second,
		NumSeries:      500,
		WritersPerCore: 1,
		ReadersPerCore: 1,
		SamplesPerBatch: 10,
	}

	result := RunLoadTest(b, cfg)

	b.ReportMetric(result.WriteThroughput, "writes/sec")
	b.ReportMetric(result.ReadThroughput, "reads/sec")

	b.Logf("Mixed Workload Results:")
	b.Logf("  Duration: %v", result.Duration)
	b.Logf("  Total Writes: %d samples", result.TotalWrites)
	b.Logf("  Total Reads: %d queries", result.TotalReads)
	b.Logf("  Write Throughput: %.0f samples/sec", result.WriteThroughput)
	b.Logf("  Read Throughput: %.0f queries/sec", result.ReadThroughput)
	b.Logf("  Write Errors: %d", result.WriteErrors)
	b.Logf("  Read Errors: %d", result.ReadErrors)
}

// Benchmark high-cardinality scenario
func BenchmarkLoadTest_HighCardinality(b *testing.B) {
	cfg := LoadTestConfig{
		Duration:       10 * time.Second,
		NumSeries:      10000, // Many series
		WritersPerCore: 2,
		ReadersPerCore: 1,
		SamplesPerBatch: 5,
	}

	result := RunLoadTest(b, cfg)

	b.ReportMetric(result.WriteThroughput, "writes/sec")
	b.ReportMetric(float64(cfg.NumSeries), "num_series")

	b.Logf("High Cardinality Results:")
	b.Logf("  Series Count: %d", cfg.NumSeries)
	b.Logf("  Total Writes: %d samples", result.TotalWrites)
	b.Logf("  Write Throughput: %.0f samples/sec", result.WriteThroughput)
}

// Benchmark write performance with different batch sizes
func BenchmarkWriteBatchSizes(b *testing.B) {
	batchSizes := []int{1, 10, 50, 100, 500, 1000}

	for _, batchSize := range batchSizes {
		b.Run(fmt.Sprintf("batch_%d", batchSize), func(b *testing.B) {
			dir := b.TempDir()
			db, err := storage.Open(storage.DefaultOptions(dir))
			if err != nil {
				b.Fatalf("failed to open TSDB: %v", err)
			}
			defer db.Close()

			s := series.NewSeries(map[string]string{
				"__name__": "batch_test",
				"host":     "server1",
			})

			samples := make([]series.Sample, batchSize)
			for i := 0; i < batchSize; i++ {
				samples[i] = series.Sample{
					Timestamp: int64(i * 1000),
					Value:     float64(i),
				}
			}

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Update timestamps
				baseTime := int64(i * batchSize * 1000)
				for j := 0; j < batchSize; j++ {
					samples[j].Timestamp = baseTime + int64(j*1000)
				}

				if err := db.Insert(s, samples); err != nil {
					// Continue on error (e.g., MemTable full)
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

// Benchmark query performance with different time ranges
func BenchmarkQueryTimeRanges(b *testing.B) {
	dir := b.TempDir()
	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		b.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	s := series.NewSeries(map[string]string{
		"__name__": "range_test",
	})

	// Insert 30 days of data at 1-minute intervals
	thirtyDays := int64(30 * 24 * 60)
	for i := int64(0); i < thirtyDays; i++ {
		samples := []series.Sample{{Timestamp: i * 60000, Value: float64(i)}}
		db.Insert(s, samples)
	}

	timeRanges := []struct {
		name  string
		hours int
	}{
		{"1_hour", 1},
		{"6_hours", 6},
		{"1_day", 24},
		{"7_days", 24 * 7},
		{"30_days", 24 * 30},
	}

	for _, tr := range timeRanges {
		b.Run(tr.name, func(b *testing.B) {
			rangeMs := int64(tr.hours * 60 * 60 * 1000)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				start := int64(i%10) * rangeMs
				end := start + rangeMs

				results, err := db.Query(s.Hash, start, end)
				if err != nil {
					b.Fatalf("query failed: %v", err)
				}
				_ = results
			}

			b.StopTimer()
			queriesPerSec := float64(b.N) / b.Elapsed().Seconds()
			b.ReportMetric(queriesPerSec, "queries/sec")
		})
	}
}
