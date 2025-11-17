// +build stress

package tests

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

// TestStress_HighWriteThroughput tests sustained high write load
func TestStress_HighWriteThroughput(t *testing.T) {
	dir := t.TempDir()

	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	// Test parameters
	duration := 5 * time.Minute
	numWorkers := 10
	targetWritesPerSec := 100000

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var (
		totalWrites   atomic.Int64
		totalErrors   atomic.Int64
		wg            sync.WaitGroup
	)

	// Create series pool
	seriesPool := make([]*series.Series, 1000)
	for i := 0; i < len(seriesPool); i++ {
		seriesPool[i] = series.NewSeries(map[string]string{
			"__name__": fmt.Sprintf("metric_%d", i%100),
			"host":     fmt.Sprintf("host_%d", i%20),
			"instance": fmt.Sprintf("instance_%d", i),
		})
	}

	t.Logf("Starting stress test: %d workers, target %d writes/sec, duration %v",
		numWorkers, targetWritesPerSec, duration)

	startTime := time.Now()

	// Start workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
			ticker := time.NewTicker(time.Second / time.Duration(targetWritesPerSec/numWorkers))
			defer ticker.Stop()

			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					s := seriesPool[r.Intn(len(seriesPool))]
					samples := []series.Sample{{
						Timestamp: time.Now().UnixMilli(),
						Value:     r.Float64() * 100,
					}}

					if err := db.Insert(s, samples); err != nil {
						totalErrors.Add(1)
					} else {
						totalWrites.Add(1)
					}
				}
			}
		}(w)
	}

	// Progress reporter
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				elapsed := time.Since(startTime).Seconds()
				writes := totalWrites.Load()
				errors := totalErrors.Load()
				throughput := float64(writes) / elapsed

				t.Logf("Progress: %d writes, %d errors, %.0f writes/sec",
					writes, errors, throughput)
			}
		}
	}()

	wg.Wait()

	elapsed := time.Since(startTime)
	writes := totalWrites.Load()
	errors := totalErrors.Load()
	throughput := float64(writes) / elapsed.Seconds()

	t.Logf("Stress test completed:")
	t.Logf("  Duration: %v", elapsed)
	t.Logf("  Total Writes: %d", writes)
	t.Logf("  Total Errors: %d", errors)
	t.Logf("  Throughput: %.0f writes/sec", throughput)
	t.Logf("  Error Rate: %.2f%%", float64(errors)/float64(writes+errors)*100)

	if throughput < float64(targetWritesPerSec)*0.5 {
		t.Errorf("throughput too low: %.0f writes/sec (target: %d)", throughput, targetWritesPerSec)
	}
}

// TestStress_HighCardinality tests handling many unique series
func TestStress_HighCardinality(t *testing.T) {
	dir := t.TempDir()

	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	numSeries := 100000
	samplesPerSeries := 10

	t.Logf("Writing %d series with %d samples each", numSeries, samplesPerSeries)

	startTime := time.Now()
	var writeErrors atomic.Int64

	// Write data for many series
	for i := 0; i < numSeries; i++ {
		s := series.NewSeries(map[string]string{
			"__name__": "high_cardinality_metric",
			"id":       fmt.Sprintf("%d", i),
			"shard":    fmt.Sprintf("%d", i%100),
			"region":   fmt.Sprintf("region_%d", i%10),
		})

		samples := make([]series.Sample, samplesPerSeries)
		now := time.Now().UnixMilli()

		for j := 0; j < samplesPerSeries; j++ {
			samples[j] = series.Sample{
				Timestamp: now + int64(j*1000),
				Value:     float64(i*samplesPerSeries + j),
			}
		}

		if err := db.Insert(s, samples); err != nil {
			writeErrors.Add(1)
		}

		if i > 0 && i%10000 == 0 {
			elapsed := time.Since(startTime)
			rate := float64(i*samplesPerSeries) / elapsed.Seconds()
			t.Logf("Progress: %d series, %.0f samples/sec", i, rate)
		}
	}

	elapsed := time.Since(startTime)
	totalSamples := int64(numSeries * samplesPerSeries)
	throughput := float64(totalSamples) / elapsed.Seconds()

	t.Logf("High cardinality test completed:")
	t.Logf("  Duration: %v", elapsed)
	t.Logf("  Series: %d", numSeries)
	t.Logf("  Total Samples: %d", totalSamples)
	t.Logf("  Write Errors: %d", writeErrors.Load())
	t.Logf("  Throughput: %.0f samples/sec", throughput)

	stats := db.GetStatsSnapshot()
	t.Logf("  Active Series: %d", stats.ActiveSeries)
	t.Logf("  Total Samples in DB: %d", stats.TotalSamples)
}

// TestStress_MixedWorkload tests realistic mixed read/write load
func TestStress_MixedWorkload(t *testing.T) {
	dir := t.TempDir()

	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	duration := 3 * time.Minute
	numSeries := 1000

	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var (
		totalWrites  atomic.Int64
		totalReads   atomic.Int64
		writeErrors  atomic.Int64
		readErrors   atomic.Int64
		wg           sync.WaitGroup
	)

	// Create series pool
	seriesPool := make([]*series.Series, numSeries)
	for i := 0; i < len(seriesPool); i++ {
		seriesPool[i] = series.NewSeries(map[string]string{
			"__name__": "mixed_workload_metric",
			"id":       fmt.Sprintf("%d", i),
		})
	}

	t.Logf("Starting mixed workload test: duration %v", duration)

	// Write workers (70% of load)
	for w := 0; w < 7; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

			for {
				select {
				case <-ctx.Done():
					return
				default:
					s := seriesPool[r.Intn(len(seriesPool))]
					samples := []series.Sample{{
						Timestamp: time.Now().UnixMilli(),
						Value:     r.Float64() * 100,
					}}

					if err := db.Insert(s, samples); err != nil {
						writeErrors.Add(1)
					} else {
						totalWrites.Add(1)
					}

					time.Sleep(time.Millisecond * 10)
				}
			}
		}(w)
	}

	// Read workers (30% of load)
	for r := 0; r < 3; r++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID+1000)))

			for {
				select {
				case <-ctx.Done():
					return
				default:
					s := seriesPool[rng.Intn(len(seriesPool))]
					now := time.Now().UnixMilli()
					start := now - int64(rng.Intn(3600000)) // Random point in last hour
					end := start + int64(rng.Intn(600000))   // Up to 10 min range

					_, err := db.Query(s.Hash, start, end)
					if err != nil {
						readErrors.Add(1)
					} else {
						totalReads.Add(1)
					}

					time.Sleep(time.Millisecond * 50)
				}
			}
		}(r)
	}

	wg.Wait()

	t.Logf("Mixed workload test completed:")
	t.Logf("  Writes: %d (errors: %d)", totalWrites.Load(), writeErrors.Load())
	t.Logf("  Reads: %d (errors: %d)", totalReads.Load(), readErrors.Load())
}

// TestStress_MemoryPressure tests behavior under memory constraints
func TestStress_MemoryPressure(t *testing.T) {
	dir := t.TempDir()

	// Configure with small MemTable to force frequent flushes
	opts := storage.DefaultOptions(dir)
	opts.MemTableSize = 10 * 1024 * 1024 // 10 MB

	db, err := storage.Open(opts)
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	duration := 2 * time.Minute
	ctx, cancel := context.WithTimeout(context.Background(), duration)
	defer cancel()

	var totalWrites atomic.Int64

	s := series.NewSeries(map[string]string{
		"__name__": "memory_pressure_test",
	})

	t.Logf("Starting memory pressure test (small MemTable): duration %v", duration)

	// Aggressive write worker
	go func() {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))

		for {
			select {
			case <-ctx.Done():
				return
			default:
				// Large batches to fill MemTable quickly
				batchSize := 1000
				samples := make([]series.Sample, batchSize)
				baseTime := time.Now().UnixMilli()

				for i := 0; i < batchSize; i++ {
					samples[i] = series.Sample{
						Timestamp: baseTime + int64(i*1000),
						Value:     r.Float64() * 100,
					}
				}

				if err := db.Insert(s, samples); err == nil {
					totalWrites.Add(int64(batchSize))
				}
			}
		}
	}()

	<-ctx.Done()

	t.Logf("Memory pressure test completed:")
	t.Logf("  Total Writes: %d", totalWrites.Load())

	stats := db.GetStatsSnapshot()
	t.Logf("  Total Samples: %d", stats.TotalSamples)
	t.Logf("  Blocks Created: %d", stats.BlocksCount)
}
