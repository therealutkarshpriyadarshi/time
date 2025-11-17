// +build integration

package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/therealutkarshpriyadarshi/time/pkg/api"
	"github.com/therealutkarshpriyadarshi/time/pkg/client"
	"github.com/therealutkarshpriyadarshi/time/pkg/series"
	"github.com/therealutkarshpriyadarshi/time/pkg/storage"
)

// TestEndToEnd_WriteAndQuery tests complete write and query flow
func TestEndToEnd_WriteAndQuery(t *testing.T) {
	dir := t.TempDir()

	// Open TSDB
	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	// Create series
	s := series.NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
		"region":   "us-west",
	})

	// Insert samples over time
	now := time.Now().UnixMilli()
	samples := make([]series.Sample, 0, 1000)

	for i := 0; i < 1000; i++ {
		samples = append(samples, series.Sample{
			Timestamp: now + int64(i*60000), // 1 minute intervals
			Value:     float64(i%100) / 100.0,
		})
	}

	err = db.Insert(s, samples)
	if err != nil {
		t.Fatalf("failed to insert samples: %v", err)
	}

	// Query back the data
	queryStart := now
	queryEnd := now + int64(1000*60000)

	results, err := db.Query(s.Hash, queryStart, queryEnd)
	if err != nil {
		t.Fatalf("failed to query: %v", err)
	}

	if len(results) != 1000 {
		t.Errorf("expected 1000 samples, got %d", len(results))
	}

	// Verify sample values
	for i, sample := range results {
		expectedValue := float64(i%100) / 100.0
		if sample.Value != expectedValue {
			t.Errorf("sample %d: expected value %f, got %f", i, expectedValue, sample.Value)
		}
	}
}

// TestEndToEnd_MultipleSeries tests handling multiple series
func TestEndToEnd_MultipleSeries(t *testing.T) {
	dir := t.TempDir()

	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	numSeries := 100
	samplesPerSeries := 100

	// Write data for multiple series
	for i := 0; i < numSeries; i++ {
		s := series.NewSeries(map[string]string{
			"__name__": "metric",
			"id":       fmt.Sprintf("%d", i),
		})

		samples := make([]series.Sample, samplesPerSeries)
		now := time.Now().UnixMilli()

		for j := 0; j < samplesPerSeries; j++ {
			samples[j] = series.Sample{
				Timestamp: now + int64(j*1000),
				Value:     float64(i*samplesPerSeries + j),
			}
		}

		err = db.Insert(s, samples)
		if err != nil {
			t.Fatalf("failed to insert series %d: %v", i, err)
		}
	}

	// Query each series
	for i := 0; i < numSeries; i++ {
		s := series.NewSeries(map[string]string{
			"__name__": "metric",
			"id":       fmt.Sprintf("%d", i),
		})

		now := time.Now().UnixMilli()
		results, err := db.Query(s.Hash, 0, now+int64(samplesPerSeries*1000))
		if err != nil {
			t.Fatalf("failed to query series %d: %v", i, err)
		}

		if len(results) != samplesPerSeries {
			t.Errorf("series %d: expected %d samples, got %d", i, samplesPerSeries, len(results))
		}
	}
}

// TestEndToEnd_WALRecovery tests crash recovery via WAL
func TestEndToEnd_WALRecovery(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Write data
	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}

	s := series.NewSeries(map[string]string{
		"__name__": "recovery_test",
	})

	samples := make([]series.Sample, 1000)
	now := time.Now().UnixMilli()

	for i := 0; i < 1000; i++ {
		samples[i] = series.Sample{
			Timestamp: now + int64(i*1000),
			Value:     float64(i),
		}
	}

	err = db.Insert(s, samples)
	if err != nil {
		t.Fatalf("failed to insert samples: %v", err)
	}

	// Close without explicit flush (data in WAL)
	db.Close()

	// Phase 2: Reopen and verify data recovered
	db2, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to reopen TSDB: %v", err)
	}
	defer db2.Close()

	results, err := db2.Query(s.Hash, 0, now+int64(1000*1000))
	if err != nil {
		t.Fatalf("failed to query after recovery: %v", err)
	}

	if len(results) != 1000 {
		t.Errorf("expected 1000 samples after recovery, got %d", len(results))
	}
}

// TestEndToEnd_APIServer tests HTTP API integration
func TestEndToEnd_APIServer(t *testing.T) {
	dir := t.TempDir()

	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	// Start API server
	server := api.NewServer(db, ":0") // Port 0 = random port
	go func() {
		server.Start()
	}()
	defer server.Stop()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Get actual server address
	addr := server.Addr()
	if addr == "" {
		t.Fatal("server address is empty")
	}

	// Create client
	c := client.NewClient(fmt.Sprintf("http://%s", addr))

	// Write data via API
	metrics := []client.Metric{
		{
			Labels: map[string]string{
				"__name__": "api_test",
				"host":     "server1",
			},
			Timestamp: time.Now(),
			Value:     0.75,
		},
		{
			Labels: map[string]string{
				"__name__": "api_test",
				"host":     "server2",
			},
			Timestamp: time.Now(),
			Value:     0.85,
		},
	}

	err = c.Write(context.Background(), metrics)
	if err != nil {
		t.Fatalf("failed to write via API: %v", err)
	}

	// Query via API
	time.Sleep(100 * time.Millisecond) // Allow time for write to complete

	results, err := c.QueryRange(
		context.Background(),
		`{__name__="api_test"}`,
		time.Now().Add(-1*time.Minute),
		time.Now(),
		10*time.Second,
	)

	if err != nil {
		t.Fatalf("failed to query via API: %v", err)
	}

	if len(results) == 0 {
		t.Error("expected results from API query")
	}
}

// TestEndToEnd_ConcurrentWriteRead tests concurrent operations
func TestEndToEnd_ConcurrentWriteRead(t *testing.T) {
	dir := t.TempDir()

	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	s := series.NewSeries(map[string]string{
		"__name__": "concurrent_test",
	})

	// Initial data
	initialSamples := make([]series.Sample, 100)
	now := time.Now().UnixMilli()

	for i := 0; i < 100; i++ {
		initialSamples[i] = series.Sample{
			Timestamp: now + int64(i*1000),
			Value:     float64(i),
		}
	}

	err = db.Insert(s, initialSamples)
	if err != nil {
		t.Fatalf("failed to insert initial samples: %v", err)
	}

	// Start concurrent writers and readers
	done := make(chan bool)
	errChan := make(chan error, 10)

	// Writer goroutines
	for w := 0; w < 5; w++ {
		go func(workerID int) {
			for i := 0; i < 100; i++ {
				samples := []series.Sample{{
					Timestamp: time.Now().UnixMilli(),
					Value:     float64(workerID*1000 + i),
				}}

				if err := db.Insert(s, samples); err != nil {
					errChan <- fmt.Errorf("writer %d: %w", workerID, err)
					return
				}

				time.Sleep(time.Millisecond)
			}
			done <- true
		}(w)
	}

	// Reader goroutines
	for r := 0; r < 5; r++ {
		go func(workerID int) {
			for i := 0; i < 100; i++ {
				_, err := db.Query(s.Hash, 0, time.Now().UnixMilli())
				if err != nil {
					errChan <- fmt.Errorf("reader %d: %w", workerID, err)
					return
				}

				time.Sleep(time.Millisecond)
			}
			done <- true
		}(r)
	}

	// Wait for completion
	for i := 0; i < 10; i++ {
		select {
		case err := <-errChan:
			t.Fatalf("concurrent operation failed: %v", err)
		case <-done:
			// One goroutine finished
		case <-time.After(30 * time.Second):
			t.Fatal("test timeout")
		}
	}
}

// TestEndToEnd_LongRunning tests database over extended period
func TestEndToEnd_LongRunning(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long-running test in short mode")
	}

	dir := t.TempDir()

	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}
	defer db.Close()

	s := series.NewSeries(map[string]string{
		"__name__": "long_running_test",
	})

	// Write data continuously for 30 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sampleCount := 0
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Logf("Wrote %d samples over 30 seconds", sampleCount)
			return
		case <-ticker.C:
			samples := []series.Sample{{
				Timestamp: time.Now().UnixMilli(),
				Value:     float64(sampleCount),
			}}

			if err := db.Insert(s, samples); err != nil {
				t.Logf("Insert error (may be expected): %v", err)
			} else {
				sampleCount++
			}

			// Periodically query
			if sampleCount%100 == 0 {
				results, err := db.Query(s.Hash, 0, time.Now().UnixMilli())
				if err != nil {
					t.Fatalf("query failed: %v", err)
				}
				t.Logf("Query returned %d samples", len(results))
			}
		}
	}
}
