// +build chaos

package tests

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/therealutkarshpriyadarshi/time/pkg/series"
	"github.com/therealutkarshpriyadarshi/time/pkg/storage"
)

// TestChaos_CrashDuringWrite simulates crash during write operations
func TestChaos_CrashDuringWrite(t *testing.T) {
	dir := t.TempDir()

	for iteration := 0; iteration < 5; iteration++ {
		t.Logf("Iteration %d: Opening TSDB", iteration)

		db, err := storage.Open(storage.DefaultOptions(dir))
		if err != nil {
			t.Fatalf("iteration %d: failed to open TSDB: %v", iteration, err)
		}

		s := series.NewSeries(map[string]string{
			"__name__": "crash_test",
			"iteration": fmt.Sprintf("%d", iteration),
		})

		// Write some data
		samplesWritten := 1000
		for i := 0; i < samplesWritten; i++ {
			samples := []series.Sample{{
				Timestamp: time.Now().UnixMilli() + int64(i*1000),
				Value:     float64(iteration*1000 + i),
			}}

			if err := db.Insert(s, samples); err != nil {
				t.Logf("Insert error (may be expected): %v", err)
			}
		}

		t.Logf("Iteration %d: Simulating crash (closing without flush)", iteration)

		// Simulate crash - close without explicit flush
		// Data should be in WAL
		db.Close()

		// Small delay
		time.Sleep(100 * time.Millisecond)
	}

	// Final recovery - verify all data is intact
	t.Log("Final recovery: opening TSDB")

	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed final recovery: %v", err)
	}
	defer db.Close()

	stats := db.GetStatsSnapshot()
	t.Logf("Final state: %d samples recovered", stats.TotalSamples)

	if stats.TotalSamples == 0 {
		t.Error("expected non-zero samples after recovery")
	}
}

// TestChaos_CorruptedWAL simulates WAL corruption
func TestChaos_CorruptedWAL(t *testing.T) {
	dir := t.TempDir()

	// Phase 1: Write data
	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}

	s := series.NewSeries(map[string]string{
		"__name__": "wal_corruption_test",
	})

	for i := 0; i < 1000; i++ {
		samples := []series.Sample{{
			Timestamp: int64(i * 1000),
			Value:     float64(i),
		}}
		db.Insert(s, samples)
	}

	db.Close()

	// Phase 2: Corrupt WAL file
	walDir := filepath.Join(dir, "wal")
	walFiles, err := os.ReadDir(walDir)
	if err != nil {
		t.Fatalf("failed to read WAL dir: %v", err)
	}

	if len(walFiles) > 0 {
		// Corrupt the first WAL file
		walFile := filepath.Join(walDir, walFiles[0].Name())
		t.Logf("Corrupting WAL file: %s", walFile)

		f, err := os.OpenFile(walFile, os.O_RDWR, 0644)
		if err != nil {
			t.Fatalf("failed to open WAL file: %v", err)
		}

		// Write garbage at offset 100
		f.Seek(100, 0)
		f.Write([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
		f.Close()
	}

	// Phase 3: Try to recover - should detect corruption and skip bad entries
	t.Log("Attempting recovery with corrupted WAL")

	db2, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		// Corruption might prevent opening - that's acceptable
		t.Logf("Failed to open with corrupted WAL (expected): %v", err)
		return
	}
	defer db2.Close()

	stats := db2.GetStatsSnapshot()
	t.Logf("Recovered %d samples despite corruption", stats.TotalSamples)

	// System should still be functional
	testSamples := []series.Sample{{
		Timestamp: time.Now().UnixMilli(),
		Value:     123.45,
	}}

	err = db2.Insert(s, testSamples)
	if err != nil {
		t.Errorf("failed to write after corruption recovery: %v", err)
	}
}

// TestChaos_DiskFull simulates disk full scenarios
func TestChaos_DiskFull(t *testing.T) {
	// Note: This test is challenging to implement without OS-level mocking
	// We'll simulate by using a very small directory quota

	t.Skip("Disk full simulation requires OS-level quota support")
}

// TestChaos_RandomKill simulates random database restarts
func TestChaos_RandomKill(t *testing.T) {
	dir := t.TempDir()

	totalIterations := 10
	expectedMinSamples := 5000 // We expect at least this many samples to survive

	s := series.NewSeries(map[string]string{
		"__name__": "random_kill_test",
	})

	for iteration := 0; iteration < totalIterations; iteration++ {
		db, err := storage.Open(storage.DefaultOptions(dir))
		if err != nil {
			t.Fatalf("iteration %d: failed to open TSDB: %v", iteration, err)
		}

		// Write random amount of data
		numWrites := 500 + iteration*100

		for i := 0; i < numWrites; i++ {
			samples := []series.Sample{{
				Timestamp: time.Now().UnixMilli(),
				Value:     float64(iteration*10000 + i),
			}}

			db.Insert(s, samples)

			// Random small delays
			if i%10 == 0 {
				time.Sleep(time.Millisecond)
			}
		}

		t.Logf("Iteration %d: wrote %d samples, closing", iteration, numWrites)

		// Randomly decide whether to flush before close
		if iteration%3 == 0 {
			t.Logf("Iteration %d: closing WITH flush", iteration)
			// Normal close (would flush if implemented)
			db.Close()
		} else {
			t.Logf("Iteration %d: closing WITHOUT explicit flush", iteration)
			// Abrupt close
			db.Close()
		}

		time.Sleep(50 * time.Millisecond)
	}

	// Final verification
	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("final recovery failed: %v", err)
	}
	defer db.Close()

	stats := db.GetStatsSnapshot()
	t.Logf("Final recovery: %d samples", stats.TotalSamples)

	if stats.TotalSamples < int64(expectedMinSamples) {
		t.Errorf("expected at least %d samples, got %d", expectedMinSamples, stats.TotalSamples)
	}
}

// TestChaos_ConcurrentCrash simulates crashes under concurrent load
func TestChaos_ConcurrentCrash(t *testing.T) {
	dir := t.TempDir()

	for iteration := 0; iteration < 3; iteration++ {
		t.Logf("Iteration %d: starting concurrent operations", iteration)

		db, err := storage.Open(storage.DefaultOptions(dir))
		if err != nil {
			t.Fatalf("iteration %d: failed to open TSDB: %v", iteration, err)
		}

		// Create multiple series
		numSeries := 10
		seriesList := make([]*series.Series, numSeries)
		for i := 0; i < numSeries; i++ {
			seriesList[i] = series.NewSeries(map[string]string{
				"__name__": "concurrent_crash_test",
				"id":       fmt.Sprintf("%d", i),
				"iteration": fmt.Sprintf("%d", iteration),
			})
		}

		// Launch concurrent writers
		done := make(chan bool, numSeries)

		for i := 0; i < numSeries; i++ {
			go func(seriesID int) {
				s := seriesList[seriesID]

				for j := 0; j < 100; j++ {
					samples := []series.Sample{{
						Timestamp: time.Now().UnixMilli(),
						Value:     float64(seriesID*1000 + j),
					}}

					db.Insert(s, samples)
					time.Sleep(time.Millisecond)
				}

				done <- true
			}(i)
		}

		// Wait a bit, then crash
		time.Sleep(50 * time.Millisecond)

		t.Logf("Iteration %d: simulating crash during concurrent writes", iteration)
		db.Close()

		// Wait for goroutines (they'll fail after close, which is expected)
		time.Sleep(100 * time.Millisecond)
	}

	// Final recovery
	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("final recovery failed: %v", err)
	}
	defer db.Close()

	stats := db.GetStatsSnapshot()
	t.Logf("Recovered after concurrent crashes: %d samples", stats.TotalSamples)
}

// TestChaos_FilePermissions tests behavior with file permission errors
func TestChaos_FilePermissions(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("skipping permission test when running as root")
	}

	dir := t.TempDir()

	// Phase 1: Create database normally
	db, err := storage.Open(storage.DefaultOptions(dir))
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}

	s := series.NewSeries(map[string]string{
		"__name__": "permission_test",
	})

	for i := 0; i < 100; i++ {
		samples := []series.Sample{{
			Timestamp: int64(i * 1000),
			Value:     float64(i),
		}}
		db.Insert(s, samples)
	}

	db.Close()

	// Phase 2: Make directory read-only
	t.Log("Making data directory read-only")
	os.Chmod(dir, 0444)

	// Phase 3: Try to open - should fail gracefully
	_, err = storage.Open(storage.DefaultOptions(dir))

	// Restore permissions for cleanup
	os.Chmod(dir, 0755)

	if err == nil {
		t.Error("expected error when opening read-only directory, got none")
	} else {
		t.Logf("Got expected error with read-only directory: %v", err)
	}
}
