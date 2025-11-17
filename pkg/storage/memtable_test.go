package storage

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

func TestNewMemTable(t *testing.T) {
	mt := NewMemTable()

	if mt == nil {
		t.Fatal("NewMemTable returned nil")
	}

	if mt.Size() != 0 {
		t.Errorf("New MemTable should have size 0, got %d", mt.Size())
	}

	if mt.MaxSize() != DefaultMaxSize {
		t.Errorf("Expected max size %d, got %d", DefaultMaxSize, mt.MaxSize())
	}

	if mt.SeriesCount() != 0 {
		t.Errorf("New MemTable should have 0 series, got %d", mt.SeriesCount())
	}
}

func TestNewMemTableWithSize(t *testing.T) {
	customSize := int64(1024 * 1024) // 1MB
	mt := NewMemTableWithSize(customSize)

	if mt.MaxSize() != customSize {
		t.Errorf("Expected max size %d, got %d", customSize, mt.MaxSize())
	}
}

func TestMemTableInsert(t *testing.T) {
	mt := NewMemTable()

	s := series.NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	})

	samples := []series.Sample{
		{Timestamp: 1000, Value: 0.5},
		{Timestamp: 2000, Value: 0.6},
		{Timestamp: 3000, Value: 0.7},
	}

	err := mt.Insert(s, samples)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	if mt.SeriesCount() != 1 {
		t.Errorf("Expected 1 series, got %d", mt.SeriesCount())
	}

	if mt.SampleCount() != 3 {
		t.Errorf("Expected 3 samples, got %d", mt.SampleCount())
	}

	if mt.Size() == 0 {
		t.Error("Size should be greater than 0 after insert")
	}
}

func TestMemTableInsert_InvalidInput(t *testing.T) {
	mt := NewMemTable()

	// Nil series
	err := mt.Insert(nil, []series.Sample{{Timestamp: 1000, Value: 0.5}})
	if err != ErrInvalidSample {
		t.Errorf("Expected ErrInvalidSample for nil series, got %v", err)
	}

	// Empty samples
	s := series.NewSeries(map[string]string{"host": "server1"})
	err = mt.Insert(s, []series.Sample{})
	if err != ErrInvalidSample {
		t.Errorf("Expected ErrInvalidSample for empty samples, got %v", err)
	}

	// Nil samples
	err = mt.Insert(s, nil)
	if err != ErrInvalidSample {
		t.Errorf("Expected ErrInvalidSample for nil samples, got %v", err)
	}
}

func TestMemTableInsert_MultipleSeries(t *testing.T) {
	mt := NewMemTable()

	s1 := series.NewSeries(map[string]string{"host": "server1"})
	s2 := series.NewSeries(map[string]string{"host": "server2"})

	samples1 := []series.Sample{{Timestamp: 1000, Value: 0.5}}
	samples2 := []series.Sample{{Timestamp: 1000, Value: 0.8}}

	mt.Insert(s1, samples1)
	mt.Insert(s2, samples2)

	if mt.SeriesCount() != 2 {
		t.Errorf("Expected 2 series, got %d", mt.SeriesCount())
	}

	if mt.SampleCount() != 2 {
		t.Errorf("Expected 2 samples, got %d", mt.SampleCount())
	}
}

func TestMemTableInsert_SameSeriesMultipleTimes(t *testing.T) {
	mt := NewMemTable()

	s := series.NewSeries(map[string]string{"host": "server1"})

	samples1 := []series.Sample{{Timestamp: 1000, Value: 0.5}}
	samples2 := []series.Sample{{Timestamp: 2000, Value: 0.6}}

	mt.Insert(s, samples1)
	mt.Insert(s, samples2)

	if mt.SeriesCount() != 1 {
		t.Errorf("Expected 1 series, got %d", mt.SeriesCount())
	}

	if mt.SampleCount() != 2 {
		t.Errorf("Expected 2 samples, got %d", mt.SampleCount())
	}
}

func TestMemTableInsert_Full(t *testing.T) {
	// Create small MemTable
	mt := NewMemTableWithSize(100)

	s := series.NewSeries(map[string]string{"host": "server1"})

	// Try to insert many samples
	samples := make([]series.Sample, 100)
	for i := range samples {
		samples[i] = series.Sample{Timestamp: int64(i * 1000), Value: float64(i)}
	}

	err := mt.Insert(s, samples)
	if err != ErrMemTableFull {
		t.Errorf("Expected ErrMemTableFull, got %v", err)
	}
}

func TestMemTableQuery(t *testing.T) {
	mt := NewMemTable()

	s := series.NewSeries(map[string]string{"host": "server1"})
	samples := []series.Sample{
		{Timestamp: 1000, Value: 0.5},
		{Timestamp: 2000, Value: 0.6},
		{Timestamp: 3000, Value: 0.7},
	}

	mt.Insert(s, samples)

	// Query all
	result, err := mt.Query(s.Hash, 0, 0)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(result) != 3 {
		t.Errorf("Expected 3 samples, got %d", len(result))
	}
}

func TestMemTableQuery_TimeRange(t *testing.T) {
	mt := NewMemTable()

	s := series.NewSeries(map[string]string{"host": "server1"})
	samples := []series.Sample{
		{Timestamp: 1000, Value: 0.5},
		{Timestamp: 2000, Value: 0.6},
		{Timestamp: 3000, Value: 0.7},
		{Timestamp: 4000, Value: 0.8},
		{Timestamp: 5000, Value: 0.9},
	}

	mt.Insert(s, samples)

	// Query time range [2000, 4000]
	result, err := mt.Query(s.Hash, 2000, 4000)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(result) != 3 {
		t.Errorf("Expected 3 samples in range, got %d", len(result))
	}

	// Verify timestamps
	if result[0].Timestamp != 2000 || result[1].Timestamp != 3000 || result[2].Timestamp != 4000 {
		t.Error("Query returned wrong timestamps")
	}
}

func TestMemTableQuery_NonExistent(t *testing.T) {
	mt := NewMemTable()

	result, err := mt.Query(12345, 0, 0)
	if err != nil {
		t.Errorf("Query for non-existent series should not error: %v", err)
	}

	if result != nil {
		t.Errorf("Query for non-existent series should return nil, got %d samples", len(result))
	}
}

func TestMemTableGetSeries(t *testing.T) {
	mt := NewMemTable()

	original := series.NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	})

	samples := []series.Sample{{Timestamp: 1000, Value: 0.5}}
	mt.Insert(original, samples)

	retrieved, exists := mt.GetSeries(original.Hash)
	if !exists {
		t.Error("Series should exist")
	}

	if !retrieved.Equals(original) {
		t.Error("Retrieved series should equal original")
	}

	// Verify it's a copy
	retrieved.Labels["region"] = "us-west"
	retrieved2, _ := mt.GetSeries(original.Hash)
	if _, hasRegion := retrieved2.Labels["region"]; hasRegion {
		t.Error("Modifying returned series should not affect stored series")
	}
}

func TestMemTableTimeRange(t *testing.T) {
	mt := NewMemTable()

	s := series.NewSeries(map[string]string{"host": "server1"})
	samples := []series.Sample{
		{Timestamp: 1000, Value: 0.5},
		{Timestamp: 5000, Value: 0.9},
		{Timestamp: 3000, Value: 0.7},
	}

	mt.Insert(s, samples)

	minTime, maxTime := mt.TimeRange()
	if minTime != 1000 {
		t.Errorf("Expected minTime 1000, got %d", minTime)
	}
	if maxTime != 5000 {
		t.Errorf("Expected maxTime 5000, got %d", maxTime)
	}
}

func TestMemTableIsFull(t *testing.T) {
	mt := NewMemTableWithSize(100)

	if mt.IsFull() {
		t.Error("Empty MemTable should not be full")
	}

	s := series.NewSeries(map[string]string{"host": "server1"})
	samples := make([]series.Sample, 10)
	for i := range samples {
		samples[i] = series.Sample{Timestamp: int64(i * 1000), Value: float64(i)}
	}

	// This should fail because it would exceed size
	mt.Insert(s, samples)

	// The insert failed, so it shouldn't be marked as full yet
	// Let's insert smaller amounts until it's full
	mt2 := NewMemTableWithSize(100)
	sample := []series.Sample{{Timestamp: 1000, Value: 0.5}}
	for !mt2.IsFull() {
		err := mt2.Insert(s, sample)
		if err == ErrMemTableFull {
			break
		}
	}
}

func TestMemTableClear(t *testing.T) {
	mt := NewMemTable()

	s := series.NewSeries(map[string]string{"host": "server1"})
	samples := []series.Sample{{Timestamp: 1000, Value: 0.5}}
	mt.Insert(s, samples)

	if mt.SeriesCount() == 0 {
		t.Error("MemTable should have data before clear")
	}

	mt.Clear()

	if mt.SeriesCount() != 0 {
		t.Errorf("MemTable should be empty after clear, got %d series", mt.SeriesCount())
	}

	if mt.Size() != 0 {
		t.Errorf("MemTable size should be 0 after clear, got %d", mt.Size())
	}

	minTime, maxTime := mt.TimeRange()
	if minTime != -1 || maxTime != -1 {
		t.Error("Time range should be reset after clear")
	}
}

func TestMemTableAllSeries(t *testing.T) {
	mt := NewMemTable()

	s1 := series.NewSeries(map[string]string{"host": "server1"})
	s2 := series.NewSeries(map[string]string{"host": "server2"})

	samples := []series.Sample{{Timestamp: 1000, Value: 0.5}}

	mt.Insert(s1, samples)
	mt.Insert(s2, samples)

	allSeries := mt.AllSeries()
	if len(allSeries) != 2 {
		t.Errorf("Expected 2 series hashes, got %d", len(allSeries))
	}

	// Verify both hashes are present
	hashMap := make(map[uint64]bool)
	for _, hash := range allSeries {
		hashMap[hash] = true
	}

	if !hashMap[s1.Hash] || !hashMap[s2.Hash] {
		t.Error("AllSeries should return all series hashes")
	}
}

func TestMemTableConcurrency(t *testing.T) {
	mt := NewMemTable()

	// Launch multiple goroutines that insert concurrently
	var wg sync.WaitGroup
	numGoroutines := 10
	samplesPerGoroutine := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			s := series.NewSeries(map[string]string{
				"host": fmt.Sprintf("server%d", id),
			})

			for j := 0; j < samplesPerGoroutine; j++ {
				samples := []series.Sample{{
					Timestamp: int64(j * 1000),
					Value:     float64(j),
				}}
				mt.Insert(s, samples)
			}
		}(i)
	}

	wg.Wait()

	// Verify counts
	expectedSeries := numGoroutines
	expectedSamples := int64(numGoroutines * samplesPerGoroutine)

	if mt.SeriesCount() != expectedSeries {
		t.Errorf("Expected %d series, got %d", expectedSeries, mt.SeriesCount())
	}

	if mt.SampleCount() != expectedSamples {
		t.Errorf("Expected %d samples, got %d", expectedSamples, mt.SampleCount())
	}
}

func TestMemTableConcurrentReadWrite(t *testing.T) {
	mt := NewMemTable()

	s := series.NewSeries(map[string]string{"host": "server1"})

	// Writer goroutine
	go func() {
		for i := 0; i < 100; i++ {
			samples := []series.Sample{{
				Timestamp: int64(i * 1000),
				Value:     float64(i),
			}}
			mt.Insert(s, samples)
			time.Sleep(time.Microsecond)
		}
	}()

	// Reader goroutines
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				mt.Query(s.Hash, 0, 0)
				mt.SeriesCount()
				mt.Size()
				time.Sleep(time.Microsecond)
			}
		}()
	}

	wg.Wait()
}

func TestMemTableStats(t *testing.T) {
	mt := NewMemTable()

	s := series.NewSeries(map[string]string{"host": "server1"})
	samples := []series.Sample{{Timestamp: 1000, Value: 0.5}}
	mt.Insert(s, samples)

	stats := mt.Stats()
	if stats == "" {
		t.Error("Stats should not be empty")
	}

	// Just verify it doesn't panic and returns something reasonable
	if len(stats) < 10 {
		t.Error("Stats string seems too short")
	}
}
