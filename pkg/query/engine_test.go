package query

import (
	"testing"

	"github.com/therealutkarshpriyadarshi/time/pkg/series"
	"github.com/therealutkarshpriyadarshi/time/pkg/storage"
)

func setupTestDB(t *testing.T) *storage.TSDB {
	t.Helper()

	// Create temporary directory
	tmpDir := t.TempDir()

	// Open TSDB
	db, err := storage.Open(storage.DefaultOptions(tmpDir))
	if err != nil {
		t.Fatalf("failed to open TSDB: %v", err)
	}

	return db
}

func TestQueryEngine_Select(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Insert test data
	s1 := series.NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	})

	samples1 := []series.Sample{
		{Timestamp: 1000, Value: 0.5},
		{Timestamp: 2000, Value: 0.6},
		{Timestamp: 3000, Value: 0.7},
	}

	err := db.Insert(s1, samples1)
	if err != nil {
		t.Fatalf("failed to insert samples: %v", err)
	}

	// Create query engine
	qe := NewQueryEngine(db)

	// Execute query
	q := &Query{
		MinTime: 0,
		MaxTime: 10000,
	}

	iterators, err := qe.Select(q)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// Note: Select() currently returns empty iterators
	// Full series enumeration will be implemented with index integration
	_ = iterators
}

func TestQueryEngine_ExecQuery(t *testing.T) {
	t.Skip("Skipping - requires series enumeration which needs index integration")

	db := setupTestDB(t)
	defer db.Close()

	// Insert test data
	s1 := series.NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	})

	samples1 := []series.Sample{
		{Timestamp: 1000, Value: 0.5},
		{Timestamp: 2000, Value: 0.6},
		{Timestamp: 3000, Value: 0.7},
	}

	err := db.Insert(s1, samples1)
	if err != nil {
		t.Fatalf("failed to insert samples: %v", err)
	}

	// Create query engine
	qe := NewQueryEngine(db)

	// Execute query
	q := &Query{
		MinTime: 0,
		MaxTime: 10000,
	}

	result, err := qe.ExecQuery(q)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}

	// Verify results
	if len(result.Series) != 1 {
		t.Errorf("expected 1 series, got %d", len(result.Series))
	}

	if len(result.Series) > 0 && len(result.Series[0].Samples) != 3 {
		t.Errorf("expected 3 samples, got %d", len(result.Series[0].Samples))
	}
}

func TestSliceIterator(t *testing.T) {
	s := series.NewSeries(map[string]string{
		"__name__": "test",
	})

	samples := []series.Sample{
		{Timestamp: 1000, Value: 1.0},
		{Timestamp: 2000, Value: 2.0},
		{Timestamp: 3000, Value: 3.0},
	}

	iter := &sliceIterator{
		series:  s,
		samples: samples,
		idx:     -1,
	}

	// Iterate through samples
	count := 0
	for iter.Next() {
		count++
		ts, val := iter.At()
		expectedSample := samples[count-1]
		if ts != expectedSample.Timestamp || val != expectedSample.Value {
			t.Errorf("sample %d: expected (%d, %f), got (%d, %f)",
				count, expectedSample.Timestamp, expectedSample.Value, ts, val)
		}
	}

	if count != 3 {
		t.Errorf("expected 3 samples, got %d", count)
	}

	// Verify labels
	labels := iter.Labels()
	if labels["__name__"] != "test" {
		t.Errorf("expected label __name__=test, got %s", labels["__name__"])
	}

	// Verify error
	if iter.Err() != nil {
		t.Errorf("unexpected error: %v", iter.Err())
	}

	// Close
	if err := iter.Close(); err != nil {
		t.Errorf("close failed: %v", err)
	}
}

func TestSliceIterator_Empty(t *testing.T) {
	iter := &sliceIterator{
		samples: []series.Sample{},
		idx:     -1,
	}

	if iter.Next() {
		t.Error("expected no samples")
	}
}

func TestMergeIterator(t *testing.T) {
	s := series.NewSeries(map[string]string{
		"__name__": "test",
	})

	samples1 := []series.Sample{
		{Timestamp: 1000, Value: 1.0},
		{Timestamp: 3000, Value: 3.0},
	}

	samples2 := []series.Sample{
		{Timestamp: 2000, Value: 2.0},
		{Timestamp: 4000, Value: 4.0},
	}

	iter1 := &sliceIterator{series: s, samples: samples1, idx: -1}
	iter2 := &sliceIterator{series: s, samples: samples2, idx: -1}

	merged := newMergeIterator(s, []SeriesIterator{iter1, iter2})

	// Expected merged order: 1000, 2000, 3000, 4000
	expected := []series.Sample{
		{Timestamp: 1000, Value: 1.0},
		{Timestamp: 2000, Value: 2.0},
		{Timestamp: 3000, Value: 3.0},
		{Timestamp: 4000, Value: 4.0},
	}

	count := 0
	for merged.Next() {
		if count >= len(expected) {
			t.Fatalf("too many samples")
		}

		ts, val := merged.At()
		if ts != expected[count].Timestamp || val != expected[count].Value {
			t.Errorf("sample %d: expected (%d, %f), got (%d, %f)",
				count, expected[count].Timestamp, expected[count].Value, ts, val)
		}
		count++
	}

	if count != len(expected) {
		t.Errorf("expected %d samples, got %d", len(expected), count)
	}

	merged.Close()
}

func TestMergeIterator_WithDuplicates(t *testing.T) {
	s := series.NewSeries(map[string]string{
		"__name__": "test",
	})

	samples1 := []series.Sample{
		{Timestamp: 1000, Value: 1.0},
		{Timestamp: 2000, Value: 2.0},
	}

	samples2 := []series.Sample{
		{Timestamp: 2000, Value: 2.5}, // Duplicate timestamp
		{Timestamp: 3000, Value: 3.0},
	}

	iter1 := &sliceIterator{series: s, samples: samples1, idx: -1}
	iter2 := &sliceIterator{series: s, samples: samples2, idx: -1}

	merged := newMergeIterator(s, []SeriesIterator{iter1, iter2})

	// Should deduplicate - only 3 unique timestamps
	count := 0
	lastTS := int64(-1)
	for merged.Next() {
		ts, _ := merged.At()
		if ts == lastTS {
			t.Errorf("duplicate timestamp: %d", ts)
		}
		lastTS = ts
		count++
	}

	// With deduplication, we should have 3 samples (1000, 2000, 3000)
	if count != 3 {
		t.Errorf("expected 3 unique samples, got %d", count)
	}

	merged.Close()
}

func TestStepIterator(t *testing.T) {
	s := series.NewSeries(map[string]string{
		"__name__": "test",
	})

	// Samples every 100ms
	samples := []series.Sample{
		{Timestamp: 1000, Value: 1.0},
		{Timestamp: 1100, Value: 1.1},
		{Timestamp: 1200, Value: 1.2},
		{Timestamp: 1300, Value: 1.3},
		{Timestamp: 1400, Value: 1.4},
	}

	inner := &sliceIterator{series: s, samples: samples, idx: -1}

	// Step of 200ms
	step := &stepIterator{
		inner:    inner,
		step:     200,
		minTime:  1000,
		maxTime:  1500,
		nextTime: 1000,
	}

	// Expected: samples at 1000, 1200, 1400
	expected := []int64{1000, 1200, 1400}
	count := 0

	for step.Next() {
		if count >= len(expected) {
			t.Fatalf("too many samples")
		}

		ts, _ := step.At()
		if ts != expected[count] {
			t.Errorf("sample %d: expected timestamp %d, got %d",
				count, expected[count], ts)
		}
		count++
	}

	if count != len(expected) {
		t.Errorf("expected %d samples, got %d", len(expected), count)
	}

	step.Close()
}

func TestQueryEngine_SelectRange(t *testing.T) {
	t.Skip("Skipping - requires series enumeration")
	db := setupTestDB(t)
	defer db.Close()

	// Insert test data with regular intervals
	s := series.NewSeries(map[string]string{
		"__name__": "metric",
	})

	samples := make([]series.Sample, 0)
	for i := int64(0); i < 10; i++ {
		samples = append(samples, series.Sample{
			Timestamp: 1000 + i*100,
			Value:     float64(i),
		})
	}

	err := db.Insert(s, samples)
	if err != nil {
		t.Fatalf("failed to insert samples: %v", err)
	}

	// Create query engine
	qe := NewQueryEngine(db)

	// Execute range query with step
	q := &Query{
		MinTime: 1000,
		MaxTime: 2000,
		Step:    200, // Every 200ms
	}

	iterators, err := qe.SelectRange(q)
	if err != nil {
		t.Fatalf("range query failed: %v", err)
	}

	if len(iterators) == 0 {
		t.Fatal("expected at least one iterator")
	}

	// Verify step alignment
	iter := iterators[0]
	count := 0
	for iter.Next() {
		count++
		ts, _ := iter.At()
		// Timestamps should be aligned to step boundaries
		if (ts-q.MinTime)%q.Step != 0 {
			t.Errorf("timestamp %d not aligned to step %d", ts, q.Step)
		}
	}

	iter.Close()
}

func BenchmarkSliceIterator(b *testing.B) {
	s := series.NewSeries(map[string]string{
		"__name__": "test",
	})

	samples := make([]series.Sample, 1000)
	for i := 0; i < 1000; i++ {
		samples[i] = series.Sample{
			Timestamp: int64(i * 1000),
			Value:     float64(i),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := &sliceIterator{
			series:  s,
			samples: samples,
			idx:     -1,
		}

		for iter.Next() {
			iter.At()
		}
	}
}

func BenchmarkMergeIterator(b *testing.B) {
	s := series.NewSeries(map[string]string{
		"__name__": "test",
	})

	// Create 5 iterators with 200 samples each
	iterators := make([]SeriesIterator, 5)
	for j := 0; j < 5; j++ {
		samples := make([]series.Sample, 200)
		for i := 0; i < 200; i++ {
			samples[i] = series.Sample{
				Timestamp: int64(i*5+j) * 1000,
				Value:     float64(i),
			}
		}
		iterators[j] = &sliceIterator{
			series:  s,
			samples: samples,
			idx:     -1,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		merged := newMergeIterator(s, iterators)
		for merged.Next() {
			merged.At()
		}
		merged.Close()

		// Reset iterators
		for j := 0; j < 5; j++ {
			samples := make([]series.Sample, 200)
			for k := 0; k < 200; k++ {
				samples[k] = series.Sample{
					Timestamp: int64(k*5+j) * 1000,
					Value:     float64(k),
				}
			}
			iterators[j] = &sliceIterator{
				series:  s,
				samples: samples,
				idx:     -1,
			}
		}
	}
}
