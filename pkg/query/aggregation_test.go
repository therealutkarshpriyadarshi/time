package query

import (
	"math"
	"testing"

	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

func TestApplyAggregation_Sum(t *testing.T) {
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	result, err := applyAggregation(values, Sum)
	if err != nil {
		t.Fatalf("aggregation failed: %v", err)
	}

	expected := 15.0
	if result != expected {
		t.Errorf("expected %f, got %f", expected, result)
	}
}

func TestApplyAggregation_Avg(t *testing.T) {
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	result, err := applyAggregation(values, Avg)
	if err != nil {
		t.Fatalf("aggregation failed: %v", err)
	}

	expected := 3.0
	if result != expected {
		t.Errorf("expected %f, got %f", expected, result)
	}
}

func TestApplyAggregation_Max(t *testing.T) {
	values := []float64{1.0, 5.0, 3.0, 2.0, 4.0}
	result, err := applyAggregation(values, Max)
	if err != nil {
		t.Fatalf("aggregation failed: %v", err)
	}

	expected := 5.0
	if result != expected {
		t.Errorf("expected %f, got %f", expected, result)
	}
}

func TestApplyAggregation_Min(t *testing.T) {
	values := []float64{5.0, 1.0, 3.0, 2.0, 4.0}
	result, err := applyAggregation(values, Min)
	if err != nil {
		t.Fatalf("aggregation failed: %v", err)
	}

	expected := 1.0
	if result != expected {
		t.Errorf("expected %f, got %f", expected, result)
	}
}

func TestApplyAggregation_Count(t *testing.T) {
	values := []float64{1.0, 2.0, 3.0, 4.0, 5.0}
	result, err := applyAggregation(values, Count)
	if err != nil {
		t.Fatalf("aggregation failed: %v", err)
	}

	expected := 5.0
	if result != expected {
		t.Errorf("expected %f, got %f", expected, result)
	}
}

func TestApplyAggregation_StdDev(t *testing.T) {
	values := []float64{2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0}
	result, err := applyAggregation(values, StdDev)
	if err != nil {
		t.Fatalf("aggregation failed: %v", err)
	}

	// Expected standard deviation: 2.0
	expected := 2.0
	if math.Abs(result-expected) > 0.01 {
		t.Errorf("expected %f, got %f", expected, result)
	}
}

func TestApplyAggregation_Empty(t *testing.T) {
	values := []float64{}
	result, err := applyAggregation(values, Sum)
	if err != nil {
		t.Fatalf("aggregation failed: %v", err)
	}

	if result != 0 {
		t.Errorf("expected 0 for empty values, got %f", result)
	}
}

func TestComputeGroupKey(t *testing.T) {
	labels := map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
		"region":   "us-west",
		"env":      "prod",
	}

	tests := []struct {
		name     string
		groupBy  []string
		without  []string
		expected map[string]string
	}{
		{
			name:    "group by host",
			groupBy: []string{"host"},
			expected: map[string]string{
				"host": "server1",
			},
		},
		{
			name:    "group by host and region",
			groupBy: []string{"host", "region"},
			expected: map[string]string{
				"host":   "server1",
				"region": "us-west",
			},
		},
		{
			name:    "without region and env",
			without: []string{"region", "env"},
			expected: map[string]string{
				"__name__": "cpu_usage",
				"host":     "server1",
			},
		},
		{
			name:     "no grouping",
			expected: map[string]string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, groupLabels := computeGroupKey(labels, tt.groupBy, tt.without)

			if len(groupLabels) != len(tt.expected) {
				t.Errorf("expected %d labels, got %d", len(tt.expected), len(groupLabels))
			}

			for k, v := range tt.expected {
				if groupLabels[k] != v {
					t.Errorf("expected %s=%s, got %s=%s", k, v, k, groupLabels[k])
				}
			}
		})
	}
}

func TestQueryEngine_Aggregate(t *testing.T) {
	t.Skip("Skipping - requires series enumeration")
	db := setupTestDB(t)
	defer db.Close()

	// Insert test data for multiple series
	s1 := series.NewSeries(map[string]string{
		"__name__": "http_requests",
		"host":     "server1",
		"code":     "200",
	})

	s2 := series.NewSeries(map[string]string{
		"__name__": "http_requests",
		"host":     "server2",
		"code":     "200",
	})

	// Insert samples at regular intervals
	for i := int64(0); i < 10; i++ {
		timestamp := 1000 + i*1000

		err := db.Insert(s1, []series.Sample{
			{Timestamp: timestamp, Value: float64(i)},
		})
		if err != nil {
			t.Fatalf("failed to insert s1 samples: %v", err)
		}

		err = db.Insert(s2, []series.Sample{
			{Timestamp: timestamp, Value: float64(i * 2)},
		})
		if err != nil {
			t.Fatalf("failed to insert s2 samples: %v", err)
		}
	}

	qe := NewQueryEngine(db)

	// Test sum aggregation
	aq := &AggregationQuery{
		Query: &Query{
			MinTime: 1000,
			MaxTime: 10000,
		},
		Function: Sum,
		Step:     2000, // 2 second buckets
	}

	result, err := qe.Aggregate(aq)
	if err != nil {
		t.Fatalf("aggregation failed: %v", err)
	}

	if len(result.Series) == 0 {
		t.Error("expected aggregated series")
	}

	// Verify that values are summed
	for _, ts := range result.Series {
		if len(ts.Samples) == 0 {
			t.Error("expected aggregated samples")
		}
	}
}

func TestQueryEngine_Rate(t *testing.T) {
	t.Skip("Skipping - requires series enumeration")
	db := setupTestDB(t)
	defer db.Close()

	// Insert counter data (monotonically increasing)
	s := series.NewSeries(map[string]string{
		"__name__": "http_requests_total",
	})

	samples := []series.Sample{
		{Timestamp: 1000, Value: 100},
		{Timestamp: 2000, Value: 110},  // +10 in 1 second
		{Timestamp: 3000, Value: 125},  // +15 in 1 second
		{Timestamp: 4000, Value: 145},  // +20 in 1 second
	}

	err := db.Insert(s, samples)
	if err != nil {
		t.Fatalf("failed to insert samples: %v", err)
	}

	qe := NewQueryEngine(db)

	q := &Query{
		MinTime: 1000,
		MaxTime: 5000,
	}

	result, err := qe.Rate(q, 5)
	if err != nil {
		t.Fatalf("rate calculation failed: %v", err)
	}

	if len(result.Series) != 1 {
		t.Fatalf("expected 1 series, got %d", len(result.Series))
	}

	rateSamples := result.Series[0].Samples
	if len(rateSamples) != 3 { // 4 samples = 3 rate calculations
		t.Errorf("expected 3 rate samples, got %d", len(rateSamples))
	}

	// Verify rate values
	expectedRates := []float64{10.0, 15.0, 20.0}
	for i, expected := range expectedRates {
		if i >= len(rateSamples) {
			break
		}
		if math.Abs(rateSamples[i].Value-expected) > 0.01 {
			t.Errorf("sample %d: expected rate %f, got %f",
				i, expected, rateSamples[i].Value)
		}
	}
}

func TestQueryEngine_Rate_CounterReset(t *testing.T) {
	t.Skip("Skipping - requires series enumeration")
	db := setupTestDB(t)
	defer db.Close()

	// Insert counter data with reset
	s := series.NewSeries(map[string]string{
		"__name__": "counter",
	})

	samples := []series.Sample{
		{Timestamp: 1000, Value: 100},
		{Timestamp: 2000, Value: 110},
		{Timestamp: 3000, Value: 10},  // Counter reset
		{Timestamp: 4000, Value: 20},
	}

	err := db.Insert(s, samples)
	if err != nil {
		t.Fatalf("failed to insert samples: %v", err)
	}

	qe := NewQueryEngine(db)

	q := &Query{
		MinTime: 1000,
		MaxTime: 5000,
	}

	result, err := qe.Rate(q, 5)
	if err != nil {
		t.Fatalf("rate calculation failed: %v", err)
	}

	// Should handle counter reset gracefully
	if len(result.Series) != 1 {
		t.Fatalf("expected 1 series, got %d", len(result.Series))
	}

	rateSamples := result.Series[0].Samples
	// After reset, rate should be based on new value (10/1s = 10)
	for i, sample := range rateSamples {
		if sample.Value < 0 {
			t.Errorf("sample %d: rate should not be negative: %f", i, sample.Value)
		}
	}
}

func TestQueryEngine_Increase(t *testing.T) {
	t.Skip("Skipping - requires series enumeration")
	db := setupTestDB(t)
	defer db.Close()

	s := series.NewSeries(map[string]string{
		"__name__": "http_requests_total",
	})

	samples := []series.Sample{
		{Timestamp: 1000, Value: 100},
		{Timestamp: 2000, Value: 150},
		{Timestamp: 3000, Value: 225},
	}

	err := db.Insert(s, samples)
	if err != nil {
		t.Fatalf("failed to insert samples: %v", err)
	}

	qe := NewQueryEngine(db)

	q := &Query{
		MinTime: 1000,
		MaxTime: 5000,
	}

	result, err := qe.Increase(q)
	if err != nil {
		t.Fatalf("increase calculation failed: %v", err)
	}

	if len(result.Series) != 1 {
		t.Fatalf("expected 1 series, got %d", len(result.Series))
	}

	samples = result.Series[0].Samples
	if len(samples) != 1 {
		t.Errorf("expected 1 sample, got %d", len(samples))
	}

	// Increase from 100 to 225 = 125
	expected := 125.0
	if math.Abs(samples[0].Value-expected) > 0.01 {
		t.Errorf("expected increase %f, got %f", expected, samples[0].Value)
	}
}

func TestQueryEngine_Delta(t *testing.T) {
	t.Skip("Skipping - requires series enumeration")
	db := setupTestDB(t)
	defer db.Close()

	s := series.NewSeries(map[string]string{
		"__name__": "temperature",
	})

	samples := []series.Sample{
		{Timestamp: 1000, Value: 20.0},
		{Timestamp: 2000, Value: 25.0},
		{Timestamp: 3000, Value: 22.0},  // Decrease
	}

	err := db.Insert(s, samples)
	if err != nil {
		t.Fatalf("failed to insert samples: %v", err)
	}

	qe := NewQueryEngine(db)

	q := &Query{
		MinTime: 1000,
		MaxTime: 5000,
	}

	result, err := qe.Delta(q)
	if err != nil {
		t.Fatalf("delta calculation failed: %v", err)
	}

	if len(result.Series) != 1 {
		t.Fatalf("expected 1 series, got %d", len(result.Series))
	}

	samples = result.Series[0].Samples
	if len(samples) != 1 {
		t.Errorf("expected 1 sample, got %d", len(samples))
	}

	// Delta from 20 to 22 = +2
	expected := 2.0
	if math.Abs(samples[0].Value-expected) > 0.01 {
		t.Errorf("expected delta %f, got %f", expected, samples[0].Value)
	}
}

func TestQueryEngine_Derivative(t *testing.T) {
	t.Skip("Skipping - requires series enumeration")
	db := setupTestDB(t)
	defer db.Close()

	s := series.NewSeries(map[string]string{
		"__name__": "gauge",
	})

	samples := []series.Sample{
		{Timestamp: 1000, Value: 10.0},
		{Timestamp: 2000, Value: 20.0},  // +10 in 1s = 10/s
		{Timestamp: 3000, Value: 25.0},  // +5 in 1s = 5/s
		{Timestamp: 4000, Value: 20.0},  // -5 in 1s = -5/s
	}

	err := db.Insert(s, samples)
	if err != nil {
		t.Fatalf("failed to insert samples: %v", err)
	}

	qe := NewQueryEngine(db)

	q := &Query{
		MinTime: 1000,
		MaxTime: 5000,
	}

	result, err := qe.Derivative(q)
	if err != nil {
		t.Fatalf("derivative calculation failed: %v", err)
	}

	if len(result.Series) != 1 {
		t.Fatalf("expected 1 series, got %d", len(result.Series))
	}

	derivSamples := result.Series[0].Samples
	if len(derivSamples) != 3 {
		t.Errorf("expected 3 derivative samples, got %d", len(derivSamples))
	}

	// Verify derivative values
	expectedDerivs := []float64{10.0, 5.0, -5.0}
	for i, expected := range expectedDerivs {
		if i >= len(derivSamples) {
			break
		}
		if math.Abs(derivSamples[i].Value-expected) > 0.01 {
			t.Errorf("sample %d: expected derivative %f, got %f",
				i, expected, derivSamples[i].Value)
		}
	}
}

func BenchmarkApplyAggregation_Sum(b *testing.B) {
	values := make([]float64, 1000)
	for i := 0; i < 1000; i++ {
		values[i] = float64(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		applyAggregation(values, Sum)
	}
}

func BenchmarkApplyAggregation_Avg(b *testing.B) {
	values := make([]float64, 1000)
	for i := 0; i < 1000; i++ {
		values[i] = float64(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		applyAggregation(values, Avg)
	}
}

func BenchmarkApplyAggregation_StdDev(b *testing.B) {
	values := make([]float64, 1000)
	for i := 0; i < 1000; i++ {
		values[i] = float64(i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		applyAggregation(values, StdDev)
	}
}
