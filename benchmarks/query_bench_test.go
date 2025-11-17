package benchmarks

import (
	"testing"

	"github.com/therealutkarshpriyadarshi/time/pkg/index"
	"github.com/therealutkarshpriyadarshi/time/pkg/query"
	"github.com/therealutkarshpriyadarshi/time/pkg/series"
	"github.com/therealutkarshpriyadarshi/time/pkg/storage"
)

func BenchmarkQueryEngine_Select_1Series(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()

	// Insert 1 series with 1000 samples
	s := series.NewSeries(map[string]string{
		"__name__": "metric",
		"host":     "server1",
	})

	samples := make([]series.Sample, 1000)
	for i := 0; i < 1000; i++ {
		samples[i] = series.Sample{
			Timestamp: int64(i * 1000),
			Value:     float64(i),
		}
	}

	if err := db.Insert(s, samples); err != nil {
		b.Fatalf("failed to insert: %v", err)
	}

	qe := query.NewQueryEngine(db)

	q := &query.Query{
		MinTime: 0,
		MaxTime: 1000000,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := qe.ExecQuery(q)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
		if len(result.Series) == 0 {
			b.Fatal("expected results")
		}
	}
}

func BenchmarkQueryEngine_Select_100Series(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()

	// Insert 100 series with 100 samples each
	for seriesIdx := 0; seriesIdx < 100; seriesIdx++ {
		s := series.NewSeries(map[string]string{
			"__name__": "metric",
			"host":     benchFormatInt("server", seriesIdx),
		})

		samples := make([]series.Sample, 100)
		for i := 0; i < 100; i++ {
			samples[i] = series.Sample{
				Timestamp: int64(i * 1000),
				Value:     float64(i),
			}
		}

		if err := db.Insert(s, samples); err != nil {
			b.Fatalf("failed to insert: %v", err)
		}
	}

	qe := query.NewQueryEngine(db)

	q := &query.Query{
		MinTime: 0,
		MaxTime: 1000000,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := qe.ExecQuery(q)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
		if len(result.Series) == 0 {
			b.Fatal("expected results")
		}
	}
}

func BenchmarkQueryEngine_Aggregate_Sum(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()

	// Insert 10 series with 100 samples each
	for seriesIdx := 0; seriesIdx < 10; seriesIdx++ {
		s := series.NewSeries(map[string]string{
			"__name__": "http_requests",
			"host":     benchFormatInt("server", seriesIdx),
		})

		samples := make([]series.Sample, 100)
		for i := 0; i < 100; i++ {
			samples[i] = series.Sample{
				Timestamp: int64(i * 1000),
				Value:     float64(i),
			}
		}

		if err := db.Insert(s, samples); err != nil {
			b.Fatalf("failed to insert: %v", err)
		}
	}

	qe := query.NewQueryEngine(db)

	aq := &query.AggregationQuery{
		Query: &query.Query{
			MinTime: 0,
			MaxTime: 100000,
		},
		Function: query.Sum,
		Step:     5000, // 5 second buckets
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := qe.Aggregate(aq)
		if err != nil {
			b.Fatalf("aggregation failed: %v", err)
		}
		if len(result.Series) == 0 {
			b.Fatal("expected aggregated results")
		}
	}
}

func BenchmarkQueryEngine_Aggregate_Avg(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()

	// Insert 10 series with 100 samples each
	for seriesIdx := 0; seriesIdx < 10; seriesIdx++ {
		s := series.NewSeries(map[string]string{
			"__name__": "cpu_usage",
			"host":     benchFormatInt("server", seriesIdx),
		})

		samples := make([]series.Sample, 100)
		for i := 0; i < 100; i++ {
			samples[i] = series.Sample{
				Timestamp: int64(i * 1000),
				Value:     float64(i % 10),
			}
		}

		if err := db.Insert(s, samples); err != nil {
			b.Fatalf("failed to insert: %v", err)
		}
	}

	qe := query.NewQueryEngine(db)

	aq := &query.AggregationQuery{
		Query: &query.Query{
			MinTime: 0,
			MaxTime: 100000,
		},
		Function: query.Avg,
		Step:     5000,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := qe.Aggregate(aq)
		if err != nil {
			b.Fatalf("aggregation failed: %v", err)
		}
		if len(result.Series) == 0 {
			b.Fatal("expected aggregated results")
		}
	}
}

func BenchmarkQueryEngine_Rate(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()

	// Insert counter data
	s := series.NewSeries(map[string]string{
		"__name__": "http_requests_total",
	})

	samples := make([]series.Sample, 1000)
	value := 0.0
	for i := 0; i < 1000; i++ {
		value += float64(i % 10) // Simulate counter increments
		samples[i] = series.Sample{
			Timestamp: int64(i * 1000),
			Value:     value,
		}
	}

	if err := db.Insert(s, samples); err != nil {
		b.Fatalf("failed to insert: %v", err)
	}

	qe := query.NewQueryEngine(db)

	q := &query.Query{
		MinTime: 0,
		MaxTime: 1000000,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := qe.Rate(q, 60)
		if err != nil {
			b.Fatalf("rate calculation failed: %v", err)
		}
		if len(result.Series) == 0 {
			b.Fatal("expected rate results")
		}
	}
}

func BenchmarkQueryEngine_Increase(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()

	// Insert counter data
	s := series.NewSeries(map[string]string{
		"__name__": "requests_total",
	})

	samples := make([]series.Sample, 1000)
	for i := 0; i < 1000; i++ {
		samples[i] = series.Sample{
			Timestamp: int64(i * 1000),
			Value:     float64(i * 10),
		}
	}

	if err := db.Insert(s, samples); err != nil {
		b.Fatalf("failed to insert: %v", err)
	}

	qe := query.NewQueryEngine(db)

	q := &query.Query{
		MinTime: 0,
		MaxTime: 1000000,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := qe.Increase(q)
		if err != nil {
			b.Fatalf("increase calculation failed: %v", err)
		}
		if len(result.Series) == 0 {
			b.Fatal("expected increase results")
		}
	}
}

func BenchmarkQueryEngine_SelectRange(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()

	// Insert data with fine granularity
	s := series.NewSeries(map[string]string{
		"__name__": "metric",
	})

	samples := make([]series.Sample, 10000)
	for i := 0; i < 10000; i++ {
		samples[i] = series.Sample{
			Timestamp: int64(i * 100), // Every 100ms
			Value:     float64(i),
		}
	}

	if err := db.Insert(s, samples); err != nil {
		b.Fatalf("failed to insert: %v", err)
	}

	qe := query.NewQueryEngine(db)

	q := &query.Query{
		MinTime: 0,
		MaxTime: 1000000,
		Step:    5000, // 5 second steps
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iterators, err := qe.SelectRange(q)
		if err != nil {
			b.Fatalf("range query failed: %v", err)
		}

		// Consume iterators
		for _, iter := range iterators {
			for iter.Next() {
				iter.At()
			}
			iter.Close()
		}
	}
}

func BenchmarkQueryEngine_Aggregate_GroupBy(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()

	// Insert data with multiple dimensions
	for region := 0; region < 5; region++ {
		for host := 0; host < 10; host++ {
			s := series.NewSeries(map[string]string{
				"__name__": "cpu_usage",
				"region":   benchFormatInt("region", region),
				"host":     benchFormatInt("server", host),
			})

			samples := make([]series.Sample, 50)
			for i := 0; i < 50; i++ {
				samples[i] = series.Sample{
					Timestamp: int64(i * 1000),
					Value:     float64(i % 100),
				}
			}

			if err := db.Insert(s, samples); err != nil {
				b.Fatalf("failed to insert: %v", err)
			}
		}
	}

	qe := query.NewQueryEngine(db)

	aq := &query.AggregationQuery{
		Query: &query.Query{
			MinTime: 0,
			MaxTime: 50000,
		},
		Function: query.Avg,
		Step:     5000,
		GroupBy:  []string{"region"}, // Group by region
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := qe.Aggregate(aq)
		if err != nil {
			b.Fatalf("aggregation failed: %v", err)
		}
		if len(result.Series) == 0 {
			b.Fatal("expected aggregated results")
		}
	}
}

func BenchmarkQueryEngine_WithMatchers(b *testing.B) {
	db := setupBenchDB(b)
	defer db.Close()

	// Insert data with various labels
	for i := 0; i < 100; i++ {
		s := series.NewSeries(map[string]string{
			"__name__": "metric",
			"host":     benchFormatInt("server", i),
			"env":      []string{"prod", "dev", "staging"}[i%3],
		})

		samples := make([]series.Sample, 50)
		for j := 0; j < 50; j++ {
			samples[j] = series.Sample{
				Timestamp: int64(j * 1000),
				Value:     float64(j),
			}
		}

		if err := db.Insert(s, samples); err != nil {
			b.Fatalf("failed to insert: %v", err)
		}
	}

	qe := query.NewQueryEngine(db)

	// Create matchers
	matchers := index.Matchers{
		index.MustNewMatcher(index.MatchEqual, "__name__", "metric"),
		index.MustNewMatcher(index.MatchEqual, "env", "prod"),
	}

	q := &query.Query{
		Matchers: matchers,
		MinTime:  0,
		MaxTime:  50000,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := qe.ExecQuery(q)
		if err != nil {
			b.Fatalf("query failed: %v", err)
		}
		if len(result.Series) == 0 {
			b.Fatal("expected filtered results")
		}
	}
}

// setupBenchDB creates a TSDB instance for benchmarking.
func setupBenchDB(b *testing.B) *storage.TSDB {
	b.Helper()

	tmpDir := b.TempDir()
	db, err := storage.Open(storage.DefaultOptions(tmpDir))
	if err != nil {
		b.Fatalf("failed to open TSDB: %v", err)
	}

	return db
}

// benchFormatInt formats an integer with a prefix for benchmarks.
func benchFormatInt(prefix string, num int) string {
	return prefix + string(rune('0'+num))
}
