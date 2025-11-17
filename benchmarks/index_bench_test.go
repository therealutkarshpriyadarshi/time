package benchmarks

import (
	"fmt"
	"testing"

	"github.com/therealutkarshpriyadarshi/time/pkg/index"
	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

// BenchmarkInvertedIndex_Add benchmarks adding series to the index.
func BenchmarkInvertedIndex_Add(b *testing.B) {
	idx := index.NewInvertedIndex()
	labels := map[string]string{
		"host":   "server1",
		"metric": "cpu",
		"env":    "prod",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Add(series.SeriesID(i+1), labels)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "series/sec")
}

// BenchmarkInvertedIndex_Add_VaryingLabels benchmarks adding series with different label cardinality.
func BenchmarkInvertedIndex_Add_VaryingLabels(b *testing.B) {
	idx := index.NewInvertedIndex()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		labels := map[string]string{
			"host":   fmt.Sprintf("server%d", i%10),
			"metric": fmt.Sprintf("metric%d", i%5),
			"env":    fmt.Sprintf("env%d", i%3),
		}
		idx.Add(series.SeriesID(i+1), labels)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "series/sec")
}

// BenchmarkInvertedIndex_Lookup_Equal benchmarks exact match queries.
func BenchmarkInvertedIndex_Lookup_Equal(b *testing.B) {
	idx := index.NewInvertedIndex()

	// Populate index with 10k series
	for i := 1; i <= 10000; i++ {
		labels := map[string]string{
			"host":   fmt.Sprintf("server%d", i%100),
			"metric": fmt.Sprintf("metric%d", i%50),
			"env":    fmt.Sprintf("env%d", i%10),
		}
		idx.Add(series.SeriesID(i), labels)
	}

	matchers := index.Matchers{
		index.MustNewMatcher(index.MatchEqual, "host", "server50"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := idx.Lookup(matchers)
		if err != nil {
			b.Fatal(err)
		}
		if result.GetCardinality() == 0 {
			b.Fatal("no results")
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

// BenchmarkInvertedIndex_Lookup_Regexp benchmarks regex match queries.
func BenchmarkInvertedIndex_Lookup_Regexp(b *testing.B) {
	idx := index.NewInvertedIndex()

	// Populate index with 10k series
	for i := 1; i <= 10000; i++ {
		labels := map[string]string{
			"host":   fmt.Sprintf("server%d", i%100),
			"metric": fmt.Sprintf("metric%d", i%50),
		}
		idx.Add(series.SeriesID(i), labels)
	}

	matchers := index.Matchers{
		index.MustNewMatcher(index.MatchRegexp, "host", "server[0-9]+"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := idx.Lookup(matchers)
		if err != nil {
			b.Fatal(err)
		}
		if result.GetCardinality() == 0 {
			b.Fatal("no results")
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

// BenchmarkInvertedIndex_Lookup_Complex benchmarks complex multi-matcher queries.
func BenchmarkInvertedIndex_Lookup_Complex(b *testing.B) {
	idx := index.NewInvertedIndex()

	// Populate index with 10k series
	for i := 1; i <= 10000; i++ {
		labels := map[string]string{
			"host":   fmt.Sprintf("server%d", i%100),
			"metric": fmt.Sprintf("metric%d", i%50),
			"env":    fmt.Sprintf("env%d", i%10),
			"dc":     fmt.Sprintf("dc%d", i%5),
		}
		idx.Add(series.SeriesID(i), labels)
	}

	matchers := index.Matchers{
		index.MustNewMatcher(index.MatchRegexp, "host", "server[0-9]+"),
		index.MustNewMatcher(index.MatchEqual, "env", "env5"),
		index.MustNewMatcher(index.MatchNotEqual, "dc", "dc0"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := idx.Lookup(matchers)
		if err != nil {
			b.Fatal(err)
		}
		// Result might be empty, which is okay
		_ = result
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

// BenchmarkInvertedIndex_Lookup_10M benchmarks lookup on 10 million series.
func BenchmarkInvertedIndex_Lookup_10M(b *testing.B) {
	if testing.Short() {
		b.Skip("skipping large benchmark in short mode")
	}

	idx := index.NewInvertedIndex()

	// Populate index with 10M series
	b.Log("Populating index with 10M series...")
	for i := 1; i <= 10_000_000; i++ {
		labels := map[string]string{
			"host":   fmt.Sprintf("server%d", i%1000),
			"metric": fmt.Sprintf("metric%d", i%100),
			"env":    fmt.Sprintf("env%d", i%10),
		}
		idx.Add(series.SeriesID(i), labels)

		if i%1_000_000 == 0 {
			b.Logf("Added %dM series", i/1_000_000)
		}
	}

	matchers := index.Matchers{
		index.MustNewMatcher(index.MatchEqual, "host", "server500"),
		index.MustNewMatcher(index.MatchEqual, "env", "env5"),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result, err := idx.Lookup(matchers)
		if err != nil {
			b.Fatal(err)
		}
		_ = result
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

// BenchmarkInvertedIndex_Delete benchmarks series deletion.
func BenchmarkInvertedIndex_Delete(b *testing.B) {
	// Populate index
	idx := index.NewInvertedIndex()
	for i := 1; i <= 100000; i++ {
		labels := map[string]string{
			"host":   fmt.Sprintf("server%d", i%100),
			"metric": "cpu",
		}
		idx.Add(series.SeriesID(i), labels)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Delete(series.SeriesID(i % 100000))
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "deletes/sec")
}

// BenchmarkInvertedIndex_Parallel benchmarks concurrent lookups.
func BenchmarkInvertedIndex_Parallel(b *testing.B) {
	idx := index.NewInvertedIndex()

	// Populate index
	for i := 1; i <= 10000; i++ {
		labels := map[string]string{
			"host":   fmt.Sprintf("server%d", i%100),
			"metric": fmt.Sprintf("metric%d", i%50),
		}
		idx.Add(series.SeriesID(i), labels)
	}

	matchers := index.Matchers{
		index.MustNewMatcher(index.MatchEqual, "host", "server50"),
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			result, err := idx.Lookup(matchers)
			if err != nil {
				b.Fatal(err)
			}
			_ = result
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "queries/sec")
}

// BenchmarkRegistry_GetOrCreate benchmarks series ID allocation.
func BenchmarkRegistry_GetOrCreate(b *testing.B) {
	registry := series.NewRegistry(series.RegistryConfig{})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := series.NewSeries(map[string]string{
			"id": fmt.Sprintf("%d", i),
		})
		_, err := registry.GetOrCreate(s)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

// BenchmarkRegistry_GetOrCreate_SameSeries benchmarks cache hits.
func BenchmarkRegistry_GetOrCreate_SameSeries(b *testing.B) {
	registry := series.NewRegistry(series.RegistryConfig{})
	s := series.NewSeries(map[string]string{"host": "server1"})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := registry.GetOrCreate(s)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

// BenchmarkRegistry_GetOrCreate_Parallel benchmarks concurrent ID allocation.
func BenchmarkRegistry_GetOrCreate_Parallel(b *testing.B) {
	registry := series.NewRegistry(series.RegistryConfig{})

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s := series.NewSeries(map[string]string{
				"id": fmt.Sprintf("%d", i),
			})
			_, err := registry.GetOrCreate(s)
			if err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

// BenchmarkRegistry_Get benchmarks series lookups.
func BenchmarkRegistry_Get(b *testing.B) {
	registry := series.NewRegistry(series.RegistryConfig{})
	s := series.NewSeries(map[string]string{"host": "server1"})
	registry.GetOrCreate(s)
	hash := s.Hash()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = registry.Get(hash)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/sec")
}

// BenchmarkMatcher_Matches benchmarks matcher evaluation.
func BenchmarkMatcher_Matches(b *testing.B) {
	m := index.MustNewMatcher(index.MatchRegexp, "host", "server[0-9]+")
	value := "server123"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = m.Matches(value)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "matches/sec")
}

// BenchmarkMatchers_Matches benchmarks multi-matcher evaluation.
func BenchmarkMatchers_Matches(b *testing.B) {
	matchers := index.Matchers{
		index.MustNewMatcher(index.MatchEqual, "host", "server1"),
		index.MustNewMatcher(index.MatchRegexp, "metric", "cpu.*"),
		index.MustNewMatcher(index.MatchNotEqual, "env", "dev"),
	}

	labels := map[string]string{
		"host":   "server1",
		"metric": "cpu_usage",
		"env":    "prod",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = matchers.Matches(labels)
	}
	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "matches/sec")
}
