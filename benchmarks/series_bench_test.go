package benchmarks

import (
	"fmt"
	"testing"

	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

// BenchmarkSeriesHash measures the performance of series hashing
func BenchmarkSeriesHash(b *testing.B) {
	labels := map[string]string{
		"__name__": "http_requests_total",
		"method":   "GET",
		"path":     "/api/users",
		"status":   "200",
		"host":     "server1",
		"region":   "us-west-2",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := series.NewSeries(labels)
		_ = s.Hash
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "hashes/sec")
}

// BenchmarkSeriesHashSmall measures hashing with few labels
func BenchmarkSeriesHashSmall(b *testing.B) {
	labels := map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := series.NewSeries(labels)
		_ = s.Hash
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "hashes/sec")
}

// BenchmarkSeriesHashLarge measures hashing with many labels (high cardinality)
func BenchmarkSeriesHashLarge(b *testing.B) {
	labels := map[string]string{
		"__name__":  "http_requests_total",
		"method":    "GET",
		"path":      "/api/users/profile/settings",
		"status":    "200",
		"host":      "server1.prod.example.com",
		"region":    "us-west-2",
		"zone":      "us-west-2a",
		"instance":  "i-1234567890abcdef0",
		"job":       "api-server",
		"namespace": "production",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s := series.NewSeries(labels)
		_ = s.Hash
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "hashes/sec")
}

// BenchmarkSeriesHash1M measures time to hash 1 million series
// Target: < 1 second
func BenchmarkSeriesHash1M(b *testing.B) {
	// Pre-generate labels
	numSeries := 1000000
	labelSets := make([]map[string]string, numSeries)
	for i := 0; i < numSeries; i++ {
		labelSets[i] = map[string]string{
			"__name__": "metric",
			"host":     fmt.Sprintf("server%d", i%1000),
			"id":       fmt.Sprintf("%d", i),
		}
	}

	b.ResetTimer()
	for i := 0; i < numSeries; i++ {
		s := series.NewSeries(labelSets[i])
		_ = s.Hash
	}

	b.ReportMetric(float64(numSeries)/b.Elapsed().Seconds(), "hashes/sec")
}

// BenchmarkSeriesEquals measures comparison performance
func BenchmarkSeriesEquals(b *testing.B) {
	s1 := series.NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
		"region":   "us-west",
	})

	s2 := series.NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
		"region":   "us-west",
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s1.Equals(s2)
	}
}

// BenchmarkSeriesClone measures cloning performance
func BenchmarkSeriesClone(b *testing.B) {
	s := series.NewSeries(map[string]string{
		"__name__": "http_requests_total",
		"method":   "GET",
		"path":     "/api/users",
		"status":   "200",
		"host":     "server1",
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.Clone()
	}
}

// BenchmarkSeriesString measures String() performance
func BenchmarkSeriesString(b *testing.B) {
	s := series.NewSeries(map[string]string{
		"__name__": "http_requests_total",
		"method":   "GET",
		"path":     "/api/users",
		"status":   "200",
		"host":     "server1",
	})

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = s.String()
	}
}

// BenchmarkSeriesHashParallel measures hashing performance with concurrent access
func BenchmarkSeriesHashParallel(b *testing.B) {
	labels := map[string]string{
		"__name__": "http_requests_total",
		"method":   "GET",
		"path":     "/api/users",
		"status":   "200",
		"host":     "server1",
	}

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			s := series.NewSeries(labels)
			_ = s.Hash
		}
	})

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "hashes/sec")
}

// BenchmarkSeriesCreation measures the full series creation overhead
func BenchmarkSeriesCreation(b *testing.B) {
	labels := map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
		"region":   "us-west",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = series.NewSeries(labels)
	}
}

// BenchmarkHighCardinalitySeries simulates high cardinality scenario
func BenchmarkHighCardinalitySeries(b *testing.B) {
	numUniqueSeries := 100000
	labelSets := make([]map[string]string, numUniqueSeries)

	// Pre-generate unique label sets
	for i := 0; i < numUniqueSeries; i++ {
		labelSets[i] = map[string]string{
			"__name__": "api_requests",
			"endpoint": fmt.Sprintf("/api/v1/resource/%d", i%1000),
			"method":   []string{"GET", "POST", "PUT", "DELETE"}[i%4],
			"status":   fmt.Sprintf("%d", 200+(i%5)*100),
			"user_id":  fmt.Sprintf("user_%d", i),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % numUniqueSeries
		_ = series.NewSeries(labelSets[idx])
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "series/sec")
}
