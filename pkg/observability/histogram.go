package observability

import (
	"math"
	"sort"
	"sync"
)

// Histogram tracks distribution of observations
type Histogram struct {
	name string
	mu   sync.RWMutex

	// Buckets for percentile calculation
	observations []float64

	// Pre-computed statistics
	count  int64
	sum    float64
	min    float64
	max    float64
}

// NewHistogram creates a new histogram
func NewHistogram(name string) *Histogram {
	return &Histogram{
		name:         name,
		observations: make([]float64, 0, 1000),
		min:          math.MaxFloat64,
		max:          -math.MaxFloat64,
	}
}

// Observe adds an observation to the histogram
func (h *Histogram) Observe(value float64) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.observations = append(h.observations, value)
	h.count++
	h.sum += value

	if value < h.min {
		h.min = value
	}
	if value > h.max {
		h.max = value
	}

	// Limit memory usage - keep only last 10000 observations
	if len(h.observations) > 10000 {
		h.observations = h.observations[len(h.observations)-10000:]
	}
}

// Stats returns histogram statistics
type HistogramStats struct {
	Name   string
	Count  int64
	Sum    float64
	Mean   float64
	Min    float64
	Max    float64
	P50    float64
	P90    float64
	P95    float64
	P99    float64
}

// GetStats returns current histogram statistics
func (h *Histogram) GetStats() *HistogramStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	stats := &HistogramStats{
		Name:  h.name,
		Count: h.count,
		Sum:   h.sum,
		Min:   h.min,
		Max:   h.max,
	}

	if h.count > 0 {
		stats.Mean = h.sum / float64(h.count)
	}

	if len(h.observations) > 0 {
		// Make a copy and sort for percentile calculation
		sorted := make([]float64, len(h.observations))
		copy(sorted, h.observations)
		sort.Float64s(sorted)

		stats.P50 = percentile(sorted, 0.50)
		stats.P90 = percentile(sorted, 0.90)
		stats.P95 = percentile(sorted, 0.95)
		stats.P99 = percentile(sorted, 0.99)
	}

	return stats
}

// Reset clears the histogram
func (h *Histogram) Reset() {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.observations = make([]float64, 0, 1000)
	h.count = 0
	h.sum = 0
	h.min = math.MaxFloat64
	h.max = -math.MaxFloat64
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}

	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}
