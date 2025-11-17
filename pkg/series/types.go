package series

import (
	"hash/fnv"
	"sort"
)

// Sample represents a single time-series data point.
// Timestamp is stored as Unix milliseconds for consistency with Prometheus.
type Sample struct {
	Timestamp int64   // Unix milliseconds
	Value     float64 // The metric value
}

// Series represents a time-series identified by a set of labels.
// Each unique combination of labels creates a unique series.
type Series struct {
	Labels map[string]string // Label key-value pairs (e.g., {__name__: "cpu_usage", host: "server1"})
	Hash   uint64            // Computed hash for fast lookup and comparison
}

// NewSeries creates a new Series from the provided labels and computes its hash.
func NewSeries(labels map[string]string) *Series {
	s := &Series{
		Labels: labels,
	}
	s.Hash = s.computeHash()
	return s
}

// computeHash generates a hash for the series based on its labels.
// The hash is deterministic and considers both label names and values.
// Labels are sorted to ensure consistent hashing regardless of insertion order.
func (s *Series) computeHash() uint64 {
	// Sort label names for consistent hashing
	names := make([]string, 0, len(s.Labels))
	for name := range s.Labels {
		names = append(names, name)
	}
	sort.Strings(names)

	// Use FNV-1a hash for good distribution and speed
	h := fnv.New64a()
	for _, name := range names {
		h.Write([]byte(name))
		h.Write([]byte{0}) // Separator
		h.Write([]byte(s.Labels[name]))
		h.Write([]byte{0}) // Separator
	}

	return h.Sum64()
}

// String returns a human-readable representation of the series labels.
func (s *Series) String() string {
	if len(s.Labels) == 0 {
		return "{}"
	}

	// Sort for consistent output
	names := make([]string, 0, len(s.Labels))
	for name := range s.Labels {
		names = append(names, name)
	}
	sort.Strings(names)

	result := "{"
	for i, name := range names {
		if i > 0 {
			result += ", "
		}
		result += name + `="` + s.Labels[name] + `"`
	}
	result += "}"

	return result
}

// Equals checks if two series have the same labels (ignoring hash).
func (s *Series) Equals(other *Series) bool {
	if len(s.Labels) != len(other.Labels) {
		return false
	}

	for k, v := range s.Labels {
		if otherV, exists := other.Labels[k]; !exists || otherV != v {
			return false
		}
	}

	return true
}

// Clone creates a deep copy of the series.
func (s *Series) Clone() *Series {
	labels := make(map[string]string, len(s.Labels))
	for k, v := range s.Labels {
		labels[k] = v
	}
	return NewSeries(labels)
}
