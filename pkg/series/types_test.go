package series

import (
	"testing"
)

func TestNewSeries(t *testing.T) {
	labels := map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
		"region":   "us-west",
	}

	s := NewSeries(labels)

	if s == nil {
		t.Fatal("NewSeries returned nil")
	}

	if s.Hash == 0 {
		t.Error("Series hash should not be zero")
	}

	if len(s.Labels) != len(labels) {
		t.Errorf("Expected %d labels, got %d", len(labels), len(s.Labels))
	}

	for k, v := range labels {
		if s.Labels[k] != v {
			t.Errorf("Label %s: expected %s, got %s", k, v, s.Labels[k])
		}
	}
}

func TestSeriesHash_Deterministic(t *testing.T) {
	labels := map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	}

	s1 := NewSeries(labels)
	s2 := NewSeries(labels)

	if s1.Hash != s2.Hash {
		t.Errorf("Same labels should produce same hash: %d != %d", s1.Hash, s2.Hash)
	}
}

func TestSeriesHash_OrderIndependent(t *testing.T) {
	// Create two maps with same content but potentially different iteration order
	labels1 := map[string]string{
		"a": "1",
		"b": "2",
		"c": "3",
	}

	labels2 := map[string]string{
		"c": "3",
		"a": "1",
		"b": "2",
	}

	s1 := NewSeries(labels1)
	s2 := NewSeries(labels2)

	if s1.Hash != s2.Hash {
		t.Errorf("Label insertion order should not affect hash: %d != %d", s1.Hash, s2.Hash)
	}
}

func TestSeriesHash_Unique(t *testing.T) {
	s1 := NewSeries(map[string]string{"host": "server1"})
	s2 := NewSeries(map[string]string{"host": "server2"})

	if s1.Hash == s2.Hash {
		t.Error("Different labels should produce different hashes")
	}
}

func TestSeriesString(t *testing.T) {
	tests := []struct {
		name   string
		labels map[string]string
		want   string
	}{
		{
			name:   "empty labels",
			labels: map[string]string{},
			want:   "{}",
		},
		{
			name:   "single label",
			labels: map[string]string{"host": "server1"},
			want:   `{host="server1"}`,
		},
		{
			name:   "multiple labels",
			labels: map[string]string{"host": "server1", "region": "us-west"},
			want:   `{host="server1", region="us-west"}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewSeries(tt.labels)
			got := s.String()
			if got != tt.want {
				t.Errorf("String() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeriesEquals(t *testing.T) {
	s1 := NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	})

	s2 := NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	})

	s3 := NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server2",
	})

	if !s1.Equals(s2) {
		t.Error("Series with same labels should be equal")
	}

	if s1.Equals(s3) {
		t.Error("Series with different labels should not be equal")
	}
}

func TestSeriesEquals_DifferentLength(t *testing.T) {
	s1 := NewSeries(map[string]string{
		"host": "server1",
	})

	s2 := NewSeries(map[string]string{
		"host":   "server1",
		"region": "us-west",
	})

	if s1.Equals(s2) {
		t.Error("Series with different number of labels should not be equal")
	}
}

func TestSeriesClone(t *testing.T) {
	original := NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	})

	cloned := original.Clone()

	// Should be equal
	if !original.Equals(cloned) {
		t.Error("Cloned series should equal original")
	}

	// Should have same hash
	if original.Hash != cloned.Hash {
		t.Error("Cloned series should have same hash")
	}

	// Should be different objects
	if original == cloned {
		t.Error("Clone should create a new object")
	}

	// Modifying clone should not affect original
	cloned.Labels["region"] = "us-west"
	if original.Equals(cloned) {
		t.Error("Modifying clone should not affect original")
	}
}

func TestSample(t *testing.T) {
	sample := Sample{
		Timestamp: 1640000000000,
		Value:     0.85,
	}

	if sample.Timestamp != 1640000000000 {
		t.Errorf("Expected timestamp 1640000000000, got %d", sample.Timestamp)
	}

	if sample.Value != 0.85 {
		t.Errorf("Expected value 0.85, got %f", sample.Value)
	}
}

func TestSeriesHash_CollisionResistance(t *testing.T) {
	// Create many series and check for hash collisions
	seen := make(map[uint64]bool)
	collisions := 0

	for i := 0; i < 10000; i++ {
		labels := map[string]string{
			"__name__": "metric",
			"id":       string(rune(i)),
		}
		s := NewSeries(labels)

		if seen[s.Hash] {
			collisions++
		}
		seen[s.Hash] = true
	}

	// We expect very few collisions with FNV-1a
	if collisions > 10 {
		t.Errorf("Too many hash collisions: %d out of 10000", collisions)
	}
}
