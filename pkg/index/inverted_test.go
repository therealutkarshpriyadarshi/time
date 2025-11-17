package index

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

func TestNewInvertedIndex(t *testing.T) {
	idx := NewInvertedIndex()
	if idx == nil {
		t.Fatal("NewInvertedIndex returned nil")
	}

	stats := idx.Stats()
	if stats.SeriesCount != 0 {
		t.Errorf("SeriesCount = %d, want 0", stats.SeriesCount)
	}
	if stats.LabelCount != 0 {
		t.Errorf("LabelCount = %d, want 0", stats.LabelCount)
	}
}

func TestInvertedIndex_Add(t *testing.T) {
	idx := NewInvertedIndex()

	labels := map[string]string{
		"host":   "server1",
		"metric": "cpu",
	}

	err := idx.Add(1, labels)
	if err != nil {
		t.Fatalf("Add() error = %v", err)
	}

	stats := idx.Stats()
	if stats.SeriesCount != 1 {
		t.Errorf("SeriesCount = %d, want 1", stats.SeriesCount)
	}
	if stats.LabelCount != 2 {
		t.Errorf("LabelCount = %d, want 2", stats.LabelCount)
	}
}

func TestInvertedIndex_Add_InvalidInput(t *testing.T) {
	idx := NewInvertedIndex()

	tests := []struct {
		name   string
		id     series.SeriesID
		labels map[string]string
	}{
		{
			name:   "zero ID",
			id:     0,
			labels: map[string]string{"host": "server1"},
		},
		{
			name:   "empty labels",
			id:     1,
			labels: map[string]string{},
		},
		{
			name:   "nil labels",
			id:     1,
			labels: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := idx.Add(tt.id, tt.labels)
			if err == nil {
				t.Error("Add() expected error, got nil")
			}
		})
	}
}

func TestInvertedIndex_Lookup_Equal(t *testing.T) {
	idx := NewInvertedIndex()

	// Add test data
	idx.Add(1, map[string]string{"host": "server1", "metric": "cpu"})
	idx.Add(2, map[string]string{"host": "server2", "metric": "cpu"})
	idx.Add(3, map[string]string{"host": "server1", "metric": "memory"})

	tests := []struct {
		name     string
		matchers Matchers
		wantIDs  []uint32
	}{
		{
			name: "single matcher - host=server1",
			matchers: Matchers{
				MustNewMatcher(MatchEqual, "host", "server1"),
			},
			wantIDs: []uint32{1, 3},
		},
		{
			name: "single matcher - metric=cpu",
			matchers: Matchers{
				MustNewMatcher(MatchEqual, "metric", "cpu"),
			},
			wantIDs: []uint32{1, 2},
		},
		{
			name: "multiple matchers - host=server1 AND metric=cpu",
			matchers: Matchers{
				MustNewMatcher(MatchEqual, "host", "server1"),
				MustNewMatcher(MatchEqual, "metric", "cpu"),
			},
			wantIDs: []uint32{1},
		},
		{
			name: "no matches",
			matchers: Matchers{
				MustNewMatcher(MatchEqual, "host", "server99"),
			},
			wantIDs: []uint32{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := idx.Lookup(tt.matchers)
			if err != nil {
				t.Fatalf("Lookup() error = %v", err)
			}

			got := result.ToArray()
			if !equalUint32Slices(got, tt.wantIDs) {
				t.Errorf("Lookup() = %v, want %v", got, tt.wantIDs)
			}
		})
	}
}

func TestInvertedIndex_Lookup_NotEqual(t *testing.T) {
	idx := NewInvertedIndex()

	idx.Add(1, map[string]string{"host": "server1", "env": "prod"})
	idx.Add(2, map[string]string{"host": "server2", "env": "dev"})
	idx.Add(3, map[string]string{"host": "server3", "env": "prod"})

	tests := []struct {
		name     string
		matchers Matchers
		wantIDs  []uint32
	}{
		{
			name: "host!=server1",
			matchers: Matchers{
				MustNewMatcher(MatchNotEqual, "host", "server1"),
			},
			wantIDs: []uint32{2, 3},
		},
		{
			name: "env!=prod",
			matchers: Matchers{
				MustNewMatcher(MatchNotEqual, "env", "prod"),
			},
			wantIDs: []uint32{2},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := idx.Lookup(tt.matchers)
			if err != nil {
				t.Fatalf("Lookup() error = %v", err)
			}

			got := result.ToArray()
			if !equalUint32Slices(got, tt.wantIDs) {
				t.Errorf("Lookup() = %v, want %v", got, tt.wantIDs)
			}
		})
	}
}

func TestInvertedIndex_Lookup_Regexp(t *testing.T) {
	idx := NewInvertedIndex()

	idx.Add(1, map[string]string{"host": "server1"})
	idx.Add(2, map[string]string{"host": "server2"})
	idx.Add(3, map[string]string{"host": "database1"})
	idx.Add(4, map[string]string{"host": "server123"})

	tests := []struct {
		name     string
		matchers Matchers
		wantIDs  []uint32
	}{
		{
			name: "host=~server.*",
			matchers: Matchers{
				MustNewMatcher(MatchRegexp, "host", "server.*"),
			},
			wantIDs: []uint32{1, 2, 4},
		},
		{
			name: "host=~^server[0-9]$",
			matchers: Matchers{
				MustNewMatcher(MatchRegexp, "host", "^server[0-9]$"),
			},
			wantIDs: []uint32{1, 2},
		},
		{
			name: "host=~database.*",
			matchers: Matchers{
				MustNewMatcher(MatchRegexp, "host", "database.*"),
			},
			wantIDs: []uint32{3},
		},
		{
			name: "host=~nomatch",
			matchers: Matchers{
				MustNewMatcher(MatchRegexp, "host", "nomatch"),
			},
			wantIDs: []uint32{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := idx.Lookup(tt.matchers)
			if err != nil {
				t.Fatalf("Lookup() error = %v", err)
			}

			got := result.ToArray()
			if !equalUint32Slices(got, tt.wantIDs) {
				t.Errorf("Lookup() = %v, want %v", got, tt.wantIDs)
			}
		})
	}
}

func TestInvertedIndex_Lookup_NotRegexp(t *testing.T) {
	idx := NewInvertedIndex()

	idx.Add(1, map[string]string{"host": "server1"})
	idx.Add(2, map[string]string{"host": "server2"})
	idx.Add(3, map[string]string{"host": "database1"})

	tests := []struct {
		name     string
		matchers Matchers
		wantIDs  []uint32
	}{
		{
			name: "host!~server.*",
			matchers: Matchers{
				MustNewMatcher(MatchNotRegexp, "host", "server.*"),
			},
			wantIDs: []uint32{3},
		},
		{
			name: "host!~^server[0-9]$",
			matchers: Matchers{
				MustNewMatcher(MatchNotRegexp, "host", "^server[0-9]$"),
			},
			wantIDs: []uint32{3},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := idx.Lookup(tt.matchers)
			if err != nil {
				t.Fatalf("Lookup() error = %v", err)
			}

			got := result.ToArray()
			if !equalUint32Slices(got, tt.wantIDs) {
				t.Errorf("Lookup() = %v, want %v", got, tt.wantIDs)
			}
		})
	}
}

func TestInvertedIndex_Lookup_ComplexQuery(t *testing.T) {
	idx := NewInvertedIndex()

	// Add test data
	idx.Add(1, map[string]string{"host": "server1", "env": "prod", "metric": "cpu"})
	idx.Add(2, map[string]string{"host": "server2", "env": "prod", "metric": "cpu"})
	idx.Add(3, map[string]string{"host": "server3", "env": "dev", "metric": "cpu"})
	idx.Add(4, map[string]string{"host": "server1", "env": "prod", "metric": "memory"})
	idx.Add(5, map[string]string{"host": "database1", "env": "prod", "metric": "cpu"})

	tests := []struct {
		name     string
		matchers Matchers
		wantIDs  []uint32
	}{
		{
			name: "host=~server.* AND env=prod AND metric=cpu",
			matchers: Matchers{
				MustNewMatcher(MatchRegexp, "host", "server.*"),
				MustNewMatcher(MatchEqual, "env", "prod"),
				MustNewMatcher(MatchEqual, "metric", "cpu"),
			},
			wantIDs: []uint32{1, 2},
		},
		{
			name: "host=~server.* AND env!=dev",
			matchers: Matchers{
				MustNewMatcher(MatchRegexp, "host", "server.*"),
				MustNewMatcher(MatchNotEqual, "env", "dev"),
			},
			wantIDs: []uint32{1, 2, 4},
		},
		{
			name: "env=prod AND metric=cpu AND host!=server1",
			matchers: Matchers{
				MustNewMatcher(MatchEqual, "env", "prod"),
				MustNewMatcher(MatchEqual, "metric", "cpu"),
				MustNewMatcher(MatchNotEqual, "host", "server1"),
			},
			wantIDs: []uint32{2, 5},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := idx.Lookup(tt.matchers)
			if err != nil {
				t.Fatalf("Lookup() error = %v", err)
			}

			got := result.ToArray()
			if !equalUint32Slices(got, tt.wantIDs) {
				t.Errorf("Lookup() = %v, want %v", got, tt.wantIDs)
			}
		})
	}
}

func TestInvertedIndex_Lookup_EmptyMatchers(t *testing.T) {
	idx := NewInvertedIndex()

	_, err := idx.Lookup(Matchers{})
	if err == nil {
		t.Error("Lookup() with empty matchers expected error, got nil")
	}
}

func TestInvertedIndex_Delete(t *testing.T) {
	idx := NewInvertedIndex()

	idx.Add(1, map[string]string{"host": "server1", "metric": "cpu"})
	idx.Add(2, map[string]string{"host": "server2", "metric": "cpu"})
	idx.Add(3, map[string]string{"host": "server1", "metric": "memory"})

	// Before delete
	result, _ := idx.Lookup(Matchers{MustNewMatcher(MatchEqual, "host", "server1")})
	if !equalUint32Slices(result.ToArray(), []uint32{1, 3}) {
		t.Errorf("Before delete: got %v, want [1, 3]", result.ToArray())
	}

	// Delete series 1
	idx.Delete(1)

	// After delete
	result, _ = idx.Lookup(Matchers{MustNewMatcher(MatchEqual, "host", "server1")})
	if !equalUint32Slices(result.ToArray(), []uint32{3}) {
		t.Errorf("After delete: got %v, want [3]", result.ToArray())
	}

	// Series 2 should still exist
	result, _ = idx.Lookup(Matchers{MustNewMatcher(MatchEqual, "host", "server2")})
	if !equalUint32Slices(result.ToArray(), []uint32{2}) {
		t.Errorf("Series 2: got %v, want [2]", result.ToArray())
	}

	stats := idx.Stats()
	if stats.SeriesCount != 2 {
		t.Errorf("SeriesCount after delete = %d, want 2", stats.SeriesCount)
	}
}

func TestInvertedIndex_LabelNames(t *testing.T) {
	idx := NewInvertedIndex()

	idx.Add(1, map[string]string{"host": "server1", "metric": "cpu", "env": "prod"})
	idx.Add(2, map[string]string{"host": "server2", "dc": "us-west"})

	names := idx.LabelNames()
	expected := []string{"dc", "env", "host", "metric"}

	if !equalStringSlices(names, expected) {
		t.Errorf("LabelNames() = %v, want %v", names, expected)
	}
}

func TestInvertedIndex_LabelValues(t *testing.T) {
	idx := NewInvertedIndex()

	idx.Add(1, map[string]string{"host": "server1"})
	idx.Add(2, map[string]string{"host": "server2"})
	idx.Add(3, map[string]string{"host": "server1"}) // duplicate

	values := idx.LabelValues("host")
	expected := []string{"server1", "server2"}

	if !equalStringSlices(values, expected) {
		t.Errorf("LabelValues(host) = %v, want %v", values, expected)
	}

	// Non-existent label
	values = idx.LabelValues("nonexistent")
	if values != nil {
		t.Errorf("LabelValues(nonexistent) = %v, want nil", values)
	}
}

func TestInvertedIndex_Stats(t *testing.T) {
	idx := NewInvertedIndex()

	idx.Add(1, map[string]string{"host": "server1", "metric": "cpu"})
	idx.Add(2, map[string]string{"host": "server2", "metric": "cpu"})
	idx.Add(3, map[string]string{"host": "server1", "metric": "memory"})

	stats := idx.Stats()

	if stats.SeriesCount != 3 {
		t.Errorf("SeriesCount = %d, want 3", stats.SeriesCount)
	}

	if stats.LabelCount != 2 {
		t.Errorf("LabelCount = %d, want 2", stats.LabelCount)
	}

	if stats.LabelValueCount["host"] != 2 {
		t.Errorf("LabelValueCount[host] = %d, want 2", stats.LabelValueCount["host"])
	}

	if stats.LabelValueCount["metric"] != 2 {
		t.Errorf("LabelValueCount[metric] = %d, want 2", stats.LabelValueCount["metric"])
	}

	if stats.TotalPostingLists != 4 {
		t.Errorf("TotalPostingLists = %d, want 4", stats.TotalPostingLists)
	}

	if stats.MemoryBytes == 0 {
		t.Error("MemoryBytes = 0, expected > 0")
	}
}

func TestInvertedIndex_Persistence(t *testing.T) {
	// Create and populate index
	idx1 := NewInvertedIndex()
	idx1.Add(1, map[string]string{"host": "server1", "metric": "cpu"})
	idx1.Add(2, map[string]string{"host": "server2", "metric": "cpu"})
	idx1.Add(3, map[string]string{"host": "server1", "metric": "memory"})

	// Write to buffer
	buf := new(bytes.Buffer)
	n, err := idx1.WriteTo(buf)
	if err != nil {
		t.Fatalf("WriteTo() error = %v", err)
	}
	if n == 0 {
		t.Error("WriteTo() wrote 0 bytes")
	}

	// Read into new index
	idx2 := NewInvertedIndex()
	n2, err := idx2.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom() error = %v", err)
	}
	if n2 != n {
		t.Errorf("ReadFrom() read %d bytes, WriteTo() wrote %d bytes", n2, n)
	}

	// Verify stats match
	stats1 := idx1.Stats()
	stats2 := idx2.Stats()

	if stats1.SeriesCount != stats2.SeriesCount {
		t.Errorf("SeriesCount mismatch: %d vs %d", stats1.SeriesCount, stats2.SeriesCount)
	}
	if stats1.LabelCount != stats2.LabelCount {
		t.Errorf("LabelCount mismatch: %d vs %d", stats1.LabelCount, stats2.LabelCount)
	}

	// Verify queries return same results
	matchers := Matchers{MustNewMatcher(MatchEqual, "host", "server1")}
	result1, _ := idx1.Lookup(matchers)
	result2, _ := idx2.Lookup(matchers)

	if !result1.Equals(result2) {
		t.Errorf("Query results mismatch: %v vs %v", result1.ToArray(), result2.ToArray())
	}
}

func TestInvertedIndex_Persistence_Empty(t *testing.T) {
	idx1 := NewInvertedIndex()

	// Write empty index
	buf := new(bytes.Buffer)
	_, err := idx1.WriteTo(buf)
	if err != nil {
		t.Fatalf("WriteTo() error = %v", err)
	}

	// Read back
	idx2 := NewInvertedIndex()
	_, err = idx2.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom() error = %v", err)
	}

	stats := idx2.Stats()
	if stats.SeriesCount != 0 {
		t.Errorf("SeriesCount = %d, want 0", stats.SeriesCount)
	}
}

func TestInvertedIndex_Persistence_InvalidData(t *testing.T) {
	idx := NewInvertedIndex()

	// Try to read from invalid data
	buf := bytes.NewBuffer([]byte("invalid data"))
	_, err := idx.ReadFrom(buf)
	if err == nil {
		t.Error("ReadFrom() expected error with invalid data, got nil")
	}
}

func TestInvertedIndex_LargeDataset(t *testing.T) {
	idx := NewInvertedIndex()

	// Add 1000 series
	for i := 1; i <= 1000; i++ {
		labels := map[string]string{
			"host":   fmt.Sprintf("server%d", i%10),
			"metric": fmt.Sprintf("metric%d", i%5),
			"env":    fmt.Sprintf("env%d", i%3),
		}
		if err := idx.Add(series.SeriesID(i), labels); err != nil {
			t.Fatalf("Add() error = %v", err)
		}
	}

	stats := idx.Stats()
	if stats.SeriesCount != 1000 {
		t.Errorf("SeriesCount = %d, want 1000", stats.SeriesCount)
	}

	// Query should work efficiently
	matchers := Matchers{
		MustNewMatcher(MatchEqual, "host", "server5"),
		MustNewMatcher(MatchEqual, "metric", "metric2"),
	}

	result, err := idx.Lookup(matchers)
	if err != nil {
		t.Fatalf("Lookup() error = %v", err)
	}

	// Should have some results
	if result.GetCardinality() == 0 {
		t.Error("Lookup() returned no results")
	}
}

// Helper functions

func equalUint32Slices(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
