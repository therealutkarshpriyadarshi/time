package series

import (
	"fmt"
	"sync"
	"testing"
)

func TestNewRegistry(t *testing.T) {
	tests := []struct {
		name           string
		cfg            RegistryConfig
		wantMaxCard    uint64
		wantLRUSize    int
		wantNextID     uint64
		wantCardinality int
	}{
		{
			name:           "default config",
			cfg:            RegistryConfig{},
			wantMaxCard:    uint64(MaxSeriesID),
			wantLRUSize:    DefaultLRUSize,
			wantNextID:     1,
			wantCardinality: 0,
		},
		{
			name: "custom config",
			cfg: RegistryConfig{
				MaxCardinality: 1000,
				LRUSize:        500,
			},
			wantMaxCard:    1000,
			wantLRUSize:    500,
			wantNextID:     1,
			wantCardinality: 0,
		},
		{
			name: "partial config - only max cardinality",
			cfg: RegistryConfig{
				MaxCardinality: 5000,
			},
			wantMaxCard:    5000,
			wantLRUSize:    DefaultLRUSize,
			wantNextID:     1,
			wantCardinality: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRegistry(tt.cfg)

			if r.maxCardinality != tt.wantMaxCard {
				t.Errorf("maxCardinality = %d, want %d", r.maxCardinality, tt.wantMaxCard)
			}
			if r.lruSize != tt.wantLRUSize {
				t.Errorf("lruSize = %d, want %d", r.lruSize, tt.wantLRUSize)
			}
			if got := r.nextID.Load(); got != tt.wantNextID {
				t.Errorf("nextID = %d, want %d", got, tt.wantNextID)
			}
			if got := r.Cardinality(); got != tt.wantCardinality {
				t.Errorf("Cardinality() = %d, want %d", got, tt.wantCardinality)
			}
		})
	}
}

func TestRegistry_GetOrCreate(t *testing.T) {
	r := NewRegistry(RegistryConfig{})

	s1 := NewSeries(map[string]string{"host": "server1", "metric": "cpu"})
	s2 := NewSeries(map[string]string{"host": "server2", "metric": "cpu"})
	s3 := NewSeries(map[string]string{"host": "server1", "metric": "cpu"}) // same as s1

	// First insert
	id1, err := r.GetOrCreate(s1)
	if err != nil {
		t.Fatalf("GetOrCreate(s1) error = %v", err)
	}
	if id1 == 0 {
		t.Error("GetOrCreate(s1) returned ID 0")
	}

	// Second insert (different series)
	id2, err := r.GetOrCreate(s2)
	if err != nil {
		t.Fatalf("GetOrCreate(s2) error = %v", err)
	}
	if id2 == 0 {
		t.Error("GetOrCreate(s2) returned ID 0")
	}
	if id1 == id2 {
		t.Errorf("GetOrCreate returned same ID for different series: %d", id1)
	}

	// Third insert (same as first - should return same ID)
	id3, err := r.GetOrCreate(s3)
	if err != nil {
		t.Fatalf("GetOrCreate(s3) error = %v", err)
	}
	if id3 != id1 {
		t.Errorf("GetOrCreate(s3) = %d, want %d (same as s1)", id3, id1)
	}

	// Verify cardinality
	if got := r.Cardinality(); got != 2 {
		t.Errorf("Cardinality() = %d, want 2", got)
	}
}

func TestRegistry_GetOrCreate_NilSeries(t *testing.T) {
	r := NewRegistry(RegistryConfig{})

	_, err := r.GetOrCreate(nil)
	if err == nil {
		t.Error("GetOrCreate(nil) expected error, got nil")
	}
}

func TestRegistry_GetOrCreate_MaxCardinality(t *testing.T) {
	r := NewRegistry(RegistryConfig{MaxCardinality: 2})

	s1 := NewSeries(map[string]string{"id": "1"})
	s2 := NewSeries(map[string]string{"id": "2"})
	s3 := NewSeries(map[string]string{"id": "3"})

	// First two should succeed
	if _, err := r.GetOrCreate(s1); err != nil {
		t.Fatalf("GetOrCreate(s1) error = %v", err)
	}
	if _, err := r.GetOrCreate(s2); err != nil {
		t.Fatalf("GetOrCreate(s2) error = %v", err)
	}

	// Third should fail (max cardinality reached)
	if _, err := r.GetOrCreate(s3); err == nil {
		t.Error("GetOrCreate(s3) expected max cardinality error, got nil")
	}

	// Inserting existing series should still work
	if _, err := r.GetOrCreate(s1); err != nil {
		t.Errorf("GetOrCreate(s1) again error = %v", err)
	}
}

func TestRegistry_Get(t *testing.T) {
	r := NewRegistry(RegistryConfig{})

	s1 := NewSeries(map[string]string{"host": "server1"})
	id1, _ := r.GetOrCreate(s1)

	// Get existing series
	if id, ok := r.Get(s1.Hash()); !ok || id != id1 {
		t.Errorf("Get(%d) = (%d, %v), want (%d, true)", s1.Hash(), id, ok, id1)
	}

	// Get non-existent series
	if id, ok := r.Get(12345); ok {
		t.Errorf("Get(12345) = (%d, %v), want (0, false)", id, ok)
	}
}

func TestRegistry_GetSeries(t *testing.T) {
	r := NewRegistry(RegistryConfig{})

	s1 := NewSeries(map[string]string{"host": "server1"})
	id1, _ := r.GetOrCreate(s1)

	// Get existing series
	series, ok := r.GetSeries(id1)
	if !ok {
		t.Fatal("GetSeries(id1) not found")
	}
	if series.Hash() != s1.Hash() {
		t.Errorf("GetSeries(id1) hash = %d, want %d", series.Hash(), s1.Hash())
	}

	// Get non-existent series
	if _, ok := r.GetSeries(99999); ok {
		t.Error("GetSeries(99999) expected not found, got found")
	}
}

func TestRegistry_Delete(t *testing.T) {
	r := NewRegistry(RegistryConfig{})

	s1 := NewSeries(map[string]string{"host": "server1"})
	s2 := NewSeries(map[string]string{"host": "server2"})

	id1, _ := r.GetOrCreate(s1)
	id2, _ := r.GetOrCreate(s2)

	if r.Cardinality() != 2 {
		t.Fatalf("Cardinality before delete = %d, want 2", r.Cardinality())
	}

	// Delete first series
	r.Delete(id1)

	if r.Cardinality() != 1 {
		t.Errorf("Cardinality after delete = %d, want 1", r.Cardinality())
	}

	// Verify s1 is deleted
	if _, ok := r.Get(s1.Hash()); ok {
		t.Error("Get(s1.Hash()) found after delete, want not found")
	}

	// Verify s2 still exists
	if _, ok := r.Get(s2.Hash()); !ok {
		t.Error("Get(s2.Hash()) not found, want found")
	}

	// Delete non-existent series (should not panic)
	r.Delete(99999)

	// Delete again (should not panic)
	r.Delete(id2)
	if r.Cardinality() != 0 {
		t.Errorf("Cardinality after all deletes = %d, want 0", r.Cardinality())
	}
}

func TestRegistry_Stats(t *testing.T) {
	r := NewRegistry(RegistryConfig{MaxCardinality: 100, LRUSize: 10})

	s1 := NewSeries(map[string]string{"id": "1"})
	s2 := NewSeries(map[string]string{"id": "2"})

	// Initial stats
	stats := r.Stats()
	if stats.Cardinality != 0 {
		t.Errorf("initial Cardinality = %d, want 0", stats.Cardinality)
	}
	if stats.MaxCardinality != 100 {
		t.Errorf("MaxCardinality = %d, want 100", stats.MaxCardinality)
	}

	// Create some series
	id1, _ := r.GetOrCreate(s1)
	id2, _ := r.GetOrCreate(s2)

	stats = r.Stats()
	if stats.Cardinality != 2 {
		t.Errorf("Cardinality after creates = %d, want 2", stats.Cardinality)
	}
	if stats.TotalCreated != 2 {
		t.Errorf("TotalCreated = %d, want 2", stats.TotalCreated)
	}
	if stats.TotalDeleted != 0 {
		t.Errorf("TotalDeleted = %d, want 0", stats.TotalDeleted)
	}

	// Delete one
	r.Delete(id1)

	stats = r.Stats()
	if stats.Cardinality != 1 {
		t.Errorf("Cardinality after delete = %d, want 1", stats.Cardinality)
	}
	if stats.TotalDeleted != 1 {
		t.Errorf("TotalDeleted = %d, want 1", stats.TotalDeleted)
	}
	if stats.ChurnRate != 0.5 {
		t.Errorf("ChurnRate = %f, want 0.5", stats.ChurnRate)
	}

	// Test LRU stats
	r.Get(s2.Hash()) // Hit (already in cache from GetOrCreate)
	stats = r.Stats()
	if stats.LRUHits == 0 {
		t.Error("LRUHits = 0, want > 0")
	}
	if stats.LRUHitRate == 0 {
		t.Error("LRUHitRate = 0, want > 0")
	}
}

func TestRegistry_MonotonicIDs(t *testing.T) {
	r := NewRegistry(RegistryConfig{})

	var ids []SeriesID
	for i := 0; i < 100; i++ {
		s := NewSeries(map[string]string{"id": fmt.Sprintf("%d", i)})
		id, err := r.GetOrCreate(s)
		if err != nil {
			t.Fatalf("GetOrCreate error at i=%d: %v", i, err)
		}
		ids = append(ids, id)
	}

	// Verify IDs are monotonically increasing
	for i := 1; i < len(ids); i++ {
		if ids[i] <= ids[i-1] {
			t.Errorf("IDs not monotonic: ids[%d]=%d, ids[%d]=%d", i-1, ids[i-1], i, ids[i])
		}
	}
}

func TestRegistry_Concurrent(t *testing.T) {
	r := NewRegistry(RegistryConfig{})

	const numGoroutines = 10
	const numOpsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Concurrent GetOrCreate
	for g := 0; g < numGoroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < numOpsPerGoroutine; i++ {
				s := NewSeries(map[string]string{
					"goroutine": fmt.Sprintf("%d", gid),
					"iter":      fmt.Sprintf("%d", i),
				})
				if _, err := r.GetOrCreate(s); err != nil {
					t.Errorf("GetOrCreate error: %v", err)
				}
			}
		}(g)
	}

	wg.Wait()

	// Verify final cardinality
	expectedCardinality := numGoroutines * numOpsPerGoroutine
	if got := r.Cardinality(); got != expectedCardinality {
		t.Errorf("Cardinality after concurrent ops = %d, want %d", got, expectedCardinality)
	}

	stats := r.Stats()
	if stats.TotalCreated != uint64(expectedCardinality) {
		t.Errorf("TotalCreated = %d, want %d", stats.TotalCreated, expectedCardinality)
	}
}

func TestRegistry_ConcurrentSameSeries(t *testing.T) {
	r := NewRegistry(RegistryConfig{})

	const numGoroutines = 100
	s := NewSeries(map[string]string{"host": "server1"})

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	ids := make([]SeriesID, numGoroutines)

	// All goroutines try to create the same series
	for g := 0; g < numGoroutines; g++ {
		go func(gid int) {
			defer wg.Done()
			id, err := r.GetOrCreate(s)
			if err != nil {
				t.Errorf("GetOrCreate error: %v", err)
			}
			ids[gid] = id
		}(g)
	}

	wg.Wait()

	// All should get the same ID
	firstID := ids[0]
	for i, id := range ids {
		if id != firstID {
			t.Errorf("ids[%d] = %d, want %d", i, id, firstID)
		}
	}

	// Cardinality should be 1
	if got := r.Cardinality(); got != 1 {
		t.Errorf("Cardinality = %d, want 1", got)
	}
}

func TestLRUCache_Basic(t *testing.T) {
	cache := newLRUCache(3)

	// Test Put and Get
	cache.Put(1, 100)
	cache.Put(2, 200)
	cache.Put(3, 300)

	if id, ok := cache.Get(1); !ok || id != 100 {
		t.Errorf("Get(1) = (%d, %v), want (100, true)", id, ok)
	}
	if id, ok := cache.Get(2); !ok || id != 200 {
		t.Errorf("Get(2) = (%d, %v), want (200, true)", id, ok)
	}

	// Add one more (should evict LRU)
	cache.Put(4, 400)

	// Key 3 should be evicted (it was least recently used)
	if _, ok := cache.Get(3); ok {
		t.Error("Get(3) found, expected evicted")
	}

	// Keys 1, 2, 4 should still exist
	if _, ok := cache.Get(1); !ok {
		t.Error("Get(1) not found, expected found")
	}
	if _, ok := cache.Get(2); !ok {
		t.Error("Get(2) not found, expected found")
	}
	if _, ok := cache.Get(4); !ok {
		t.Error("Get(4) not found, expected found")
	}
}

func TestLRUCache_UpdateExisting(t *testing.T) {
	cache := newLRUCache(2)

	cache.Put(1, 100)
	cache.Put(2, 200)

	// Update existing key
	cache.Put(1, 150)

	if id, ok := cache.Get(1); !ok || id != 150 {
		t.Errorf("Get(1) after update = (%d, %v), want (150, true)", id, ok)
	}

	// Add new key (should evict 2, not 1, because 1 was just updated)
	cache.Put(3, 300)

	if _, ok := cache.Get(2); ok {
		t.Error("Get(2) found, expected evicted")
	}
	if _, ok := cache.Get(1); !ok {
		t.Error("Get(1) not found, expected found")
	}
}

func TestLRUCache_Delete(t *testing.T) {
	cache := newLRUCache(3)

	cache.Put(1, 100)
	cache.Put(2, 200)
	cache.Put(3, 300)

	cache.Delete(2)

	if _, ok := cache.Get(2); ok {
		t.Error("Get(2) found after delete, expected not found")
	}

	// Delete non-existent (should not panic)
	cache.Delete(999)
}

func TestLRUCache_MoveToFront(t *testing.T) {
	cache := newLRUCache(3)

	cache.Put(1, 100)
	cache.Put(2, 200)
	cache.Put(3, 300)

	// Access key 1 (moves to front)
	cache.Get(1)

	// Add new key (should evict 2, the new LRU)
	cache.Put(4, 400)

	if _, ok := cache.Get(2); ok {
		t.Error("Get(2) found, expected evicted")
	}
	if _, ok := cache.Get(1); !ok {
		t.Error("Get(1) not found, expected found (was moved to front)")
	}
}
