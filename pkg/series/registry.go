package series

import (
	"fmt"
	"sync"
	"sync/atomic"
)

// SeriesID is a unique identifier for a time series.
// It's a monotonically increasing integer assigned to each unique series.
type SeriesID uint64

const (
	// MaxSeriesID is the maximum number of series that can be tracked.
	// This limits memory usage and provides a reasonable upper bound.
	MaxSeriesID SeriesID = 1<<32 - 1 // ~4.3 billion series

	// DefaultLRUSize is the default size of the LRU cache for series lookups.
	// This cache speeds up repeated lookups of the same series.
	DefaultLRUSize = 100_000
)

// Registry manages the mapping between series (identified by hash) and monotonic series IDs.
// It provides:
// - Monotonic ID allocation for new series
// - Fast hash -> ID lookups with LRU caching
// - Cardinality tracking and limits
// - Churn rate monitoring
type Registry struct {
	mu sync.RWMutex

	// nextID is the next series ID to allocate (monotonically increasing)
	nextID atomic.Uint64

	// hashToID maps series hash to series ID
	hashToID map[uint64]SeriesID

	// idToSeries maps series ID to the actual series metadata
	idToSeries map[SeriesID]*Series

	// lru is a simple LRU cache for frequently accessed series lookups
	lru      *lruCache
	lruSize  int
	lruHits  atomic.Uint64
	lruMiss  atomic.Uint64

	// cardinality tracking
	maxCardinality uint64
	totalCreated   atomic.Uint64 // total series ever created
	totalDeleted   atomic.Uint64 // total series deleted (for churn tracking)
}

// RegistryConfig holds configuration for creating a new Registry.
type RegistryConfig struct {
	// MaxCardinality is the maximum number of active series allowed.
	// If 0, defaults to MaxSeriesID.
	MaxCardinality uint64

	// LRUSize is the size of the LRU cache for series lookups.
	// If 0, defaults to DefaultLRUSize.
	LRUSize int
}

// NewRegistry creates a new series ID registry with the given configuration.
func NewRegistry(cfg RegistryConfig) *Registry {
	if cfg.MaxCardinality == 0 {
		cfg.MaxCardinality = uint64(MaxSeriesID)
	}
	if cfg.LRUSize == 0 {
		cfg.LRUSize = DefaultLRUSize
	}

	r := &Registry{
		hashToID:       make(map[uint64]SeriesID),
		idToSeries:     make(map[SeriesID]*Series),
		lru:            newLRUCache(cfg.LRUSize),
		lruSize:        cfg.LRUSize,
		maxCardinality: cfg.MaxCardinality,
	}
	r.nextID.Store(1) // Start IDs from 1 (0 is reserved for "not found")
	return r
}

// GetOrCreate returns the series ID for the given series, creating a new ID if necessary.
// If the series already exists, it returns the existing ID.
// If the series is new and cardinality limit is reached, it returns an error.
func (r *Registry) GetOrCreate(s *Series) (SeriesID, error) {
	if s == nil {
		return 0, fmt.Errorf("series cannot be nil")
	}

	hash := s.Hash

	// Fast path: check LRU cache first (no lock needed)
	if id, ok := r.lru.Get(hash); ok {
		r.lruHits.Add(1)
		return id, nil
	}
	r.lruMiss.Add(1)

	// Check if series exists (read lock)
	r.mu.RLock()
	if id, exists := r.hashToID[hash]; exists {
		r.mu.RUnlock()
		r.lru.Put(hash, id) // update LRU cache
		return id, nil
	}
	r.mu.RUnlock()

	// Series doesn't exist, need to create it (write lock)
	r.mu.Lock()
	defer r.mu.Unlock()

	// Double-check after acquiring write lock (another goroutine may have created it)
	if id, exists := r.hashToID[hash]; exists {
		r.lru.Put(hash, id)
		return id, nil
	}

	// Check cardinality limit
	if uint64(len(r.hashToID)) >= r.maxCardinality {
		return 0, fmt.Errorf("max cardinality reached: %d", r.maxCardinality)
	}

	// Allocate new ID
	newID := SeriesID(r.nextID.Add(1) - 1)
	if newID > MaxSeriesID {
		return 0, fmt.Errorf("max series ID exceeded: %d", MaxSeriesID)
	}

	// Store mappings
	r.hashToID[hash] = newID
	r.idToSeries[newID] = s
	r.lru.Put(hash, newID)
	r.totalCreated.Add(1)

	return newID, nil
}

// Get returns the series ID for the given series hash, or 0 if not found.
func (r *Registry) Get(hash uint64) (SeriesID, bool) {
	// Fast path: check LRU cache first
	if id, ok := r.lru.Get(hash); ok {
		r.lruHits.Add(1)
		return id, true
	}
	r.lruMiss.Add(1)

	// Slow path: check main index
	r.mu.RLock()
	id, exists := r.hashToID[hash]
	r.mu.RUnlock()

	if exists {
		r.lru.Put(hash, id)
	}
	return id, exists
}

// GetSeries returns the series metadata for the given series ID.
func (r *Registry) GetSeries(id SeriesID) (*Series, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.idToSeries[id]
	return s, ok
}

// Delete removes a series from the registry by its ID.
// This is used for series eviction/cleanup.
func (r *Registry) Delete(id SeriesID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if s, exists := r.idToSeries[id]; exists {
		hash := s.Hash
		delete(r.hashToID, hash)
		delete(r.idToSeries, id)
		r.lru.Delete(hash)
		r.totalDeleted.Add(1)
	}
}

// Cardinality returns the current number of active series in the registry.
func (r *Registry) Cardinality() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.hashToID)
}

// Stats returns statistics about the registry.
type RegistryStats struct {
	Cardinality    int     // current active series count
	MaxCardinality uint64  // maximum allowed series
	TotalCreated   uint64  // total series ever created
	TotalDeleted   uint64  // total series deleted
	ChurnRate      float64 // deletion rate (deleted / created)
	LRUHits        uint64  // LRU cache hits
	LRUMiss        uint64  // LRU cache misses
	LRUHitRate     float64 // cache hit rate (hits / total lookups)
}

// Stats returns current registry statistics.
func (r *Registry) Stats() RegistryStats {
	r.mu.RLock()
	cardinality := len(r.hashToID)
	r.mu.RUnlock()

	created := r.totalCreated.Load()
	deleted := r.totalDeleted.Load()
	hits := r.lruHits.Load()
	miss := r.lruMiss.Load()

	var churnRate float64
	if created > 0 {
		churnRate = float64(deleted) / float64(created)
	}

	var hitRate float64
	total := hits + miss
	if total > 0 {
		hitRate = float64(hits) / float64(total)
	}

	return RegistryStats{
		Cardinality:    cardinality,
		MaxCardinality: r.maxCardinality,
		TotalCreated:   created,
		TotalDeleted:   deleted,
		ChurnRate:      churnRate,
		LRUHits:        hits,
		LRUMiss:        miss,
		LRUHitRate:     hitRate,
	}
}

// lruCache is a simple LRU cache using a map and a doubly-linked list.
type lruCache struct {
	mu       sync.RWMutex
	capacity int
	items    map[uint64]*lruNode
	head     *lruNode // most recently used
	tail     *lruNode // least recently used
}

type lruNode struct {
	hash uint64
	id   SeriesID
	prev *lruNode
	next *lruNode
}

func newLRUCache(capacity int) *lruCache {
	return &lruCache{
		capacity: capacity,
		items:    make(map[uint64]*lruNode),
	}
}

func (c *lruCache) Get(hash uint64) (SeriesID, bool) {
	c.mu.RLock()
	node, ok := c.items[hash]
	c.mu.RUnlock()

	if !ok {
		return 0, false
	}

	// Move to front (most recently used)
	c.mu.Lock()
	c.moveToFront(node)
	c.mu.Unlock()

	return node.id, true
}

func (c *lruCache) Put(hash uint64, id SeriesID) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If already exists, update and move to front
	if node, exists := c.items[hash]; exists {
		node.id = id
		c.moveToFront(node)
		return
	}

	// Create new node
	node := &lruNode{hash: hash, id: id}
	c.items[hash] = node

	// Add to front
	if c.head == nil {
		c.head = node
		c.tail = node
	} else {
		node.next = c.head
		c.head.prev = node
		c.head = node
	}

	// Evict LRU if over capacity
	if len(c.items) > c.capacity {
		c.evictLRU()
	}
}

func (c *lruCache) Delete(hash uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.items[hash]
	if !ok {
		return
	}

	c.removeNode(node)
	delete(c.items, hash)
}

func (c *lruCache) moveToFront(node *lruNode) {
	if node == c.head {
		return
	}

	// Remove from current position
	if node.prev != nil {
		node.prev.next = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	}
	if node == c.tail {
		c.tail = node.prev
	}

	// Move to front
	node.prev = nil
	node.next = c.head
	if c.head != nil {
		c.head.prev = node
	}
	c.head = node
}

func (c *lruCache) removeNode(node *lruNode) {
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		c.head = node.next
	}

	if node.next != nil {
		node.next.prev = node.prev
	} else {
		c.tail = node.prev
	}
}

func (c *lruCache) evictLRU() {
	if c.tail == nil {
		return
	}

	delete(c.items, c.tail.hash)
	c.removeNode(c.tail)
}
