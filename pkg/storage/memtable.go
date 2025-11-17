package storage

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

var (
	// ErrMemTableFull indicates the MemTable has reached its size limit
	ErrMemTableFull = errors.New("memtable is full")

	// ErrInvalidSample indicates the sample data is invalid
	ErrInvalidSample = errors.New("invalid sample")
)

const (
	// DefaultMaxSize is the default maximum size in bytes (256MB)
	DefaultMaxSize = 256 * 1024 * 1024

	// EstimatedBytesPerSample is an estimate of memory usage per sample
	EstimatedBytesPerSample = 24 // 8 bytes timestamp + 8 bytes value + ~8 bytes overhead
)

// MemTable is an in-memory buffer for time-series samples.
// It provides thread-safe operations for inserting and querying samples.
// When the MemTable reaches its size threshold, it should be flushed to disk.
type MemTable struct {
	// series maps seriesHash -> samples
	series map[uint64][]series.Sample

	// seriesMeta maps seriesHash -> Series metadata
	seriesMeta map[uint64]*series.Series

	// size tracks the approximate memory usage in bytes
	size int64

	// maxSize is the size threshold for triggering a flush
	maxSize int64

	// createdAt tracks when this MemTable was created
	createdAt time.Time

	// minTime and maxTime track the time range of samples
	minTime int64
	maxTime int64

	// mu protects all fields
	mu sync.RWMutex
}

// NewMemTable creates a new MemTable with the default maximum size.
func NewMemTable() *MemTable {
	return NewMemTableWithSize(DefaultMaxSize)
}

// NewMemTableWithSize creates a new MemTable with a custom maximum size.
func NewMemTableWithSize(maxSize int64) *MemTable {
	return &MemTable{
		series:     make(map[uint64][]series.Sample),
		seriesMeta: make(map[uint64]*series.Series),
		maxSize:    maxSize,
		createdAt:  time.Now(),
		minTime:    -1,
		maxTime:    -1,
	}
}

// Insert adds samples for a given series to the MemTable.
// Returns an error if the MemTable is full or if the input is invalid.
func (m *MemTable) Insert(s *series.Series, samples []series.Sample) error {
	if s == nil || len(samples) == 0 {
		return ErrInvalidSample
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Check if we have space
	estimatedSize := int64(len(samples)) * EstimatedBytesPerSample
	if m.size+estimatedSize > m.maxSize {
		return ErrMemTableFull
	}

	// Store series metadata if not already present
	if _, exists := m.seriesMeta[s.Hash]; !exists {
		m.seriesMeta[s.Hash] = s.Clone()
		// Add estimated size for series metadata
		for k, v := range s.Labels {
			m.size += int64(len(k) + len(v) + 16) // rough estimate
		}
	}

	// Get existing samples or create new slice
	existingSamples := m.series[s.Hash]

	// Append new samples
	m.series[s.Hash] = append(existingSamples, samples...)
	m.size += estimatedSize

	// Update time range
	for _, sample := range samples {
		if m.minTime == -1 || sample.Timestamp < m.minTime {
			m.minTime = sample.Timestamp
		}
		if m.maxTime == -1 || sample.Timestamp > m.maxTime {
			m.maxTime = sample.Timestamp
		}
	}

	return nil
}

// Query retrieves samples for a given series hash within a time range.
// Returns all samples if start and end are both 0.
func (m *MemTable) Query(seriesHash uint64, start, end int64) ([]series.Sample, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	samples, exists := m.series[seriesHash]
	if !exists {
		return nil, nil // No error, just no data
	}

	// If no time range specified, return all samples
	if start == 0 && end == 0 {
		result := make([]series.Sample, len(samples))
		copy(result, samples)
		return result, nil
	}

	// Filter by time range
	result := make([]series.Sample, 0, len(samples))
	for _, sample := range samples {
		if sample.Timestamp >= start && sample.Timestamp <= end {
			result = append(result, sample)
		}
	}

	return result, nil
}

// GetSeries retrieves the series metadata for a given hash.
func (m *MemTable) GetSeries(seriesHash uint64) (*series.Series, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	s, exists := m.seriesMeta[seriesHash]
	if !exists {
		return nil, false
	}
	return s.Clone(), true
}

// Size returns the current size of the MemTable in bytes.
func (m *MemTable) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size
}

// MaxSize returns the maximum size threshold.
func (m *MemTable) MaxSize() int64 {
	return m.maxSize
}

// IsFull returns true if the MemTable has reached its size threshold.
func (m *MemTable) IsFull() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.size >= m.maxSize
}

// SeriesCount returns the number of unique series in the MemTable.
func (m *MemTable) SeriesCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.series)
}

// SampleCount returns the total number of samples in the MemTable.
func (m *MemTable) SampleCount() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var count int64
	for _, samples := range m.series {
		count += int64(len(samples))
	}
	return count
}

// TimeRange returns the minimum and maximum timestamps in the MemTable.
func (m *MemTable) TimeRange() (int64, int64) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.minTime, m.maxTime
}

// CreatedAt returns when this MemTable was created.
func (m *MemTable) CreatedAt() time.Time {
	return m.createdAt
}

// AllSeries returns all series hashes in the MemTable.
func (m *MemTable) AllSeries() []uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()

	hashes := make([]uint64, 0, len(m.series))
	for hash := range m.series {
		hashes = append(hashes, hash)
	}
	return hashes
}

// Stats returns statistics about the MemTable.
func (m *MemTable) Stats() string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return fmt.Sprintf("MemTable{series: %d, samples: %d, size: %d/%d bytes (%.1f%%), timeRange: [%d, %d]}",
		len(m.series),
		m.SampleCount(),
		m.size,
		m.maxSize,
		float64(m.size)/float64(m.maxSize)*100,
		m.minTime,
		m.maxTime,
	)
}

// Clear removes all data from the MemTable and resets its state.
// This is typically called after a successful flush to disk.
func (m *MemTable) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.series = make(map[uint64][]series.Sample)
	m.seriesMeta = make(map[uint64]*series.Series)
	m.size = 0
	m.minTime = -1
	m.maxTime = -1
	m.createdAt = time.Now()
}
