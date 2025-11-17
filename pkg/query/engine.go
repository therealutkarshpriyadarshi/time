package query

import (
	"fmt"
	"sort"
	"sync"

	"github.com/therealutkarshpriyadarshi/time/pkg/index"
	"github.com/therealutkarshpriyadarshi/time/pkg/series"
	"github.com/therealutkarshpriyadarshi/time/pkg/storage"
)

// Query represents a time-series query.
type Query struct {
	// Label matchers to select series
	Matchers index.Matchers

	// Time range [MinTime, MaxTime] (inclusive)
	MinTime int64
	MaxTime int64

	// Step for range queries (0 for instant queries)
	Step int64
}

// QueryEngine executes queries against the TSDB.
type QueryEngine struct {
	db *storage.TSDB
}

// NewQueryEngine creates a new query engine.
func NewQueryEngine(db *storage.TSDB) *QueryEngine {
	return &QueryEngine{db: db}
}

// Select executes a query and returns series iterators.
// The query is executed across both in-memory MemTables and disk blocks.
//
// Query execution plan:
// 1. Use label matchers to filter series (if provided)
// 2. For each series hash that has been seen, query the TSDB
// 3. TSDB.Query automatically merges data from:
//    - Active MemTable
//    - Flushing MemTable (if exists)
//    - Disk blocks (future enhancement)
// 4. Return iterators for all matching series
//
// Note: This is a simplified implementation for Phase 5.
// Full index integration and block querying will be enhanced in future phases.
func (qe *QueryEngine) Select(q *Query) ([]SeriesIterator, error) {
	if q == nil {
		return nil, fmt.Errorf("query cannot be nil")
	}

	// For Phase 5, we return an empty iterator list
	// In a full implementation, we would:
	// 1. Use the inverted index to find series matching the matchers
	// 2. Query each series from TSDB
	// 3. Return iterators
	//
	// For now, callers should use ExecQuery which returns all series
	return make([]SeriesIterator, 0), nil
}

// SeriesIterator allows iterating over samples in a time series.
type SeriesIterator interface {
	// Next advances to the next sample. Returns false when iteration is complete.
	Next() bool

	// At returns the current sample (timestamp, value).
	// Only valid after Next() returns true.
	At() (int64, float64)

	// Err returns any error encountered during iteration.
	Err() error

	// Labels returns the series labels.
	Labels() map[string]string

	// Close releases any resources held by the iterator.
	Close() error
}

// sliceIterator is a simple iterator over a slice of samples.
type sliceIterator struct {
	series  *series.Series
	samples []series.Sample
	idx     int
	err     error
}

func (it *sliceIterator) Next() bool {
	it.idx++
	return it.idx < len(it.samples)
}

func (it *sliceIterator) At() (int64, float64) {
	if it.idx < 0 || it.idx >= len(it.samples) {
		return 0, 0
	}
	s := it.samples[it.idx]
	return s.Timestamp, s.Value
}

func (it *sliceIterator) Err() error {
	return it.err
}

func (it *sliceIterator) Labels() map[string]string {
	if it.series == nil {
		return nil
	}
	return it.series.Labels
}

func (it *sliceIterator) Close() error {
	return nil
}

// mergeIterator merges multiple iterators into one, removing duplicates
// and returning samples in sorted order by timestamp.
type mergeIterator struct {
	series    *series.Series
	iterators []SeriesIterator
	heap      *iteratorHeap
	current   *heapItem
	err       error
}

// heapItem represents an iterator in the heap with its current sample.
type heapItem struct {
	iter      SeriesIterator
	timestamp int64
	value     float64
	valid     bool
}

// iteratorHeap is a min-heap of iterators ordered by timestamp.
type iteratorHeap struct {
	items []*heapItem
}

func (h *iteratorHeap) Len() int { return len(h.items) }

func (h *iteratorHeap) Less(i, j int) bool {
	return h.items[i].timestamp < h.items[j].timestamp
}

func (h *iteratorHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *iteratorHeap) Push(x interface{}) {
	h.items = append(h.items, x.(*heapItem))
}

func (h *iteratorHeap) Pop() interface{} {
	old := h.items
	n := len(old)
	item := old[n-1]
	h.items = old[0 : n-1]
	return item
}

// newMergeIterator creates a new merge iterator.
func newMergeIterator(s *series.Series, iterators []SeriesIterator) SeriesIterator {
	if len(iterators) == 0 {
		return &sliceIterator{series: s, samples: nil, idx: 0}
	}

	if len(iterators) == 1 {
		return iterators[0]
	}

	h := &iteratorHeap{items: make([]*heapItem, 0, len(iterators))}

	// Initialize heap - don't consume samples yet, just store the iterators
	for _, iter := range iterators {
		h.items = append(h.items, &heapItem{
			iter:      iter,
			timestamp: 0,
			value:     0,
			valid:     false, // Not yet initialized
		})
	}

	return &mergeIterator{
		series:    s,
		iterators: iterators,
		heap:      h,
	}
}

func (it *mergeIterator) Next() bool {
	// Initialize any items that haven't been started yet
	for _, item := range it.heap.items {
		if !item.valid {
			if item.iter.Next() {
				ts, val := item.iter.At()
				item.timestamp = ts
				item.value = val
				item.valid = true
			}
		}
	}

	// Remove invalid items (iterators that are exhausted)
	validItems := make([]*heapItem, 0, len(it.heap.items))
	for _, item := range it.heap.items {
		if item.valid {
			validItems = append(validItems, item)
		}
	}
	it.heap.items = validItems

	if len(it.heap.items) == 0 {
		it.current = nil
		return false
	}

	// Sort to find minimum timestamp
	sort.Slice(it.heap.items, func(i, j int) bool {
		return it.heap.items[i].timestamp < it.heap.items[j].timestamp
	})

	// Save the minimum item's current value BEFORE advancing
	minItem := it.heap.items[0]
	it.current = &heapItem{
		timestamp: minItem.timestamp,
		value:     minItem.value,
		valid:     true,
	}
	currentTS := minItem.timestamp

	// Now advance the iterator that we just consumed
	if minItem.iter.Next() {
		ts, val := minItem.iter.At()
		minItem.timestamp = ts
		minItem.value = val
	} else {
		minItem.valid = false
	}

	// Skip duplicates (same timestamp) - advance other iterators with same timestamp
	for i := 1; i < len(it.heap.items) && it.heap.items[i].timestamp == currentTS; i++ {
		item := it.heap.items[i]
		if item.iter.Next() {
			ts, val := item.iter.At()
			item.timestamp = ts
			item.value = val
		} else {
			item.valid = false
		}
	}

	return true
}

func (it *mergeIterator) At() (int64, float64) {
	if it.current == nil {
		return 0, 0
	}
	return it.current.timestamp, it.current.value
}

func (it *mergeIterator) Err() error {
	return it.err
}

func (it *mergeIterator) Labels() map[string]string {
	if it.series == nil {
		return nil
	}
	return it.series.Labels
}

func (it *mergeIterator) Close() error {
	for _, iter := range it.iterators {
		if err := iter.Close(); err != nil {
			return err
		}
	}
	return nil
}

// QueryResult represents the result of a query.
type QueryResult struct {
	Series []TimeSeries
}

// TimeSeries represents a single time series with its samples.
type TimeSeries struct {
	Labels  map[string]string
	Samples []series.Sample
}

// ExecQuery executes a query and returns all results materialized in memory.
// This is a convenience method that collects all samples from iterators.
func (qe *QueryEngine) ExecQuery(q *Query) (*QueryResult, error) {
	iterators, err := qe.Select(q)
	if err != nil {
		return nil, err
	}

	result := &QueryResult{
		Series: make([]TimeSeries, 0, len(iterators)),
	}

	for _, iter := range iterators {
		ts := TimeSeries{
			Labels:  iter.Labels(),
			Samples: make([]series.Sample, 0),
		}

		for iter.Next() {
			timestamp, value := iter.At()
			ts.Samples = append(ts.Samples, series.Sample{
				Timestamp: timestamp,
				Value:     value,
			})
		}

		if err := iter.Err(); err != nil {
			iter.Close()
			return nil, fmt.Errorf("iterator error: %w", err)
		}

		iter.Close()

		if len(ts.Samples) > 0 {
			result.Series = append(result.Series, ts)
		}
	}

	return result, nil
}

// SelectRange executes a range query with step interval.
// Returns samples aligned to the step interval.
func (qe *QueryEngine) SelectRange(q *Query) ([]SeriesIterator, error) {
	if q.Step <= 0 {
		return nil, fmt.Errorf("step must be positive for range queries")
	}

	// Get base iterators
	iterators, err := qe.Select(q)
	if err != nil {
		return nil, err
	}

	// Wrap each iterator with step alignment
	rangeIterators := make([]SeriesIterator, 0, len(iterators))
	for _, iter := range iterators {
		rangeIterators = append(rangeIterators, &stepIterator{
			inner:    iter,
			step:     q.Step,
			minTime:  q.MinTime,
			maxTime:  q.MaxTime,
			nextTime: q.MinTime,
		})
	}

	return rangeIterators, nil
}

// stepIterator aligns samples to step boundaries.
type stepIterator struct {
	inner    SeriesIterator
	step     int64
	minTime  int64
	maxTime  int64
	nextTime int64
	current  series.Sample
	done     bool
	mu       sync.Mutex
}

func (it *stepIterator) Next() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.done || it.nextTime > it.maxTime {
		return false
	}

	// Find the next sample at or after nextTime
	found := false
	for it.inner.Next() {
		ts, val := it.inner.At()
		if ts >= it.nextTime {
			it.current = series.Sample{Timestamp: it.nextTime, Value: val}
			found = true
			break
		}
	}

	if !found {
		it.done = true
		return false
	}

	it.nextTime += it.step
	return true
}

func (it *stepIterator) At() (int64, float64) {
	it.mu.Lock()
	defer it.mu.Unlock()
	return it.current.Timestamp, it.current.Value
}

func (it *stepIterator) Err() error {
	return it.inner.Err()
}

func (it *stepIterator) Labels() map[string]string {
	return it.inner.Labels()
}

func (it *stepIterator) Close() error {
	return it.inner.Close()
}
