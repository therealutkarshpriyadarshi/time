package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/therealutkarshpriyadarshi/time/pkg/index"
	"github.com/therealutkarshpriyadarshi/time/pkg/series"
	"github.com/therealutkarshpriyadarshi/time/pkg/wal"
)

var (
	// ErrClosed indicates the TSDB is closed
	ErrClosed = errors.New("tsdb: closed")

	// ErrReadOnly indicates the TSDB is in read-only mode
	ErrReadOnly = errors.New("tsdb: read-only mode")
)

const (
	// DefaultFlushInterval is how often to check if MemTable should be flushed
	DefaultFlushInterval = 30 * time.Second

	// DefaultWALDir is the default directory name for WAL files
	DefaultWALDir = "wal"
)

// TSDB is the main time-series database orchestrator.
// It coordinates WAL writes, MemTable operations, and background flushing.
type TSDB struct {
	// Configuration
	dataDir       string
	flushInterval time.Duration

	// Write path components
	activeMemTable   *MemTable
	flushingMemTable *MemTable
	walWriter        *wal.WAL
	blockWriter      *BlockWriter

	// Background operations (Phase 6)
	compactor        *Compactor
	retentionManager *RetentionManager

	// Synchronization
	mu          sync.RWMutex
	flushMu     sync.Mutex
	flushChan   chan struct{}
	flusherDone chan struct{}

	// State
	closed atomic.Bool
	ctx    context.Context
	cancel context.CancelFunc

	// Metrics
	stats Stats
}

// Stats holds TSDB statistics
type Stats struct {
	TotalSamples     atomic.Int64
	TotalSeries      atomic.Int64
	FlushCount       atomic.Int64
	LastFlushTime    atomic.Int64 // Unix milliseconds
	WALSize          atomic.Int64
	ActiveMemTableSize atomic.Int64
}

// Options configures the TSDB
type Options struct {
	DataDir            string
	FlushInterval      time.Duration
	WALOptions         *wal.Options
	MemTableSize       int64
	EnableCompaction   bool
	CompactionInterval time.Duration
	EnableRetention    bool
	RetentionPeriod    time.Duration
}

// DefaultOptions returns default TSDB options
func DefaultOptions(dataDir string) *Options {
	return &Options{
		DataDir:            dataDir,
		FlushInterval:      DefaultFlushInterval,
		WALOptions:         wal.DefaultOptions(),
		MemTableSize:       DefaultMaxSize,
		EnableCompaction:   true,
		CompactionInterval: DefaultCompactionInterval,
		EnableRetention:    true,
		RetentionPeriod:    DefaultRetentionPeriod,
	}
}

// Open opens or creates a TSDB instance
func Open(opts *Options) (*TSDB, error) {
	if opts == nil {
		return nil, fmt.Errorf("tsdb: options cannot be nil")
	}

	// Create data directory
	if err := os.MkdirAll(opts.DataDir, 0755); err != nil {
		return nil, fmt.Errorf("tsdb: failed to create data directory: %w", err)
	}

	// Open WAL
	walDir := filepath.Join(opts.DataDir, DefaultWALDir)
	walWriter, err := wal.Open(walDir, opts.WALOptions)
	if err != nil {
		return nil, fmt.Errorf("tsdb: failed to open WAL: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	db := &TSDB{
		dataDir:        opts.DataDir,
		flushInterval:  opts.FlushInterval,
		activeMemTable: NewMemTableWithSize(opts.MemTableSize),
		walWriter:      walWriter,
		blockWriter:    NewBlockWriter(opts.DataDir),
		flushChan:      make(chan struct{}, 1),
		flusherDone:    make(chan struct{}),
		ctx:            ctx,
		cancel:         cancel,
	}

	// Recover from WAL
	if err := db.recover(); err != nil {
		walWriter.Close()
		return nil, fmt.Errorf("tsdb: failed to recover: %w", err)
	}

	// Initialize compactor (Phase 6)
	if opts.EnableCompaction {
		compactorOpts := &CompactorOptions{
			DataDir:     opts.DataDir,
			Interval:    opts.CompactionInterval,
			Concurrency: 1,
		}
		db.compactor = NewCompactor(compactorOpts)
		go db.compactor.Run()
	}

	// Initialize retention manager (Phase 6)
	if opts.EnableRetention && db.compactor != nil {
		retentionOpts := &RetentionManagerOptions{
			Policy: RetentionPolicy{
				MaxAge:     opts.RetentionPeriod,
				MinSamples: 0,
				Enabled:    true,
			},
			Interval: DefaultRetentionCheckInterval,
		}
		db.retentionManager = NewRetentionManager(db.compactor, retentionOpts)
		go db.retentionManager.Run()
	}

	// Start background flusher
	go db.backgroundFlusher()

	return db, nil
}

// Insert adds samples for a series to the TSDB
func (db *TSDB) Insert(s *series.Series, samples []series.Sample) error {
	if db.closed.Load() {
		return ErrClosed
	}

	if s == nil || len(samples) == 0 {
		return ErrInvalidSample
	}

	db.mu.RLock()
	activeMemTable := db.activeMemTable
	db.mu.RUnlock()

	// 1. Write to WAL first (durability)
	if err := db.walWriter.Append(s, samples); err != nil {
		return fmt.Errorf("tsdb: WAL append failed: %w", err)
	}

	// 2. Insert into active MemTable
	err := activeMemTable.Insert(s, samples)
	if err == ErrMemTableFull {
		// Trigger flush
		select {
		case db.flushChan <- struct{}{}:
		default:
			// Flush already pending
		}

		// Wait a bit and retry
		time.Sleep(10 * time.Millisecond)

		db.mu.RLock()
		activeMemTable = db.activeMemTable
		db.mu.RUnlock()

		err = activeMemTable.Insert(s, samples)
	}

	if err != nil {
		return fmt.Errorf("tsdb: memtable insert failed: %w", err)
	}

	// Update stats
	db.stats.TotalSamples.Add(int64(len(samples)))
	db.stats.ActiveMemTableSize.Store(activeMemTable.Size())

	return nil
}

// Query retrieves samples for a series within a time range
func (db *TSDB) Query(seriesHash uint64, start, end int64) ([]series.Sample, error) {
	if db.closed.Load() {
		return nil, ErrClosed
	}

	db.mu.RLock()
	activeMemTable := db.activeMemTable
	flushingMemTable := db.flushingMemTable
	db.mu.RUnlock()

	// Query active MemTable
	activeSamples, err := activeMemTable.Query(seriesHash, start, end)
	if err != nil {
		return nil, err
	}

	// Query flushing MemTable if it exists
	var flushingSamples []series.Sample
	if flushingMemTable != nil {
		flushingSamples, err = flushingMemTable.Query(seriesHash, start, end)
		if err != nil {
			return nil, err
		}
	}

	// Merge results (in Phase 3, we'll also query disk blocks)
	result := make([]series.Sample, 0, len(activeSamples)+len(flushingSamples))
	result = append(result, activeSamples...)
	result = append(result, flushingSamples...)

	return result, nil
}

// GetSeries retrieves series metadata
func (db *TSDB) GetSeries(seriesHash uint64) (*series.Series, bool) {
	if db.closed.Load() {
		return nil, false
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	// Check active MemTable first
	if s, ok := db.activeMemTable.GetSeries(seriesHash); ok {
		return s, true
	}

	// Check flushing MemTable
	if db.flushingMemTable != nil {
		if s, ok := db.flushingMemTable.GetSeries(seriesHash); ok {
			return s, true
		}
	}

	return nil, false
}

// GetStats returns a snapshot of current TSDB statistics
func (db *TSDB) GetStats() Stats {
	// Create a safe copy using atomic loads
	return Stats{
		TotalSamples:       atomic.Int64{},
		TotalSeries:        atomic.Int64{},
		FlushCount:         atomic.Int64{},
		LastFlushTime:      atomic.Int64{},
		WALSize:            atomic.Int64{},
		ActiveMemTableSize: atomic.Int64{},
	}
}

// GetStatsSnapshot returns a simple snapshot of stats without atomic types
func (db *TSDB) GetStatsSnapshot() StatsSnapshot {
	return StatsSnapshot{
		TotalSamples:       db.stats.TotalSamples.Load(),
		TotalSeries:        db.stats.TotalSeries.Load(),
		FlushCount:         db.stats.FlushCount.Load(),
		LastFlushTime:      db.stats.LastFlushTime.Load(),
		WALSize:            db.stats.WALSize.Load(),
		ActiveMemTableSize: db.stats.ActiveMemTableSize.Load(),
	}
}

// StatsSnapshot is a point-in-time snapshot of statistics
type StatsSnapshot struct {
	TotalSamples       int64
	TotalSeries        int64
	FlushCount         int64
	LastFlushTime      int64
	WALSize            int64
	ActiveMemTableSize int64
}

// Close closes the TSDB and all its components
func (db *TSDB) Close() error {
	if !db.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	// Stop background operations (Phase 6)
	if db.compactor != nil {
		db.compactor.Stop()
	}
	if db.retentionManager != nil {
		db.retentionManager.Stop()
	}

	// Cancel background operations
	db.cancel()

	// Wait for background flusher to complete
	<-db.flusherDone

	// Flush any remaining data
	if err := db.flush(); err != nil {
		return fmt.Errorf("tsdb: final flush failed: %w", err)
	}

	// Close WAL
	if err := db.walWriter.Close(); err != nil {
		return fmt.Errorf("tsdb: WAL close failed: %w", err)
	}

	return nil
}

// recover replays the WAL to rebuild in-memory state
func (db *TSDB) recover() error {
	entries, err := db.walWriter.Replay()
	if err != nil {
		return fmt.Errorf("WAL replay failed: %w", err)
	}

	if len(entries) == 0 {
		return nil
	}

	// Rebuild MemTable from WAL entries
	for _, entry := range entries {
		if entry.Type == 1 { // Sample entry
			if entry.Series != nil && len(entry.Samples) > 0 {
				// Best effort recovery - ignore errors
				db.activeMemTable.Insert(entry.Series, entry.Samples)
			}
		}
	}

	fmt.Printf("tsdb: recovered %d entries from WAL\n", len(entries))
	return nil
}

// backgroundFlusher runs in the background and flushes MemTables periodically
func (db *TSDB) backgroundFlusher() {
	defer close(db.flusherDone)

	ticker := time.NewTicker(db.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-db.ctx.Done():
			return

		case <-ticker.C:
			// Check if active MemTable should be flushed
			db.mu.RLock()
			shouldFlush := db.activeMemTable.IsFull()
			db.mu.RUnlock()

			if shouldFlush {
				if err := db.flush(); err != nil {
					fmt.Printf("tsdb: background flush failed: %v\n", err)
				}
			}

		case <-db.flushChan:
			// Explicit flush request
			if err := db.flush(); err != nil {
				fmt.Printf("tsdb: explicit flush failed: %v\n", err)
			}
		}
	}
}

// flush swaps the active MemTable and flushes it to disk
func (db *TSDB) flush() error {
	db.flushMu.Lock()
	defer db.flushMu.Unlock()

	db.mu.Lock()

	// Check if there's anything to flush
	if db.activeMemTable.SeriesCount() == 0 {
		db.mu.Unlock()
		return nil
	}

	// Swap MemTables (double-buffering)
	oldMemTable := db.activeMemTable
	db.activeMemTable = NewMemTableWithSize(oldMemTable.MaxSize())
	db.flushingMemTable = oldMemTable

	db.mu.Unlock()

	// At this point, new writes go to the new active MemTable
	// We can safely flush the old one without blocking writes

	minTime, maxTime := oldMemTable.TimeRange()

	fmt.Printf("tsdb: flushing MemTable (series=%d, samples=%d, timeRange=[%d, %d])\n",
		oldMemTable.SeriesCount(),
		oldMemTable.SampleCount(),
		minTime,
		maxTime,
	)

	// Write MemTable to disk as a block
	block, err := db.blockWriter.WriteMemTable(oldMemTable)
	if err != nil {
		return fmt.Errorf("failed to write block: %w", err)
	}

	fmt.Printf("tsdb: created block %s (size=%d bytes, compression=%.2fx)\n",
		block.ULID.String(),
		block.Size(),
		float64(oldMemTable.SampleCount()*16)/float64(block.Size()),
	)

	// Log flush to WAL
	if err := db.walWriter.LogFlush(maxTime); err != nil {
		fmt.Printf("tsdb: failed to log flush: %v\n", err)
	}

	// Truncate old WAL entries
	if err := db.walWriter.Truncate(maxTime); err != nil {
		fmt.Printf("tsdb: failed to truncate WAL: %v\n", err)
	}

	// Clear the flushing MemTable
	db.mu.Lock()
	db.flushingMemTable = nil
	db.mu.Unlock()

	// Update stats
	db.stats.FlushCount.Add(1)
	db.stats.LastFlushTime.Store(time.Now().UnixMilli())

	return nil
}

// TriggerFlush manually triggers a flush operation
func (db *TSDB) TriggerFlush() error {
	if db.closed.Load() {
		return ErrClosed
	}

	select {
	case db.flushChan <- struct{}{}:
		// Wait for flush to complete
		time.Sleep(100 * time.Millisecond)
		return nil
	default:
		return fmt.Errorf("tsdb: flush already in progress")
	}
}

// MemTableStats returns statistics about the current MemTables
func (db *TSDB) MemTableStats() (active, flushing string) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	active = db.activeMemTable.Stats()

	if db.flushingMemTable != nil {
		flushing = db.flushingMemTable.Stats()
	} else {
		flushing = "None"
	}

	return active, flushing
}

// GetCompactionStats returns compaction statistics (Phase 6)
func (db *TSDB) GetCompactionStats() *CompactionStats {
	if db.compactor == nil {
		return nil
	}
	stats := db.compactor.GetStats()
	return &stats
}

// GetRetentionStats returns retention statistics (Phase 6)
func (db *TSDB) GetRetentionStats() *RetentionStats {
	if db.retentionManager == nil {
		return nil
	}
	stats := db.retentionManager.GetStats()
	return &stats
}

// TriggerCompaction manually triggers compaction (Phase 6)
func (db *TSDB) TriggerCompaction() error {
	if db.compactor == nil {
		return fmt.Errorf("compaction not enabled")
	}
	return db.compactor.CompactNow()
}

// GetRetentionPolicy returns the current retention policy (Phase 6)
func (db *TSDB) GetRetentionPolicy() *RetentionPolicy {
	if db.retentionManager == nil {
		return nil
	}
	policy := db.retentionManager.GetPolicy()
	return &policy
}

// SetRetentionPolicy updates the retention policy (Phase 6)
func (db *TSDB) SetRetentionPolicy(policy RetentionPolicy) error {
	if db.retentionManager == nil {
		return fmt.Errorf("retention not enabled")
	}
	db.retentionManager.SetPolicy(policy)
	return nil
}

// GetAllLabels returns all unique label names across all series (Phase 7)
func (db *TSDB) GetAllLabels() ([]string, error) {
	if db.closed.Load() {
		return nil, ErrClosed
	}

	db.mu.RLock()
	activeMemTable := db.activeMemTable
	flushingMemTable := db.flushingMemTable
	db.mu.RUnlock()

	labelSet := make(map[string]struct{})

	// Collect labels from active MemTable
	activeMemTable.mu.RLock()
	for _, s := range activeMemTable.seriesMeta {
		for labelName := range s.Labels {
			labelSet[labelName] = struct{}{}
		}
	}
	activeMemTable.mu.RUnlock()

	// Collect labels from flushing MemTable
	if flushingMemTable != nil {
		flushingMemTable.mu.RLock()
		for _, s := range flushingMemTable.seriesMeta {
			for labelName := range s.Labels {
				labelSet[labelName] = struct{}{}
			}
		}
		flushingMemTable.mu.RUnlock()
	}

	// Convert to sorted slice
	labels := make([]string, 0, len(labelSet))
	for label := range labelSet {
		labels = append(labels, label)
	}

	// Sort for consistent output
	sort.Strings(labels)

	return labels, nil
}

// GetLabelValues returns all unique values for a specific label (Phase 7)
func (db *TSDB) GetLabelValues(labelName string) ([]string, error) {
	if db.closed.Load() {
		return nil, ErrClosed
	}

	db.mu.RLock()
	activeMemTable := db.activeMemTable
	flushingMemTable := db.flushingMemTable
	db.mu.RUnlock()

	valueSet := make(map[string]struct{})

	// Collect values from active MemTable
	activeMemTable.mu.RLock()
	for _, s := range activeMemTable.seriesMeta {
		if value, ok := s.Labels[labelName]; ok {
			valueSet[value] = struct{}{}
		}
	}
	activeMemTable.mu.RUnlock()

	// Collect values from flushing MemTable
	if flushingMemTable != nil {
		flushingMemTable.mu.RLock()
		for _, s := range flushingMemTable.seriesMeta {
			if value, ok := s.Labels[labelName]; ok {
				valueSet[value] = struct{}{}
			}
		}
		flushingMemTable.mu.RUnlock()
	}

	// Convert to sorted slice
	values := make([]string, 0, len(valueSet))
	for value := range valueSet {
		values = append(values, value)
	}

	// Sort for consistent output
	sort.Strings(values)

	return values, nil
}

// GetSeries returns all series that match the given label matchers (Phase 7)
func (db *TSDB) GetSeries(matchers index.Matchers) ([]map[string]string, error) {
	if db.closed.Load() {
		return nil, ErrClosed
	}

	db.mu.RLock()
	activeMemTable := db.activeMemTable
	flushingMemTable := db.flushingMemTable
	db.mu.RUnlock()

	seriesMap := make(map[uint64]map[string]string) // Use hash to deduplicate

	// Collect matching series from active MemTable
	activeMemTable.mu.RLock()
	for _, s := range activeMemTable.seriesMeta {
		if matchLabels(s.Labels, matchers) {
			seriesMap[s.Hash] = s.Labels
		}
	}
	activeMemTable.mu.RUnlock()

	// Collect matching series from flushing MemTable
	if flushingMemTable != nil {
		flushingMemTable.mu.RLock()
		for _, s := range flushingMemTable.seriesMeta {
			if matchLabels(s.Labels, matchers) {
				seriesMap[s.Hash] = s.Labels
			}
		}
		flushingMemTable.mu.RUnlock()
	}

	// Convert to slice
	result := make([]map[string]string, 0, len(seriesMap))
	for _, labels := range seriesMap {
		result = append(result, labels)
	}

	return result, nil
}

// matchLabels checks if the given labels match all matchers
func matchLabels(labels map[string]string, matchers index.Matchers) bool {
	if len(matchers) == 0 {
		return true // Empty matchers match everything
	}

	for _, matcher := range matchers {
		if !matcher.Matches(labels) {
			return false
		}
	}

	return true
}
