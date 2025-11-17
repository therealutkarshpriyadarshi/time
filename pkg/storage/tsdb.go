package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

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

	// DefaultBlocksDir is the default directory name for block storage
	DefaultBlocksDir = "data"
)

// TSDB is the main time-series database orchestrator.
// It coordinates WAL writes, MemTable operations, and background flushing.
type TSDB struct {
	// Configuration
	dataDir       string
	blocksDir     string
	flushInterval time.Duration

	// Write path components
	activeMemTable   *MemTable
	flushingMemTable *MemTable
	walWriter        *wal.WAL

	// Storage components
	blocks []*Block

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
	DataDir       string
	FlushInterval time.Duration
	WALOptions    *wal.Options
	MemTableSize  int64
}

// DefaultOptions returns default TSDB options
func DefaultOptions(dataDir string) *Options {
	return &Options{
		DataDir:       dataDir,
		FlushInterval: DefaultFlushInterval,
		WALOptions:    wal.DefaultOptions(),
		MemTableSize:  DefaultMaxSize,
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

	// Create blocks directory
	blocksDir := filepath.Join(opts.DataDir, DefaultBlocksDir)
	if err := os.MkdirAll(blocksDir, 0755); err != nil {
		return nil, fmt.Errorf("tsdb: failed to create blocks directory: %w", err)
	}

	// Open WAL
	walDir := filepath.Join(opts.DataDir, DefaultWALDir)
	walWriter, err := wal.Open(walDir, opts.WALOptions)
	if err != nil {
		return nil, fmt.Errorf("tsdb: failed to open WAL: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	db := &TSDB{
		dataDir:       opts.DataDir,
		blocksDir:     blocksDir,
		flushInterval: opts.FlushInterval,
		activeMemTable: NewMemTableWithSize(opts.MemTableSize),
		walWriter:     walWriter,
		blocks:        make([]*Block, 0),
		flushChan:     make(chan struct{}, 1),
		flusherDone:   make(chan struct{}),
		ctx:           ctx,
		cancel:        cancel,
	}

	// Recover from WAL
	if err := db.recover(); err != nil {
		walWriter.Close()
		return nil, fmt.Errorf("tsdb: failed to recover: %w", err)
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

	// Query disk blocks
	var blockSamples []series.Sample
	db.mu.RLock()
	blocks := db.blocks
	db.mu.RUnlock()

	for _, block := range blocks {
		if block.Overlaps(start, end) {
			samples, err := block.Query(seriesHash, start, end)
			if err != nil {
				return nil, fmt.Errorf("failed to query block %s: %w", block.ULID(), err)
			}
			blockSamples = append(blockSamples, samples...)
		}
	}

	// Merge results from all sources
	result := make([]series.Sample, 0, len(activeSamples)+len(flushingSamples)+len(blockSamples))
	result = append(result, activeSamples...)
	result = append(result, flushingSamples...)
	result = append(result, blockSamples...)

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

	// Write MemTable to disk blocks
	block, err := db.writeMemTableToBlocks(oldMemTable, minTime, maxTime)
	if err != nil {
		fmt.Printf("tsdb: failed to write blocks: %v\n", err)
		// Continue with cleanup even if write fails
	} else {
		// Add block to tracking
		db.mu.Lock()
		db.blocks = append(db.blocks, block)
		db.mu.Unlock()
		fmt.Printf("tsdb: successfully wrote block %s (%d series, %d samples)\n",
			block.ULID(),
			block.Meta().Stats.NumSeries,
			block.Meta().Stats.NumSamples,
		)
	}

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

// writeMemTableToBlocks writes the MemTable data to disk blocks.
// Creates chunks for each series and writes them to a new block.
func (db *TSDB) writeMemTableToBlocks(mt *MemTable, minTime, maxTime int64) (*Block, error) {
	// Create new block
	block, err := NewBlock(minTime, maxTime, db.blocksDir)
	if err != nil {
		return nil, fmt.Errorf("failed to create block: %w", err)
	}

	// Get all series from MemTable
	allSeriesHashes := mt.AllSeries()

	// For each series, create chunks and write to block
	for _, seriesHash := range allSeriesHashes {
		// Query all samples for this series
		samples, err := mt.Query(seriesHash, minTime, maxTime)
		if err != nil {
			return nil, fmt.Errorf("failed to query series %v: %w", seriesHash, err)
		}

		if len(samples) == 0 {
			continue
		}

		// Create chunks for this series (chunk every DefaultChunkSize samples)
		for i := 0; i < len(samples); i += DefaultChunkSize {
			chunk := NewChunk()

			// Append samples to chunk
			end := i + DefaultChunkSize
			if end > len(samples) {
				end = len(samples)
			}

			for j := i; j < end; j++ {
				if err := chunk.Append(samples[j]); err != nil {
					return nil, fmt.Errorf("failed to append sample to chunk: %w", err)
				}
			}

			// Seal the chunk
			if err := chunk.Seal(); err != nil {
				return nil, fmt.Errorf("failed to seal chunk: %w", err)
			}

			// Write chunk to block
			if err := block.WriteChunk(seriesHash, chunk); err != nil {
				return nil, fmt.Errorf("failed to write chunk: %w", err)
			}
		}

		// Increment series count for this block
		block.IncrementSeriesCount()
	}

	// Write block metadata
	if err := block.WriteMeta(); err != nil {
		return nil, fmt.Errorf("failed to write block metadata: %w", err)
	}

	return block, nil
}
