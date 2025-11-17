package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

// CompactionLevel represents the tiered compaction level
type CompactionLevel int

const (
	// Level0 represents raw 2-hour ingestion blocks
	Level0 CompactionLevel = 0
	// Level1 represents merged 12-hour blocks (6x L0 blocks)
	Level1 CompactionLevel = 1
	// Level2 represents merged 7-day blocks (14x L1 blocks)
	Level2 CompactionLevel = 2
)

const (
	// DefaultCompactionInterval is how often to run compaction
	DefaultCompactionInterval = 5 * time.Minute

	// Level0Duration is the duration of Level 0 blocks (2 hours)
	Level0Duration = 2 * time.Hour

	// Level1Duration is the duration of Level 1 blocks (12 hours)
	Level1Duration = 12 * time.Hour

	// Level2Duration is the duration of Level 2 blocks (7 days)
	Level2Duration = 7 * 24 * time.Hour

	// MinBlocksForCompaction is the minimum number of blocks to trigger compaction
	MinBlocksForCompaction = 3
)

// Compactor manages background compaction of time-series blocks.
// It implements a tiered compaction strategy similar to LSM trees:
// - Level 0: 2-hour blocks (raw ingestion)
// - Level 1: 12-hour blocks (merge 6x L0 blocks)
// - Level 2: 7-day blocks (merge 14x L1 blocks)
type Compactor struct {
	dataDir     string
	interval    time.Duration
	concurrency int

	// Block management
	blockReader *BlockReader
	blockWriter *BlockWriter

	// State
	mu      sync.RWMutex
	running atomic.Bool
	ctx     context.Context
	cancel  context.CancelFunc

	// Metrics
	stats CompactionStats
}

// CompactionStats holds compaction metrics
type CompactionStats struct {
	TotalCompactions   atomic.Int64
	BlocksMerged       atomic.Int64
	BytesReclaimed     atomic.Int64
	LastCompactionTime atomic.Int64 // Unix milliseconds
	CompactionErrors   atomic.Int64
	Level0Compactions  atomic.Int64
	Level1Compactions  atomic.Int64
}

// CompactorOptions configures the compactor
type CompactorOptions struct {
	DataDir     string
	Interval    time.Duration
	Concurrency int // Number of concurrent compaction workers
}

// DefaultCompactorOptions returns default compactor options
func DefaultCompactorOptions(dataDir string) *CompactorOptions {
	return &CompactorOptions{
		DataDir:     dataDir,
		Interval:    DefaultCompactionInterval,
		Concurrency: 1, // Conservative default
	}
}

// NewCompactor creates a new compactor instance
func NewCompactor(opts *CompactorOptions) *Compactor {
	if opts == nil {
		opts = DefaultCompactorOptions("")
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Compactor{
		dataDir:     opts.DataDir,
		interval:    opts.Interval,
		concurrency: opts.Concurrency,
		blockReader: NewBlockReader(opts.DataDir),
		blockWriter: NewBlockWriter(opts.DataDir),
		ctx:         ctx,
		cancel:      cancel,
	}
}

// Run starts the background compaction loop
func (c *Compactor) Run() error {
	if c.running.Swap(true) {
		return fmt.Errorf("compactor already running")
	}
	defer c.running.Store(false)

	ticker := time.NewTicker(c.interval)
	defer ticker.Stop()

	// Run initial compaction
	if err := c.compact(); err != nil {
		c.stats.CompactionErrors.Add(1)
		// Log error but continue
	}

	for {
		select {
		case <-ticker.C:
			if err := c.compact(); err != nil {
				c.stats.CompactionErrors.Add(1)
				// Log error but continue
			}
		case <-c.ctx.Done():
			return nil
		}
	}
}

// Stop stops the compactor gracefully
func (c *Compactor) Stop() error {
	c.cancel()
	return nil
}

// compact performs a single compaction cycle
func (c *Compactor) compact() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Load all blocks from disk
	if err := c.blockReader.LoadBlocks(); err != nil {
		return fmt.Errorf("failed to load blocks: %w", err)
	}

	blocks := c.blockReader.Blocks()
	if len(blocks) < MinBlocksForCompaction {
		return nil // Not enough blocks to compact
	}

	// Group blocks by level
	level0Blocks := c.getBlocksByLevel(blocks, Level0)
	level1Blocks := c.getBlocksByLevel(blocks, Level1)

	// Compact Level 0 blocks to Level 1
	if len(level0Blocks) >= MinBlocksForCompaction {
		if err := c.compactLevel(level0Blocks, Level0, Level1); err != nil {
			return fmt.Errorf("failed to compact level 0: %w", err)
		}
		c.stats.Level0Compactions.Add(1)
	}

	// Compact Level 1 blocks to Level 2
	if len(level1Blocks) >= MinBlocksForCompaction {
		if err := c.compactLevel(level1Blocks, Level1, Level2); err != nil {
			return fmt.Errorf("failed to compact level 1: %w", err)
		}
		c.stats.Level1Compactions.Add(1)
	}

	c.stats.TotalCompactions.Add(1)
	c.stats.LastCompactionTime.Store(time.Now().UnixMilli())

	return nil
}

// compactLevel compacts blocks from one level to the next
func (c *Compactor) compactLevel(blocks []*Block, fromLevel, toLevel CompactionLevel) error {
	if len(blocks) == 0 {
		return nil
	}

	// Group blocks by time windows
	groups := c.groupBlocksByTimeWindow(blocks, c.getLevelDuration(toLevel))

	for _, group := range groups {
		if len(group) < MinBlocksForCompaction {
			continue // Need at least MinBlocksForCompaction blocks to merge
		}

		// Merge blocks in this group
		if err := c.mergeBlocks(group); err != nil {
			return fmt.Errorf("failed to merge blocks: %w", err)
		}
	}

	return nil
}

// mergeBlocks merges multiple blocks into a single larger block
func (c *Compactor) mergeBlocks(blocks []*Block) error {
	if len(blocks) <= 1 {
		return nil // Nothing to merge
	}

	// Sort blocks by time
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].MinTime < blocks[j].MinTime
	})

	// Calculate merged block time range
	minTime := blocks[0].MinTime
	maxTime := blocks[len(blocks)-1].MaxTime

	// Create new merged block
	mergedBlock, err := NewBlock(minTime, maxTime)
	if err != nil {
		return fmt.Errorf("failed to create merged block: %w", err)
	}

	// Collect all unique series across blocks
	seriesMap := make(map[uint64]*series.Series)
	seriesSamples := make(map[uint64][]series.Sample)

	for _, block := range blocks {
		// First, collect all series hashes from this block
		var seriesHashes []uint64
		block.mu.RLock()
		for hash, s := range block.series {
			seriesMap[hash] = s
			seriesHashes = append(seriesHashes, hash)
		}
		block.mu.RUnlock()

		// Now get samples for each series (without holding the lock)
		for _, hash := range seriesHashes {
			samples, err := block.GetSeries(hash, minTime, maxTime)
			if err != nil {
				return fmt.Errorf("failed to get series samples: %w", err)
			}

			seriesSamples[hash] = append(seriesSamples[hash], samples...)
		}
	}

	// Add all series to merged block
	for hash, s := range seriesMap {
		samples := seriesSamples[hash]
		if len(samples) == 0 {
			continue
		}

		// Sort and deduplicate samples
		samples = c.deduplicateSamples(samples)

		if err := mergedBlock.AddSeries(s, samples); err != nil {
			return fmt.Errorf("failed to add series to merged block: %w", err)
		}
	}

	// Persist merged block
	if err := mergedBlock.Persist(c.dataDir); err != nil {
		return fmt.Errorf("failed to persist merged block: %w", err)
	}

	// Delete old blocks atomically
	var totalReclaimed int64
	for _, block := range blocks {
		blockSize := block.Size()
		if err := block.Delete(); err != nil {
			return fmt.Errorf("failed to delete old block %s: %w", block.ULID.String(), err)
		}
		totalReclaimed += blockSize
	}

	// Update metrics
	c.stats.BlocksMerged.Add(int64(len(blocks)))
	c.stats.BytesReclaimed.Add(totalReclaimed)

	return nil
}

// deduplicateSamples removes duplicate samples and sorts by timestamp
func (c *Compactor) deduplicateSamples(samples []series.Sample) []series.Sample {
	if len(samples) <= 1 {
		return samples
	}

	// Sort by timestamp
	sort.Slice(samples, func(i, j int) bool {
		return samples[i].Timestamp < samples[j].Timestamp
	})

	// Deduplicate - keep last value for duplicate timestamps
	result := make([]series.Sample, 0, len(samples))
	seen := make(map[int64]bool)

	// Process in reverse to keep last value
	for i := len(samples) - 1; i >= 0; i-- {
		if !seen[samples[i].Timestamp] {
			result = append(result, samples[i])
			seen[samples[i].Timestamp] = true
		}
	}

	// Reverse to restore chronological order
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return result
}

// groupBlocksByTimeWindow groups blocks into time windows for compaction
func (c *Compactor) groupBlocksByTimeWindow(blocks []*Block, windowDuration time.Duration) [][]*Block {
	if len(blocks) == 0 {
		return nil
	}

	// Sort blocks by minTime
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].MinTime < blocks[j].MinTime
	})

	var groups [][]*Block
	var currentGroup []*Block
	var windowStart int64

	for _, block := range blocks {
		if len(currentGroup) == 0 {
			// Start new group
			windowStart = block.MinTime
			currentGroup = []*Block{block}
		} else {
			// Check if block fits in current window
			windowEnd := windowStart + windowDuration.Milliseconds()
			if block.MinTime < windowEnd {
				currentGroup = append(currentGroup, block)
			} else {
				// Start new group
				groups = append(groups, currentGroup)
				windowStart = block.MinTime
				currentGroup = []*Block{block}
			}
		}
	}

	// Add last group
	if len(currentGroup) > 0 {
		groups = append(groups, currentGroup)
	}

	return groups
}

// getBlocksByLevel filters blocks by their level (based on duration)
func (c *Compactor) getBlocksByLevel(blocks []*Block, level CompactionLevel) []*Block {
	var result []*Block
	levelDuration := c.getLevelDuration(level)
	tolerance := time.Hour.Milliseconds() // Allow some tolerance

	for _, block := range blocks {
		duration := block.MaxTime - block.MinTime
		expectedDuration := levelDuration.Milliseconds()

		// Check if block duration matches this level (with tolerance)
		if duration >= expectedDuration-tolerance && duration <= expectedDuration+tolerance {
			result = append(result, block)
		}
	}

	return result
}

// getLevelDuration returns the duration for a compaction level
func (c *Compactor) getLevelDuration(level CompactionLevel) time.Duration {
	switch level {
	case Level0:
		return Level0Duration
	case Level1:
		return Level1Duration
	case Level2:
		return Level2Duration
	default:
		return Level0Duration
	}
}

// GetStats returns a snapshot of compaction statistics
func (c *Compactor) GetStats() CompactionStats {
	// Return a copy of the current stats
	var stats CompactionStats
	stats.TotalCompactions.Store(c.stats.TotalCompactions.Load())
	stats.BlocksMerged.Store(c.stats.BlocksMerged.Load())
	stats.BytesReclaimed.Store(c.stats.BytesReclaimed.Load())
	stats.LastCompactionTime.Store(c.stats.LastCompactionTime.Load())
	stats.CompactionErrors.Store(c.stats.CompactionErrors.Load())
	stats.Level0Compactions.Store(c.stats.Level0Compactions.Load())
	stats.Level1Compactions.Store(c.stats.Level1Compactions.Load())
	return stats
}

// CompactNow triggers an immediate compaction (for testing/debugging)
func (c *Compactor) CompactNow() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.compact()
}

// BlockCount returns the number of blocks at each level
func (c *Compactor) BlockCount() (level0, level1, level2 int, err error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if err := c.blockReader.LoadBlocks(); err != nil {
		return 0, 0, 0, fmt.Errorf("failed to load blocks: %w", err)
	}

	blocks := c.blockReader.Blocks()
	level0Blocks := c.getBlocksByLevel(blocks, Level0)
	level1Blocks := c.getBlocksByLevel(blocks, Level1)
	level2Blocks := c.getBlocksByLevel(blocks, Level2)

	return len(level0Blocks), len(level1Blocks), len(level2Blocks), nil
}

// SetDataDir updates the data directory (for testing)
func (c *Compactor) SetDataDir(dir string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dataDir = dir
	c.blockReader = NewBlockReader(dir)
	c.blockWriter = NewBlockWriter(dir)
}

// CleanupOldBlocks removes blocks older than the specified cutoff time
// This is used by the retention policy
func (c *Compactor) CleanupOldBlocks(cutoffTime int64) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if err := c.blockReader.LoadBlocks(); err != nil {
		return 0, fmt.Errorf("failed to load blocks: %w", err)
	}

	blocks := c.blockReader.Blocks()
	deletedCount := 0

	for _, block := range blocks {
		// Delete block if its maxTime is older than cutoff
		if block.MaxTime < cutoffTime {
			blockSize := block.Size()
			if err := block.Delete(); err != nil {
				return deletedCount, fmt.Errorf("failed to delete block %s: %w", block.ULID.String(), err)
			}
			deletedCount++
			c.stats.BytesReclaimed.Add(blockSize)
		}
	}

	return deletedCount, nil
}

// ValidateBlocks checks all blocks for corruption
func (c *Compactor) ValidateBlocks() error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if err := c.blockReader.LoadBlocks(); err != nil {
		return fmt.Errorf("failed to load blocks: %w", err)
	}

	blocks := c.blockReader.Blocks()
	for _, block := range blocks {
		// Check if meta.json exists
		metaPath := filepath.Join(block.Dir(), MetaFile)
		if _, err := os.Stat(metaPath); err != nil {
			return fmt.Errorf("block %s missing meta.json: %w", block.ULID.String(), err)
		}

		// Check if chunks directory exists
		chunksDir := filepath.Join(block.Dir(), ChunksDir)
		if _, err := os.Stat(chunksDir); err != nil {
			return fmt.Errorf("block %s missing chunks directory: %w", block.ULID.String(), err)
		}

		// Validate time range
		if block.MinTime > block.MaxTime {
			return fmt.Errorf("block %s has invalid time range: min=%d > max=%d",
				block.ULID.String(), block.MinTime, block.MaxTime)
		}
	}

	return nil
}
