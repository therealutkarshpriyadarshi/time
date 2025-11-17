package storage

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultRetentionCheckInterval is how often to check retention policy
	DefaultRetentionCheckInterval = 1 * time.Hour

	// DefaultRetentionPeriod is the default data retention period (30 days)
	DefaultRetentionPeriod = 30 * 24 * time.Hour
)

// RetentionPolicy defines data retention rules
type RetentionPolicy struct {
	// MaxAge is the maximum age of data to keep
	MaxAge time.Duration

	// MinSamples is the minimum number of samples to keep per series
	// If set, series with fewer samples won't be deleted even if old
	MinSamples int64

	// Enabled indicates if retention policy is active
	Enabled bool
}

// RetentionManager manages data retention and garbage collection
type RetentionManager struct {
	policy    RetentionPolicy
	compactor *Compactor
	interval  time.Duration

	// State
	mu      sync.RWMutex
	running atomic.Bool
	ctx     context.Context
	cancel  context.CancelFunc

	// Metrics
	stats RetentionStats
}

// RetentionStats holds retention metrics
type RetentionStats struct {
	BlocksDeleted      atomic.Int64
	BytesReclaimed     atomic.Int64
	LastCleanupTime    atomic.Int64 // Unix milliseconds
	CleanupErrors      atomic.Int64
	TotalCleanups      atomic.Int64
	SeriesGarbageCollected atomic.Int64
}

// RetentionManagerOptions configures the retention manager
type RetentionManagerOptions struct {
	Policy   RetentionPolicy
	Interval time.Duration
}

// DefaultRetentionManagerOptions returns default retention manager options
func DefaultRetentionManagerOptions() *RetentionManagerOptions {
	return &RetentionManagerOptions{
		Policy: RetentionPolicy{
			MaxAge:     DefaultRetentionPeriod,
			MinSamples: 0,
			Enabled:    true,
		},
		Interval: DefaultRetentionCheckInterval,
	}
}

// NewRetentionManager creates a new retention manager
func NewRetentionManager(compactor *Compactor, opts *RetentionManagerOptions) *RetentionManager {
	if opts == nil {
		opts = DefaultRetentionManagerOptions()
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &RetentionManager{
		policy:    opts.Policy,
		compactor: compactor,
		interval:  opts.Interval,
		ctx:       ctx,
		cancel:    cancel,
	}
}

// Run starts the background retention enforcement loop
func (rm *RetentionManager) Run() error {
	if rm.running.Swap(true) {
		return fmt.Errorf("retention manager already running")
	}
	defer rm.running.Store(false)

	if !rm.policy.Enabled {
		return fmt.Errorf("retention policy is disabled")
	}

	ticker := time.NewTicker(rm.interval)
	defer ticker.Stop()

	// Run initial cleanup
	if err := rm.cleanup(); err != nil {
		rm.stats.CleanupErrors.Add(1)
		// Log error but continue
	}

	for {
		select {
		case <-ticker.C:
			if err := rm.cleanup(); err != nil {
				rm.stats.CleanupErrors.Add(1)
				// Log error but continue
			}
		case <-rm.ctx.Done():
			return nil
		}
	}
}

// Stop stops the retention manager gracefully
func (rm *RetentionManager) Stop() error {
	rm.cancel()
	return nil
}

// cleanup performs a single retention cleanup cycle
func (rm *RetentionManager) cleanup() error {
	rm.mu.RLock()
	enabled := rm.policy.Enabled
	maxAge := rm.policy.MaxAge
	rm.mu.RUnlock()

	if !enabled {
		return nil
	}

	// Calculate cutoff time
	cutoffTime := time.Now().Add(-maxAge).UnixMilli()

	// Delete old blocks using the compactor
	deletedCount, err := rm.compactor.CleanupOldBlocks(cutoffTime)
	if err != nil {
		return fmt.Errorf("failed to cleanup old blocks: %w", err)
	}

	// Update metrics
	rm.stats.BlocksDeleted.Add(int64(deletedCount))
	rm.stats.TotalCleanups.Add(1)
	rm.stats.LastCleanupTime.Store(time.Now().UnixMilli())

	return nil
}

// SetPolicy updates the retention policy
func (rm *RetentionManager) SetPolicy(policy RetentionPolicy) {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.policy = policy
}

// GetPolicy returns the current retention policy
func (rm *RetentionManager) GetPolicy() RetentionPolicy {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.policy
}

// GetStats returns a snapshot of retention statistics
func (rm *RetentionManager) GetStats() RetentionStats {
	// Return a copy of the current stats
	var stats RetentionStats
	stats.BlocksDeleted.Store(rm.stats.BlocksDeleted.Load())
	stats.BytesReclaimed.Store(rm.stats.BytesReclaimed.Load())
	stats.LastCleanupTime.Store(rm.stats.LastCleanupTime.Load())
	stats.CleanupErrors.Store(rm.stats.CleanupErrors.Load())
	stats.TotalCleanups.Store(rm.stats.TotalCleanups.Load())
	stats.SeriesGarbageCollected.Store(rm.stats.SeriesGarbageCollected.Load())
	return stats
}

// CleanupNow triggers an immediate cleanup (for testing/debugging)
func (rm *RetentionManager) CleanupNow() error {
	return rm.cleanup()
}

// CalculateRetentionStats calculates statistics about data retention
func (rm *RetentionManager) CalculateRetentionStats() (*RetentionStatsReport, error) {
	rm.mu.RLock()
	defer rm.mu.RUnlock()

	// Load all blocks
	if err := rm.compactor.blockReader.LoadBlocks(); err != nil {
		return nil, fmt.Errorf("failed to load blocks: %w", err)
	}

	blocks := rm.compactor.blockReader.Blocks()
	cutoffTime := time.Now().Add(-rm.policy.MaxAge).UnixMilli()

	report := &RetentionStatsReport{
		TotalBlocks:      len(blocks),
		PolicyMaxAge:     rm.policy.MaxAge,
		CutoffTime:       cutoffTime,
	}

	var totalSize int64
	var eligibleForDeletionSize int64

	for _, block := range blocks {
		blockSize := block.Size()
		totalSize += blockSize

		if block.MaxTime < cutoffTime {
			report.BlocksEligibleForDeletion++
			eligibleForDeletionSize += blockSize
		}

		// Calculate age
		age := time.Now().UnixMilli() - block.MinTime
		if age > report.OldestBlockAge {
			report.OldestBlockAge = age
		}
		if report.NewestBlockAge == 0 || age < report.NewestBlockAge {
			report.NewestBlockAge = age
		}
	}

	report.TotalDataSize = totalSize
	report.ReclaimableSize = eligibleForDeletionSize

	return report, nil
}

// RetentionStatsReport provides a detailed retention statistics report
type RetentionStatsReport struct {
	TotalBlocks               int
	BlocksEligibleForDeletion int
	TotalDataSize             int64
	ReclaimableSize           int64
	PolicyMaxAge              time.Duration
	CutoffTime                int64
	OldestBlockAge            int64 // milliseconds
	NewestBlockAge            int64 // milliseconds
}

// String returns a human-readable representation of the retention stats
func (r *RetentionStatsReport) String() string {
	return fmt.Sprintf(
		"RetentionStats{TotalBlocks: %d, EligibleForDeletion: %d, TotalSize: %d bytes, Reclaimable: %d bytes, MaxAge: %v}",
		r.TotalBlocks,
		r.BlocksEligibleForDeletion,
		r.TotalDataSize,
		r.ReclaimableSize,
		r.PolicyMaxAge,
	)
}

// Enable enables the retention policy
func (rm *RetentionManager) Enable() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.policy.Enabled = true
}

// Disable disables the retention policy
func (rm *RetentionManager) Disable() {
	rm.mu.Lock()
	defer rm.mu.Unlock()
	rm.policy.Enabled = false
}

// IsEnabled returns whether the retention policy is enabled
func (rm *RetentionManager) IsEnabled() bool {
	rm.mu.RLock()
	defer rm.mu.RUnlock()
	return rm.policy.Enabled
}
