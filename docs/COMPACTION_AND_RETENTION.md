# Compaction and Retention - Phase 6

This document describes the background operations implemented in Phase 6: compaction and retention policies.

## Overview

Phase 6 introduces two critical background operations for maintaining database health and performance:

1. **Compaction**: Merges smaller blocks into larger ones to reduce query overhead and improve storage efficiency
2. **Retention Policy**: Automatically deletes old data based on configured retention periods

Both operations run in the background and can be configured or disabled as needed.

## Table of Contents

- [Compaction](#compaction)
  - [Architecture](#compaction-architecture)
  - [Tiered Strategy](#tiered-compaction-strategy)
  - [Configuration](#compaction-configuration)
  - [Metrics](#compaction-metrics)
- [Retention Policy](#retention-policy)
  - [Architecture](#retention-architecture)
  - [Configuration](#retention-configuration)
  - [Metrics](#retention-metrics)
- [Usage Examples](#usage-examples)
- [Performance Considerations](#performance-considerations)

---

## Compaction

### Compaction Architecture

Compaction is the process of merging multiple smaller blocks into fewer, larger blocks. This provides several benefits:

- **Reduced Query Overhead**: Fewer blocks to scan during queries
- **Better Compression**: Larger blocks compress more efficiently
- **Deduplication**: Removes duplicate samples across blocks
- **Storage Optimization**: Reclaims disk space

#### How It Works

1. **Block Discovery**: Scans the data directory for blocks
2. **Level Classification**: Groups blocks by their duration (level)
3. **Candidate Selection**: Identifies groups of blocks eligible for merging
4. **Block Merging**: Combines series data from multiple blocks
5. **Deduplication**: Removes duplicate timestamps (keeps last value)
6. **Persistence**: Writes the merged block to disk
7. **Cleanup**: Deletes the original blocks atomically

### Tiered Compaction Strategy

The compactor implements a three-tier compaction strategy similar to LSM trees:

| Level | Duration | Description | Trigger |
|-------|----------|-------------|---------|
| **Level 0** | 2 hours | Raw ingestion blocks | MemTable flush |
| **Level 1** | 12 hours | Merged L0 blocks (6x) | ≥3 L0 blocks in time window |
| **Level 2** | 7 days | Merged L1 blocks (14x) | ≥3 L1 blocks in time window |

**Example Compaction Flow:**

```
Level 0: [Block1:2h] [Block2:2h] [Block3:2h] [Block4:2h]
           ↓ Compact (merge 3+ blocks)
Level 1: [MergedBlock:12h]

Level 1: [Block1:12h] [Block2:12h] [Block3:12h]
           ↓ Compact (merge 3+ blocks)
Level 2: [MergedBlock:7d]
```

### Compaction Configuration

```go
// Enable compaction with custom settings
opts := storage.DefaultOptions("./data")
opts.EnableCompaction = true
opts.CompactionInterval = 5 * time.Minute  // Check every 5 minutes

db, err := storage.Open(opts)
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Manual compaction trigger (for testing/admin operations)
err = db.TriggerCompaction()
if err != nil {
    log.Printf("Compaction failed: %v", err)
}
```

### Compaction Metrics

The compactor exposes the following metrics:

```go
stats := db.GetCompactionStats()

fmt.Printf("Total Compactions: %d\n", stats.TotalCompactions.Load())
fmt.Printf("Blocks Merged: %d\n", stats.BlocksMerged.Load())
fmt.Printf("Bytes Reclaimed: %d\n", stats.BytesReclaimed.Load())
fmt.Printf("Last Compaction: %d\n", stats.LastCompactionTime.Load())
fmt.Printf("Compaction Errors: %d\n", stats.CompactionErrors.Load())
fmt.Printf("Level 0 Compactions: %d\n", stats.Level0Compactions.Load())
fmt.Printf("Level 1 Compactions: %d\n", stats.Level1Compactions.Load())
```

---

## Retention Policy

### Retention Architecture

The retention manager automatically deletes blocks older than a configured retention period. This prevents unbounded disk usage growth.

#### How It Works

1. **Policy Check**: Evaluates if retention is enabled
2. **Cutoff Calculation**: Determines cutoff time (now - maxAge)
3. **Block Scan**: Identifies blocks older than cutoff
4. **Deletion**: Removes old blocks from disk
5. **Metrics Update**: Tracks blocks deleted and bytes reclaimed

### Retention Configuration

```go
// Enable retention with 30-day policy (default)
opts := storage.DefaultOptions("./data")
opts.EnableRetention = true
opts.RetentionPeriod = 30 * 24 * time.Hour  // 30 days

db, err := storage.Open(opts)
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// Update retention policy at runtime
newPolicy := storage.RetentionPolicy{
    MaxAge:     7 * 24 * time.Hour,  // 7 days
    MinSamples: 100,                 // Keep at least 100 samples (optional)
    Enabled:    true,
}
err = db.SetRetentionPolicy(newPolicy)
if err != nil {
    log.Printf("Failed to update policy: %v", err)
}

// Get current policy
policy := db.GetRetentionPolicy()
fmt.Printf("Max Age: %v\n", policy.MaxAge)
fmt.Printf("Enabled: %v\n", policy.Enabled)
```

### Retention Metrics

```go
stats := db.GetRetentionStats()

fmt.Printf("Blocks Deleted: %d\n", stats.BlocksDeleted.Load())
fmt.Printf("Bytes Reclaimed: %d\n", stats.BytesReclaimed.Load())
fmt.Printf("Total Cleanups: %d\n", stats.TotalCleanups.Load())
fmt.Printf("Last Cleanup: %d\n", stats.LastCleanupTime.Load())
fmt.Printf("Cleanup Errors: %d\n", stats.CleanupErrors.Load())
```

---

## Usage Examples

### Basic Setup with All Features Enabled

```go
package main

import (
    "log"
    "time"

    "github.com/therealutkarshpriyadarshi/time/pkg/storage"
    "github.com/therealutkarshpriyadarshi/time/pkg/series"
)

func main() {
    // Configure TSDB with compaction and retention
    opts := storage.DefaultOptions("./tsdb-data")
    opts.EnableCompaction = true
    opts.CompactionInterval = 10 * time.Minute
    opts.EnableRetention = true
    opts.RetentionPeriod = 30 * 24 * time.Hour  // 30 days

    db, err := storage.Open(opts)
    if err != nil {
        log.Fatalf("Failed to open TSDB: %v", err)
    }
    defer db.Close()

    // Write some data
    s := series.NewSeries(map[string]string{
        "__name__": "cpu_usage",
        "host":     "server1",
    })

    samples := []series.Sample{
        {Timestamp: time.Now().UnixMilli(), Value: 0.75},
    }

    err = db.Insert(s, samples)
    if err != nil {
        log.Printf("Insert failed: %v", err)
    }

    // Monitor background operations
    go monitorBackgroundOps(db)

    // Keep running
    select {}
}

func monitorBackgroundOps(db *storage.TSDB) {
    ticker := time.NewTicker(1 * time.Minute)
    defer ticker.Stop()

    for range ticker.C {
        // Compaction stats
        compactionStats := db.GetCompactionStats()
        if compactionStats != nil {
            log.Printf("Compaction - Total: %d, Blocks Merged: %d, Bytes Reclaimed: %d MB",
                compactionStats.TotalCompactions.Load(),
                compactionStats.BlocksMerged.Load(),
                compactionStats.BytesReclaimed.Load() / 1024 / 1024,
            )
        }

        // Retention stats
        retentionStats := db.GetRetentionStats()
        if retentionStats != nil {
            log.Printf("Retention - Blocks Deleted: %d, Total Cleanups: %d",
                retentionStats.BlocksDeleted.Load(),
                retentionStats.TotalCleanups.Load(),
            )
        }
    }
}
```

### Disable Background Operations

```go
// Disable compaction and retention
opts := storage.DefaultOptions("./data")
opts.EnableCompaction = false
opts.EnableRetention = false

db, err := storage.Open(opts)
// ...
```

### Manual Operations for Testing

```go
// Manual compaction trigger
err := db.TriggerCompaction()
if err != nil {
    log.Printf("Manual compaction failed: %v", err)
}

// Check compaction results
compactionStats := db.GetCompactionStats()
log.Printf("Compaction completed, merged %d blocks",
    compactionStats.BlocksMerged.Load())
```

---

## Performance Considerations

### Compaction

**Impact on Performance:**

- **CPU**: Moderate during compaction (decompression + re-compression)
- **Disk I/O**: High during merge (reading old blocks, writing new block)
- **Memory**: Proportional to block size (typically <100MB)
- **Query Latency**: Minimal impact (read locks are brief)

**Best Practices:**

1. **Interval**: Set compaction interval based on write rate
   - High write rate: 5-10 minutes
   - Low write rate: 30-60 minutes

2. **Concurrency**: Default is 1 worker (conservative)
   - Can increase for high-throughput systems
   - Monitor CPU usage

3. **Block Size**: Default 2-hour L0 blocks work well for most workloads
   - Larger blocks: Better compression, slower compaction
   - Smaller blocks: Faster compaction, more query overhead

### Retention

**Impact on Performance:**

- **CPU**: Very low (simple timestamp comparison)
- **Disk I/O**: Low (just block deletion)
- **Memory**: Minimal (<1MB)
- **Query Latency**: None (operates on old, unused blocks)

**Best Practices:**

1. **Retention Period**: Balance storage cost vs. data needs
   - Production metrics: 30-90 days
   - Development/testing: 7-14 days
   - Long-term trends: 1+ year

2. **Check Interval**: Default 1 hour is suitable for most cases
   - Can increase to reduce overhead
   - Daily checks are often sufficient

3. **MinSamples**: Optional safeguard
   - Prevents deletion of sparse but important series
   - Usually not needed for metrics data

### Monitoring

Monitor these metrics to ensure healthy background operations:

```go
// Check if background operations are keeping up
compactionStats := db.GetCompactionStats()
retentionStats := db.GetRetentionStats()

// Alert if:
// 1. No compactions in last hour (and there's data)
if time.Now().UnixMilli() - compactionStats.LastCompactionTime.Load() > 3600000 {
    log.Warn("No recent compactions")
}

// 2. Compaction errors increasing
if compactionStats.CompactionErrors.Load() > 10 {
    log.Error("High compaction error rate")
}

// 3. No retention cleanups in last day
if time.Now().UnixMilli() - retentionStats.LastCleanupTime.Load() > 86400000 {
    log.Warn("No recent retention cleanups")
}
```

---

## Implementation Details

### Thread Safety

Both compaction and retention are thread-safe:

- **Compactor**: Uses `sync.RWMutex` for block access
- **RetentionManager**: Uses `sync.RWMutex` for policy updates
- **Block Deletion**: Atomic (all or nothing)
- **Concurrent Reads**: Supported during compaction

### Failure Handling

Both operations are designed to be safe and idempotent:

**Compaction Failures:**
- Original blocks preserved on error
- Partial merged blocks cleaned up
- Error logged, operation retried on next cycle
- No data loss

**Retention Failures:**
- Blocks only deleted on successful validation
- Errors logged but don't stop future cycles
- Safe to retry

### Disk Space Reclamation

After compaction or retention deletes blocks:

1. Block directory removed with `os.RemoveAll()`
2. Disk space freed immediately (on most filesystems)
3. `BytesReclaimed` metric updated
4. No manual intervention needed

---

## Troubleshooting

### Compaction Not Running

**Symptoms:** `LastCompactionTime` not updating

**Possible Causes:**
1. Compaction disabled: Check `opts.EnableCompaction`
2. Not enough blocks: Need ≥3 blocks in same time window
3. Compaction interval too long: Check `opts.CompactionInterval`
4. Errors in logs: Check `CompactionErrors` metric

**Resolution:**
```go
// Check status
stats := db.GetCompactionStats()
log.Printf("Last compaction: %d ms ago",
    time.Now().UnixMilli() - stats.LastCompactionTime.Load())
log.Printf("Errors: %d", stats.CompactionErrors.Load())

// Force compaction
db.TriggerCompaction()
```

### Retention Not Deleting Old Data

**Symptoms:** Old blocks not being deleted

**Possible Causes:**
1. Retention disabled: Check `opts.EnableRetention`
2. Retention period too long: Check `opts.RetentionPeriod`
3. No blocks old enough: Check block ages
4. Policy disabled: Check `policy.Enabled`

**Resolution:**
```go
// Check policy
policy := db.GetRetentionPolicy()
log.Printf("Retention enabled: %v, MaxAge: %v", policy.Enabled, policy.MaxAge)

// Check stats
stats := db.GetRetentionStats()
log.Printf("Blocks deleted: %d", stats.BlocksDeleted.Load())
```

### High Compaction Overhead

**Symptoms:** High CPU/disk I/O during compaction

**Possible Causes:**
1. Too frequent compaction: Interval too short
2. Very large blocks: L2 blocks with lots of data
3. Too many concurrent compactions: Concurrency too high

**Resolution:**
```go
// Reduce compaction frequency
opts.CompactionInterval = 30 * time.Minute  // Instead of 5 minutes

// Or disable compaction during peak hours (implement custom logic)
```

---

## Future Enhancements

Potential improvements for future phases:

1. **Downsampling**: Store lower-resolution data for old blocks
2. **Partial Compaction**: Compact subsets of series
3. **Smart Scheduling**: Compact during low-traffic periods
4. **Compression Tuning**: Different compression per level
5. **Multi-tier Storage**: Move old blocks to cheaper storage
6. **Series Deletion**: Delete individual series (tombstones)
7. **Configurable Triggers**: Custom compaction criteria

---

## References

- [ROADMAP.md](../ROADMAP.md) - Phase 6 specifications
- [DESIGN.md](DESIGN.md) - Overall architecture
- [COMPRESSION.md](COMPRESSION.md) - Compression algorithms
- [LSM Tree Paper](https://www.cs.umb.edu/~poneil/lsmtree.pdf) - Inspiration for tiered compaction

---

**Phase 6 Status**: Complete ✓

**Key Features:**
- ✅ Tiered compaction (3 levels)
- ✅ Automatic block merging
- ✅ Deduplication
- ✅ Retention policy
- ✅ Background goroutines
- ✅ Comprehensive metrics
- ✅ Thread-safe operations
- ✅ Configurable policies
- ✅ Manual triggers for testing
