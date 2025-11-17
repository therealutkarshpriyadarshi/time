package storage

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

func TestCompactorBasic(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "compactor_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create compactor
	opts := DefaultCompactorOptions(tmpDir)
	compactor := NewCompactor(opts)
	defer compactor.Stop()

	// Test that compactor was created
	if compactor == nil {
		t.Fatal("compactor should not be nil")
	}

	// Test that we can get stats
	stats := compactor.GetStats()
	if stats.TotalCompactions.Load() != 0 {
		t.Errorf("expected 0 compactions, got %d", stats.TotalCompactions.Load())
	}
}

func TestCompactorMergeBlocks(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "compactor_merge_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test blocks with overlapping time ranges
	baseTime := time.Now().UnixMilli()

	// Create 3 Level 0 blocks (2 hours each)
	blocks := make([]*Block, 3)
	for i := 0; i < 3; i++ {
		minTime := baseTime + int64(i)*Level0Duration.Milliseconds()
		maxTime := minTime + Level0Duration.Milliseconds()

		block, err := NewBlock(minTime, maxTime)
		if err != nil {
			t.Fatalf("failed to create block: %v", err)
		}

		// Add test data
		testSeries := series.NewSeries(map[string]string{
			"__name__": "test_metric",
			"host":     "server1",
			"instance": "1",
		})

		samples := []series.Sample{
			{Timestamp: minTime + 1000, Value: float64(i)},
			{Timestamp: minTime + 2000, Value: float64(i + 1)},
			{Timestamp: minTime + 3000, Value: float64(i + 2)},
		}

		if err := block.AddSeries(testSeries, samples); err != nil {
			t.Fatalf("failed to add series: %v", err)
		}

		if err := block.Persist(tmpDir); err != nil {
			t.Fatalf("failed to persist block: %v", err)
		}

		blocks[i] = block
	}

	// Create compactor and merge blocks
	opts := DefaultCompactorOptions(tmpDir)
	compactor := NewCompactor(opts)
	defer compactor.Stop()

	// Trigger merge
	if err := compactor.mergeBlocks(blocks); err != nil {
		t.Fatalf("failed to merge blocks: %v", err)
	}

	// Verify that original blocks are deleted
	for _, block := range blocks {
		if _, err := os.Stat(block.Dir()); !os.IsNotExist(err) {
			t.Errorf("block %s should have been deleted", block.ULID.String())
		}
	}

	// Verify that a new merged block exists
	entries, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("failed to read dir: %v", err)
	}

	// Should have exactly 1 block (the merged one)
	blockCount := 0
	for _, entry := range entries {
		if entry.IsDir() && len(entry.Name()) > 10 {
			blockCount++
		}
	}

	if blockCount != 1 {
		t.Errorf("expected 1 merged block, got %d", blockCount)
	}
}

func TestCompactorDeduplication(t *testing.T) {
	// Create temporary directory
	tmpDir, err := os.MkdirTemp("", "compactor_dedup_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := DefaultCompactorOptions(tmpDir)
	compactor := NewCompactor(opts)
	defer compactor.Stop()

	// Test samples with duplicates
	samples := []series.Sample{
		{Timestamp: 1000, Value: 1.0},
		{Timestamp: 2000, Value: 2.0},
		{Timestamp: 1000, Value: 1.5}, // Duplicate timestamp
		{Timestamp: 3000, Value: 3.0},
		{Timestamp: 2000, Value: 2.5}, // Duplicate timestamp
	}

	deduplicated := compactor.deduplicateSamples(samples)

	// Should have 3 unique timestamps
	if len(deduplicated) != 3 {
		t.Errorf("expected 3 unique samples, got %d", len(deduplicated))
	}

	// Verify sorted by timestamp
	for i := 1; i < len(deduplicated); i++ {
		if deduplicated[i].Timestamp <= deduplicated[i-1].Timestamp {
			t.Errorf("samples not sorted: %d <= %d", deduplicated[i].Timestamp, deduplicated[i-1].Timestamp)
		}
	}

	// Verify that last value is kept for duplicates
	expectedValues := map[int64]float64{
		1000: 1.5, // Last value for timestamp 1000
		2000: 2.5, // Last value for timestamp 2000
		3000: 3.0,
	}

	for _, sample := range deduplicated {
		if expected, ok := expectedValues[sample.Timestamp]; ok {
			if sample.Value != expected {
				t.Errorf("timestamp %d: expected value %.1f, got %.1f",
					sample.Timestamp, expected, sample.Value)
			}
		}
	}
}

func TestCompactorGroupBlocksByTimeWindow(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compactor_group_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := DefaultCompactorOptions(tmpDir)
	compactor := NewCompactor(opts)
	defer compactor.Stop()

	// Create blocks at different times
	baseTime := time.Now().UnixMilli()

	blocks := make([]*Block, 5)
	for i := 0; i < 5; i++ {
		minTime := baseTime + int64(i)*Level0Duration.Milliseconds()
		maxTime := minTime + Level0Duration.Milliseconds()

		block, err := NewBlock(minTime, maxTime)
		if err != nil {
			t.Fatalf("failed to create block: %v", err)
		}
		blocks[i] = block
	}

	// Group by Level1 duration (12 hours)
	groups := compactor.groupBlocksByTimeWindow(blocks, Level1Duration)

	// We should have at least 1 group
	if len(groups) == 0 {
		t.Error("expected at least 1 group")
	}

	// Verify that blocks within a group are within the time window
	for i, group := range groups {
		if len(group) == 0 {
			continue
		}

		windowStart := group[0].MinTime
		windowEnd := windowStart + Level1Duration.Milliseconds()

		for _, block := range group {
			if block.MinTime < windowStart || block.MinTime >= windowEnd {
				t.Errorf("group %d: block %s outside window [%d, %d)",
					i, block.ULID.String(), windowStart, windowEnd)
			}
		}
	}
}

func TestCompactorGetBlocksByLevel(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compactor_level_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := DefaultCompactorOptions(tmpDir)
	compactor := NewCompactor(opts)
	defer compactor.Stop()

	baseTime := time.Now().UnixMilli()

	// Create blocks at different levels
	// Level 0: 2 hours
	block0, _ := NewBlock(baseTime, baseTime+Level0Duration.Milliseconds())

	// Level 1: 12 hours
	block1, _ := NewBlock(baseTime, baseTime+Level1Duration.Milliseconds())

	// Level 2: 7 days
	block2, _ := NewBlock(baseTime, baseTime+Level2Duration.Milliseconds())

	allBlocks := []*Block{block0, block1, block2}

	// Test Level 0
	level0Blocks := compactor.getBlocksByLevel(allBlocks, Level0)
	if len(level0Blocks) != 1 {
		t.Errorf("expected 1 Level0 block, got %d", len(level0Blocks))
	}

	// Test Level 1
	level1Blocks := compactor.getBlocksByLevel(allBlocks, Level1)
	if len(level1Blocks) != 1 {
		t.Errorf("expected 1 Level1 block, got %d", len(level1Blocks))
	}

	// Test Level 2
	level2Blocks := compactor.getBlocksByLevel(allBlocks, Level2)
	if len(level2Blocks) != 1 {
		t.Errorf("expected 1 Level2 block, got %d", len(level2Blocks))
	}
}

func TestCompactorBlockCount(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compactor_count_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	opts := DefaultCompactorOptions(tmpDir)
	compactor := NewCompactor(opts)
	defer compactor.Stop()

	// Initially should have no blocks
	l0, l1, l2, err := compactor.BlockCount()
	if err != nil {
		t.Fatalf("failed to get block count: %v", err)
	}

	if l0 != 0 || l1 != 0 || l2 != 0 {
		t.Errorf("expected all levels to be 0, got l0=%d, l1=%d, l2=%d", l0, l1, l2)
	}
}

func TestCompactorValidateBlocks(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compactor_validate_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create a valid block
	baseTime := time.Now().UnixMilli()
	block, err := NewBlock(baseTime, baseTime+Level0Duration.Milliseconds())
	if err != nil {
		t.Fatalf("failed to create block: %v", err)
	}

	testSeries := series.NewSeries(map[string]string{
		"__name__": "test_metric",
	})
	samples := []series.Sample{
		{Timestamp: baseTime + 1000, Value: 1.0},
	}

	if err := block.AddSeries(testSeries, samples); err != nil {
		t.Fatalf("failed to add series: %v", err)
	}

	if err := block.Persist(tmpDir); err != nil {
		t.Fatalf("failed to persist block: %v", err)
	}

	// Validate blocks
	opts := DefaultCompactorOptions(tmpDir)
	compactor := NewCompactor(opts)
	defer compactor.Stop()

	if err := compactor.ValidateBlocks(); err != nil {
		t.Errorf("validation should pass for valid blocks: %v", err)
	}

	// Test validation with corrupted block (delete meta.json)
	metaPath := filepath.Join(block.Dir(), MetaFile)
	if err := os.Remove(metaPath); err != nil {
		t.Fatalf("failed to remove meta.json: %v", err)
	}

	// Reload blocks
	compactor.blockReader.LoadBlocks()

	if err := compactor.ValidateBlocks(); err == nil {
		t.Error("validation should fail for corrupted blocks")
	}
}

func TestCompactorCleanupOldBlocks(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "compactor_cleanup_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create blocks with different ages
	now := time.Now().UnixMilli()
	oldTime := now - (31 * 24 * time.Hour).Milliseconds() // 31 days ago
	recentTime := now - (1 * 24 * time.Hour).Milliseconds() // 1 day ago

	// Old block
	oldBlock, _ := NewBlock(oldTime, oldTime+Level0Duration.Milliseconds())
	testSeries := series.NewSeries(map[string]string{"__name__": "old_metric"})
	oldSamples := []series.Sample{{Timestamp: oldTime + 1000, Value: 1.0}}
	oldBlock.AddSeries(testSeries, oldSamples)
	oldBlock.Persist(tmpDir)

	// Recent block
	recentBlock, _ := NewBlock(recentTime, recentTime+Level0Duration.Milliseconds())
	recentSamples := []series.Sample{{Timestamp: recentTime + 1000, Value: 2.0}}
	recentBlock.AddSeries(testSeries, recentSamples)
	recentBlock.Persist(tmpDir)

	// Create compactor
	opts := DefaultCompactorOptions(tmpDir)
	compactor := NewCompactor(opts)
	defer compactor.Stop()

	// Clean up blocks older than 30 days
	cutoffTime := now - (30 * 24 * time.Hour).Milliseconds()
	deletedCount, err := compactor.CleanupOldBlocks(cutoffTime)
	if err != nil {
		t.Fatalf("failed to cleanup old blocks: %v", err)
	}

	// Should have deleted 1 block (the old one)
	if deletedCount != 1 {
		t.Errorf("expected 1 deleted block, got %d", deletedCount)
	}

	// Verify old block is deleted
	if _, err := os.Stat(oldBlock.Dir()); !os.IsNotExist(err) {
		t.Error("old block should have been deleted")
	}

	// Verify recent block still exists
	if _, err := os.Stat(recentBlock.Dir()); os.IsNotExist(err) {
		t.Error("recent block should not have been deleted")
	}
}

func BenchmarkCompactorMergeBlocks(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "compactor_bench_*")
	defer os.RemoveAll(tmpDir)

	opts := DefaultCompactorOptions(tmpDir)
	compactor := NewCompactor(opts)
	defer compactor.Stop()

	// Create test blocks
	baseTime := time.Now().UnixMilli()
	blocks := make([]*Block, 5)

	for i := 0; i < 5; i++ {
		minTime := baseTime + int64(i)*Level0Duration.Milliseconds()
		maxTime := minTime + Level0Duration.Milliseconds()

		block, _ := NewBlock(minTime, maxTime)
		testSeries := series.NewSeries(map[string]string{
			"__name__": "bench_metric",
			"instance": string(rune(i)),
		})

		samples := make([]series.Sample, 100)
		for j := 0; j < 100; j++ {
			samples[j] = series.Sample{
				Timestamp: minTime + int64(j)*1000,
				Value:     float64(j),
			}
		}

		block.AddSeries(testSeries, samples)
		block.Persist(tmpDir)
		blocks[i] = block
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		// Note: This will delete the blocks, so we'd need to recreate for real benchmarks
		compactor.mergeBlocks(blocks)
	}
}
