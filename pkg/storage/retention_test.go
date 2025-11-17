package storage

import (
	"os"
	"testing"
	"time"

	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

func TestRetentionManagerBasic(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "retention_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create compactor (required for retention manager)
	compactorOpts := DefaultCompactorOptions(tmpDir)
	compactor := NewCompactor(compactorOpts)
	defer compactor.Stop()

	// Create retention manager
	opts := DefaultRetentionManagerOptions()
	rm := NewRetentionManager(compactor, opts)
	defer rm.Stop()

	// Test that retention manager was created
	if rm == nil {
		t.Fatal("retention manager should not be nil")
	}

	// Test that we can get stats
	stats := rm.GetStats()
	if stats.TotalCleanups.Load() != 0 {
		t.Errorf("expected 0 cleanups, got %d", stats.TotalCleanups.Load())
	}
}

func TestRetentionManagerPolicy(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "retention_policy_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	compactorOpts := DefaultCompactorOptions(tmpDir)
	compactor := NewCompactor(compactorOpts)
	defer compactor.Stop()

	opts := DefaultRetentionManagerOptions()
	rm := NewRetentionManager(compactor, opts)
	defer rm.Stop()

	// Test default policy
	policy := rm.GetPolicy()
	if policy.MaxAge != DefaultRetentionPeriod {
		t.Errorf("expected max age %v, got %v", DefaultRetentionPeriod, policy.MaxAge)
	}
	if !policy.Enabled {
		t.Error("policy should be enabled by default")
	}

	// Test updating policy
	newPolicy := RetentionPolicy{
		MaxAge:     7 * 24 * time.Hour,
		MinSamples: 100,
		Enabled:    true,
	}
	rm.SetPolicy(newPolicy)

	policy = rm.GetPolicy()
	if policy.MaxAge != newPolicy.MaxAge {
		t.Errorf("expected max age %v, got %v", newPolicy.MaxAge, policy.MaxAge)
	}
	if policy.MinSamples != newPolicy.MinSamples {
		t.Errorf("expected min samples %d, got %d", newPolicy.MinSamples, policy.MinSamples)
	}
}

func TestRetentionManagerCleanup(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "retention_cleanup_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create some old and recent blocks
	now := time.Now().UnixMilli()

	// Old block (35 days ago)
	oldTime := now - (35 * 24 * time.Hour).Milliseconds()
	oldBlock, _ := NewBlock(oldTime, oldTime+Level0Duration.Milliseconds())
	testSeries := series.NewSeries(map[string]string{"__name__": "old_metric"})
	oldSamples := []series.Sample{{Timestamp: oldTime + 1000, Value: 1.0}}
	oldBlock.AddSeries(testSeries, oldSamples)
	oldBlock.Persist(tmpDir)

	// Recent block (5 days ago)
	recentTime := now - (5 * 24 * time.Hour).Milliseconds()
	recentBlock, _ := NewBlock(recentTime, recentTime+Level0Duration.Milliseconds())
	recentSamples := []series.Sample{{Timestamp: recentTime + 1000, Value: 2.0}}
	recentBlock.AddSeries(testSeries, recentSamples)
	recentBlock.Persist(tmpDir)

	// Create retention manager with 30-day retention
	compactorOpts := DefaultCompactorOptions(tmpDir)
	compactor := NewCompactor(compactorOpts)
	defer compactor.Stop()

	opts := &RetentionManagerOptions{
		Policy: RetentionPolicy{
			MaxAge:     30 * 24 * time.Hour,
			MinSamples: 0,
			Enabled:    true,
		},
		Interval: 1 * time.Hour,
	}
	rm := NewRetentionManager(compactor, opts)
	defer rm.Stop()

	// Debug: Check what blocks exist before cleanup
	entries, _ := os.ReadDir(tmpDir)
	var blockCount int
	for _, entry := range entries {
		if entry.IsDir() && len(entry.Name()) > 10 {
			blockCount++
			t.Logf("Block found before cleanup: %s", entry.Name())
		}
	}
	t.Logf("Total blocks before cleanup: %d", blockCount)

	// Trigger cleanup
	if err := rm.CleanupNow(); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}

	// Debug: Check what blocks exist after cleanup
	entries, _ = os.ReadDir(tmpDir)
	blockCount = 0
	for _, entry := range entries {
		if entry.IsDir() && len(entry.Name()) > 10 {
			blockCount++
			t.Logf("Block found after cleanup: %s", entry.Name())
		}
	}
	t.Logf("Total blocks after cleanup: %d", blockCount)

	// Verify old block is deleted
	if _, err := os.Stat(oldBlock.Dir()); !os.IsNotExist(err) {
		t.Errorf("old block should have been deleted, path: %s", oldBlock.Dir())
	}

	// Verify recent block still exists
	if _, err := os.Stat(recentBlock.Dir()); os.IsNotExist(err) {
		t.Errorf("recent block should not have been deleted, path: %s", recentBlock.Dir())
	}

	// Check stats
	stats := rm.GetStats()
	if stats.TotalCleanups.Load() < 1 {
		t.Errorf("expected at least 1 cleanup, got %d", stats.TotalCleanups.Load())
	}
	if stats.BlocksDeleted.Load() < 1 {
		t.Errorf("expected at least 1 block deleted, got %d", stats.BlocksDeleted.Load())
	}
}

func TestRetentionManagerCalculateStats(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "retention_stats_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	now := time.Now().UnixMilli()

	// Create blocks with different ages
	ages := []time.Duration{
		5 * 24 * time.Hour,  // 5 days
		10 * 24 * time.Hour, // 10 days
		35 * 24 * time.Hour, // 35 days
		40 * 24 * time.Hour, // 40 days
	}

	testSeries := series.NewSeries(map[string]string{"__name__": "test"})

	for i, age := range ages {
		blockTime := now - age.Milliseconds()
		block, _ := NewBlock(blockTime, blockTime+Level0Duration.Milliseconds())
		samples := []series.Sample{{Timestamp: blockTime + 1000, Value: float64(i)}}
		block.AddSeries(testSeries, samples)
		block.Persist(tmpDir)
	}

	// Create retention manager with 30-day retention
	compactorOpts := DefaultCompactorOptions(tmpDir)
	compactor := NewCompactor(compactorOpts)
	defer compactor.Stop()

	opts := &RetentionManagerOptions{
		Policy: RetentionPolicy{
			MaxAge:     30 * 24 * time.Hour,
			MinSamples: 0,
			Enabled:    true,
		},
		Interval: 1 * time.Hour,
	}
	rm := NewRetentionManager(compactor, opts)
	defer rm.Stop()

	// Calculate retention stats
	report, err := rm.CalculateRetentionStats()
	if err != nil {
		t.Fatalf("failed to calculate stats: %v", err)
	}

	// Should have 4 total blocks
	if report.TotalBlocks != 4 {
		t.Errorf("expected 4 total blocks, got %d", report.TotalBlocks)
	}

	// Should have 2 blocks eligible for deletion (35 and 40 days old)
	if report.BlocksEligibleForDeletion != 2 {
		t.Errorf("expected 2 blocks eligible for deletion, got %d", report.BlocksEligibleForDeletion)
	}

	// Verify policy max age
	if report.PolicyMaxAge != 30*24*time.Hour {
		t.Errorf("expected max age 30 days, got %v", report.PolicyMaxAge)
	}
}

func TestRetentionManagerEnableDisable(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "retention_enable_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	compactorOpts := DefaultCompactorOptions(tmpDir)
	compactor := NewCompactor(compactorOpts)
	defer compactor.Stop()

	opts := DefaultRetentionManagerOptions()
	rm := NewRetentionManager(compactor, opts)
	defer rm.Stop()

	// Test enabled by default
	if !rm.IsEnabled() {
		t.Error("retention should be enabled by default")
	}

	// Test disable
	rm.Disable()
	if rm.IsEnabled() {
		t.Error("retention should be disabled")
	}

	// Test enable
	rm.Enable()
	if !rm.IsEnabled() {
		t.Error("retention should be enabled")
	}
}

func TestRetentionManagerWithDisabledPolicy(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "retention_disabled_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Create old block
	oldTime := time.Now().UnixMilli() - (60 * 24 * time.Hour).Milliseconds()
	oldBlock, _ := NewBlock(oldTime, oldTime+Level0Duration.Milliseconds())
	testSeries := series.NewSeries(map[string]string{"__name__": "test"})
	oldSamples := []series.Sample{{Timestamp: oldTime + 1000, Value: 1.0}}
	oldBlock.AddSeries(testSeries, oldSamples)
	oldBlock.Persist(tmpDir)

	compactorOpts := DefaultCompactorOptions(tmpDir)
	compactor := NewCompactor(compactorOpts)
	defer compactor.Stop()

	// Create retention manager with policy disabled
	opts := &RetentionManagerOptions{
		Policy: RetentionPolicy{
			MaxAge:     30 * 24 * time.Hour,
			MinSamples: 0,
			Enabled:    false, // Disabled
		},
		Interval: 1 * time.Hour,
	}
	rm := NewRetentionManager(compactor, opts)
	defer rm.Stop()

	// Trigger cleanup (should do nothing because policy is disabled)
	if err := rm.CleanupNow(); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}

	// Verify block still exists (not deleted because policy disabled)
	if _, err := os.Stat(oldBlock.Dir()); os.IsNotExist(err) {
		t.Error("block should not have been deleted when policy is disabled")
	}
}

func TestRetentionStatsReport(t *testing.T) {
	report := &RetentionStatsReport{
		TotalBlocks:               10,
		BlocksEligibleForDeletion: 3,
		TotalDataSize:             1024 * 1024 * 100, // 100MB
		ReclaimableSize:           1024 * 1024 * 30,  // 30MB
		PolicyMaxAge:              30 * 24 * time.Hour,
	}

	// Test String() method
	str := report.String()
	if str == "" {
		t.Error("String() should return non-empty string")
	}

	// Verify it contains expected information
	expectedFields := []string{
		"TotalBlocks",
		"EligibleForDeletion",
		"TotalSize",
		"Reclaimable",
		"MaxAge",
	}

	for _, field := range expectedFields {
		if !contains(str, field) {
			t.Errorf("String() output should contain %s", field)
		}
	}
}

func TestRetentionManagerStressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	tmpDir, err := os.MkdirTemp("", "retention_stress_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	now := time.Now().UnixMilli()
	testSeries := series.NewSeries(map[string]string{"__name__": "stress_test"})

	// Create many blocks
	numBlocks := 50
	for i := 0; i < numBlocks; i++ {
		// Create blocks spanning 100 days
		age := time.Duration(i*2) * 24 * time.Hour
		blockTime := now - age.Milliseconds()

		block, _ := NewBlock(blockTime, blockTime+Level0Duration.Milliseconds())
		samples := []series.Sample{{Timestamp: blockTime + 1000, Value: float64(i)}}
		block.AddSeries(testSeries, samples)
		block.Persist(tmpDir)
	}

	// Create retention manager with 30-day retention
	compactorOpts := DefaultCompactorOptions(tmpDir)
	compactor := NewCompactor(compactorOpts)
	defer compactor.Stop()

	opts := &RetentionManagerOptions{
		Policy: RetentionPolicy{
			MaxAge:     30 * 24 * time.Hour,
			MinSamples: 0,
			Enabled:    true,
		},
		Interval: 1 * time.Hour,
	}
	rm := NewRetentionManager(compactor, opts)
	defer rm.Stop()

	// Trigger cleanup
	if err := rm.CleanupNow(); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}

	// Verify that old blocks were deleted
	stats := rm.GetStats()
	if stats.BlocksDeleted.Load() == 0 {
		t.Error("expected some blocks to be deleted")
	}

	// Re-initialize compactor to reload blocks from disk
	compactor2 := NewCompactor(DefaultCompactorOptions(tmpDir))
	defer compactor2.Stop()

	// Create new retention manager with reloaded compactor
	rm2 := NewRetentionManager(compactor2, opts)
	defer rm2.Stop()

	// Calculate remaining blocks with fresh data
	report, err := rm2.CalculateRetentionStats()
	if err != nil {
		t.Fatalf("failed to calculate stats: %v", err)
	}

	// Remaining blocks should all be within retention period
	if report.BlocksEligibleForDeletion > 0 {
		t.Logf("Total blocks: %d, Eligible for deletion: %d", report.TotalBlocks, report.BlocksEligibleForDeletion)
		t.Errorf("expected no blocks eligible for deletion after cleanup, got %d",
			report.BlocksEligibleForDeletion)
	}
}

// Helper function to check if a string contains a substring
func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr || len(s) > len(substr) &&
		(s[:len(substr)] == substr ||
		s[len(s)-len(substr):] == substr ||
		containsMiddle(s, substr)))
}

func containsMiddle(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func BenchmarkRetentionManagerCleanup(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "retention_bench_*")
	defer os.RemoveAll(tmpDir)

	// Create test blocks
	now := time.Now().UnixMilli()
	testSeries := series.NewSeries(map[string]string{"__name__": "bench"})

	for i := 0; i < 20; i++ {
		age := time.Duration(i*5) * 24 * time.Hour
		blockTime := now - age.Milliseconds()

		block, _ := NewBlock(blockTime, blockTime+Level0Duration.Milliseconds())
		samples := []series.Sample{{Timestamp: blockTime + 1000, Value: float64(i)}}
		block.AddSeries(testSeries, samples)
		block.Persist(tmpDir)
	}

	compactorOpts := DefaultCompactorOptions(tmpDir)
	compactor := NewCompactor(compactorOpts)
	defer compactor.Stop()

	opts := &RetentionManagerOptions{
		Policy: RetentionPolicy{
			MaxAge:     30 * 24 * time.Hour,
			MinSamples: 0,
			Enabled:    true,
		},
		Interval: 1 * time.Hour,
	}
	rm := NewRetentionManager(compactor, opts)
	defer rm.Stop()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		rm.CleanupNow()
	}
}
