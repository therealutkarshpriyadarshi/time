package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

// TestBlockCreateAndPersist tests block creation and persistence
func TestBlockCreateAndPersist(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Create block
	block, err := NewBlock(1000, 10000)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	// Add series
	s := series.NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	})

	samples := []series.Sample{
		{Timestamp: 1000, Value: 0.5},
		{Timestamp: 2000, Value: 0.6},
		{Timestamp: 3000, Value: 0.7},
	}

	if err := block.AddSeries(s, samples); err != nil {
		t.Fatalf("AddSeries failed: %v", err)
	}

	// Persist block
	if err := block.Persist(tmpDir); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Verify directory structure
	blockDir := filepath.Join(tmpDir, block.ULID.String())
	if _, err := os.Stat(blockDir); os.IsNotExist(err) {
		t.Errorf("block directory not created: %s", blockDir)
	}

	metaFile := filepath.Join(blockDir, MetaFile)
	if _, err := os.Stat(metaFile); os.IsNotExist(err) {
		t.Errorf("meta.json not created")
	}

	chunksDir := filepath.Join(blockDir, ChunksDir)
	if _, err := os.Stat(chunksDir); os.IsNotExist(err) {
		t.Errorf("chunks directory not created")
	}
}

// TestBlockOpenAndLoad tests opening a persisted block
func TestBlockOpenAndLoad(t *testing.T) {
	// Create temporary directory
	tmpDir := t.TempDir()

	// Create and persist block
	originalBlock, err := NewBlock(1000, 10000)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	s := series.NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	})

	samples := []series.Sample{
		{Timestamp: 1000, Value: 0.5},
		{Timestamp: 2000, Value: 0.6},
		{Timestamp: 3000, Value: 0.7},
	}

	if err := originalBlock.AddSeries(s, samples); err != nil {
		t.Fatalf("AddSeries failed: %v", err)
	}

	if err := originalBlock.Persist(tmpDir); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	// Open block
	blockDir := filepath.Join(tmpDir, originalBlock.ULID.String())
	loadedBlock, err := OpenBlock(blockDir)
	if err != nil {
		t.Fatalf("OpenBlock failed: %v", err)
	}

	// Verify metadata
	if loadedBlock.ULID.String() != originalBlock.ULID.String() {
		t.Errorf("ULID mismatch: got %s, want %s", loadedBlock.ULID.String(), originalBlock.ULID.String())
	}

	if loadedBlock.MinTime != originalBlock.MinTime {
		t.Errorf("MinTime mismatch: got %d, want %d", loadedBlock.MinTime, originalBlock.MinTime)
	}

	if loadedBlock.MaxTime != originalBlock.MaxTime {
		t.Errorf("MaxTime mismatch: got %d, want %d", loadedBlock.MaxTime, originalBlock.MaxTime)
	}

	if loadedBlock.NumSamples != originalBlock.NumSamples {
		t.Errorf("NumSamples mismatch: got %d, want %d", loadedBlock.NumSamples, originalBlock.NumSamples)
	}
}

// TestBlockGetSeries tests querying series from a block
func TestBlockGetSeries(t *testing.T) {
	block, err := NewBlock(1000, 10000)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	s := series.NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	})

	samples := []series.Sample{
		{Timestamp: 1000, Value: 0.5},
		{Timestamp: 2000, Value: 0.6},
		{Timestamp: 3000, Value: 0.7},
		{Timestamp: 4000, Value: 0.8},
		{Timestamp: 5000, Value: 0.9},
	}

	if err := block.AddSeries(s, samples); err != nil {
		t.Fatalf("AddSeries failed: %v", err)
	}

	// Query full range
	result, err := block.GetSeries(s.Hash, 1000, 5000)
	if err != nil {
		t.Fatalf("GetSeries failed: %v", err)
	}

	if len(result) != 5 {
		t.Errorf("expected 5 samples, got %d", len(result))
	}

	// Query partial range
	result, err = block.GetSeries(s.Hash, 2000, 4000)
	if err != nil {
		t.Fatalf("GetSeries failed: %v", err)
	}

	if len(result) != 3 {
		t.Errorf("expected 3 samples, got %d", len(result))
	}

	// Verify values
	expectedTimestamps := []int64{2000, 3000, 4000}
	for i, sample := range result {
		if sample.Timestamp != expectedTimestamps[i] {
			t.Errorf("sample %d: timestamp got %d, want %d", i, sample.Timestamp, expectedTimestamps[i])
		}
	}

	// Query non-existent series
	result, err = block.GetSeries(99999, 1000, 5000)
	if err != nil {
		t.Fatalf("GetSeries failed: %v", err)
	}

	if len(result) != 0 {
		t.Errorf("expected 0 samples for non-existent series, got %d", len(result))
	}
}

// TestBlockOverlaps tests time range overlap detection
func TestBlockOverlaps(t *testing.T) {
	block, err := NewBlock(1000, 5000)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	tests := []struct {
		name     string
		minTime  int64
		maxTime  int64
		overlaps bool
	}{
		{"complete overlap", 1000, 5000, true},
		{"partial overlap - start", 500, 2000, true},
		{"partial overlap - end", 4000, 6000, true},
		{"contains block", 500, 6000, true},
		{"inside block", 2000, 3000, true},
		{"before block", 100, 500, false},
		{"after block", 6000, 7000, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := block.Overlaps(tt.minTime, tt.maxTime)
			if result != tt.overlaps {
				t.Errorf("Overlaps(%d, %d): got %v, want %v", tt.minTime, tt.maxTime, result, tt.overlaps)
			}
		})
	}
}

// TestBlockWriterWriteMemTable tests writing a MemTable to a block
func TestBlockWriterWriteMemTable(t *testing.T) {
	tmpDir := t.TempDir()

	// Create and populate MemTable
	mt := NewMemTable()

	s1 := series.NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	})

	s2 := series.NewSeries(map[string]string{
		"__name__": "memory_usage",
		"host":     "server1",
	})

	samples1 := []series.Sample{
		{Timestamp: 1000, Value: 0.5},
		{Timestamp: 2000, Value: 0.6},
	}

	samples2 := []series.Sample{
		{Timestamp: 1000, Value: 1.5},
		{Timestamp: 2000, Value: 1.6},
	}

	if err := mt.Insert(s1, samples1); err != nil {
		t.Fatalf("Insert s1 failed: %v", err)
	}

	if err := mt.Insert(s2, samples2); err != nil {
		t.Fatalf("Insert s2 failed: %v", err)
	}

	// Write MemTable to block
	writer := NewBlockWriter(tmpDir)
	block, err := writer.WriteMemTable(mt)
	if err != nil {
		t.Fatalf("WriteMemTable failed: %v", err)
	}

	// Verify block
	if block.NumSeries != 2 {
		t.Errorf("NumSeries: got %d, want 2", block.NumSeries)
	}

	if block.NumSamples != 4 {
		t.Errorf("NumSamples: got %d, want 4", block.NumSamples)
	}

	// Verify data can be queried
	result, err := block.GetSeries(s1.Hash, 0, 10000)
	if err != nil {
		t.Fatalf("GetSeries failed: %v", err)
	}

	if len(result) != 2 {
		t.Errorf("expected 2 samples, got %d", len(result))
	}
}

// TestBlockReaderLoadBlocks tests loading multiple blocks
func TestBlockReaderLoadBlocks(t *testing.T) {
	tmpDir := t.TempDir()

	// Create multiple blocks
	writer := NewBlockWriter(tmpDir)

	// Block 1
	mt1 := NewMemTable()
	s1 := series.NewSeries(map[string]string{"__name__": "metric1"})
	samples1 := []series.Sample{{Timestamp: 1000, Value: 1.0}}
	mt1.Insert(s1, samples1)

	block1, err := writer.WriteMemTable(mt1)
	if err != nil {
		t.Fatalf("WriteMemTable 1 failed: %v", err)
	}

	// Block 2
	mt2 := NewMemTable()
	s2 := series.NewSeries(map[string]string{"__name__": "metric2"})
	samples2 := []series.Sample{{Timestamp: 2000, Value: 2.0}}
	mt2.Insert(s2, samples2)

	block2, err := writer.WriteMemTable(mt2)
	if err != nil {
		t.Fatalf("WriteMemTable 2 failed: %v", err)
	}

	// Load blocks
	reader := NewBlockReader(tmpDir)
	if err := reader.LoadBlocks(); err != nil {
		t.Fatalf("LoadBlocks failed: %v", err)
	}

	blocks := reader.Blocks()
	if len(blocks) != 2 {
		t.Fatalf("expected 2 blocks, got %d", len(blocks))
	}

	// Verify blocks are sorted by time
	if blocks[0].ULID.Time() > blocks[1].ULID.Time() {
		t.Error("blocks are not sorted by time")
	}

	// Verify ULIDs match
	ulidMap := map[string]bool{
		block1.ULID.String(): false,
		block2.ULID.String(): false,
	}

	for _, block := range blocks {
		if _, exists := ulidMap[block.ULID.String()]; exists {
			ulidMap[block.ULID.String()] = true
		}
	}

	for ulid, found := range ulidMap {
		if !found {
			t.Errorf("block %s not found in loaded blocks", ulid)
		}
	}
}

// TestBlockReaderQuery tests querying across multiple blocks
func TestBlockReaderQuery(t *testing.T) {
	tmpDir := t.TempDir()

	s := series.NewSeries(map[string]string{"__name__": "metric1"})

	// Create multiple blocks with the same series
	writer := NewBlockWriter(tmpDir)

	// Block 1: timestamps 1000-2000
	mt1 := NewMemTable()
	samples1 := []series.Sample{
		{Timestamp: 1000, Value: 1.0},
		{Timestamp: 1500, Value: 1.5},
	}
	mt1.Insert(s, samples1)
	if _, err := writer.WriteMemTable(mt1); err != nil {
		t.Fatalf("WriteMemTable 1 failed: %v", err)
	}

	// Block 2: timestamps 3000-4000
	mt2 := NewMemTable()
	samples2 := []series.Sample{
		{Timestamp: 3000, Value: 3.0},
		{Timestamp: 3500, Value: 3.5},
	}
	mt2.Insert(s, samples2)
	if _, err := writer.WriteMemTable(mt2); err != nil {
		t.Fatalf("WriteMemTable 2 failed: %v", err)
	}

	// Load and query blocks
	reader := NewBlockReader(tmpDir)
	if err := reader.LoadBlocks(); err != nil {
		t.Fatalf("LoadBlocks failed: %v", err)
	}

	// Query across both blocks
	result, err := reader.Query(s.Hash, 0, 5000)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(result) != 4 {
		t.Errorf("expected 4 samples, got %d", len(result))
	}

	// Query only first block
	result, err = reader.Query(s.Hash, 0, 2000)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(result) != 2 {
		t.Errorf("expected 2 samples from first block, got %d", len(result))
	}
}

// TestBlockDelete tests block deletion
func TestBlockDelete(t *testing.T) {
	tmpDir := t.TempDir()

	block, err := NewBlock(1000, 5000)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	s := series.NewSeries(map[string]string{"__name__": "metric1"})
	samples := []series.Sample{{Timestamp: 1000, Value: 1.0}}

	if err := block.AddSeries(s, samples); err != nil {
		t.Fatalf("AddSeries failed: %v", err)
	}

	if err := block.Persist(tmpDir); err != nil {
		t.Fatalf("Persist failed: %v", err)
	}

	blockDir := block.Dir()

	// Verify block exists
	if _, err := os.Stat(blockDir); os.IsNotExist(err) {
		t.Fatal("block directory does not exist")
	}

	// Delete block
	if err := block.Delete(); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify block is deleted
	if _, err := os.Stat(blockDir); !os.IsNotExist(err) {
		t.Error("block directory still exists after deletion")
	}
}

// TestBlockString tests the String method
func TestBlockString(t *testing.T) {
	block, err := NewBlock(1000, 5000)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	s := series.NewSeries(map[string]string{"__name__": "metric1"})
	samples := []series.Sample{
		{Timestamp: 1000, Value: 1.0},
		{Timestamp: 2000, Value: 2.0},
	}

	if err := block.AddSeries(s, samples); err != nil {
		t.Fatalf("AddSeries failed: %v", err)
	}

	str := block.String()
	if str == "" {
		t.Error("String() returned empty string")
	}

	t.Logf("Block string: %s", str)
}
