package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

func TestNewBlock(t *testing.T) {
	dir := t.TempDir()

	minTime := int64(1640000000000)
	maxTime := int64(1640007200000) // 2 hours later

	block, err := NewBlock(minTime, maxTime, dir)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	// Verify metadata
	if block.Meta().MinTime != minTime {
		t.Errorf("Expected minTime %d, got %d", minTime, block.Meta().MinTime)
	}

	if block.Meta().MaxTime != maxTime {
		t.Errorf("Expected maxTime %d, got %d", maxTime, block.Meta().MaxTime)
	}

	if block.Meta().Version != BlockVersion {
		t.Errorf("Expected version %d, got %d", BlockVersion, block.Meta().Version)
	}

	// Verify directories were created
	if _, err := os.Stat(block.Dir()); os.IsNotExist(err) {
		t.Error("Block directory was not created")
	}

	chunksDir := filepath.Join(block.Dir(), ChunksDirName)
	if _, err := os.Stat(chunksDir); os.IsNotExist(err) {
		t.Error("Chunks directory was not created")
	}
}

func TestBlock_WriteAndReadChunk(t *testing.T) {
	dir := t.TempDir()

	block, err := NewBlock(1640000000000, 1640007200000, dir)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	// Create a chunk
	chunk := NewChunk()
	baseTime := int64(1640000000000)
	for i := 0; i < 10; i++ {
		chunk.Append(series.Sample{
			Timestamp: baseTime + int64(i)*60000,
			Value:     float64(100 + i),
		})
	}
	chunk.Seal()

	// Write chunk
	seriesHash := uint64(12345)
	err = block.WriteChunk(seriesHash, chunk)
	if err != nil {
		t.Fatalf("WriteChunk failed: %v", err)
	}

	// Read chunks back
	chunks, err := block.ReadChunks(seriesHash)
	if err != nil {
		t.Fatalf("ReadChunks failed: %v", err)
	}

	if len(chunks) != 1 {
		t.Fatalf("Expected 1 chunk, got %d", len(chunks))
	}

	// Verify chunk data
	readChunk := chunks[0]
	if readChunk.NumSamples != chunk.NumSamples {
		t.Errorf("NumSamples mismatch: expected %d, got %d", chunk.NumSamples, readChunk.NumSamples)
	}

	if readChunk.MinTime != chunk.MinTime {
		t.Errorf("MinTime mismatch: expected %d, got %d", chunk.MinTime, readChunk.MinTime)
	}

	if readChunk.MaxTime != chunk.MaxTime {
		t.Errorf("MaxTime mismatch: expected %d, got %d", chunk.MaxTime, readChunk.MaxTime)
	}
}

func TestBlock_WriteMultipleChunks(t *testing.T) {
	dir := t.TempDir()

	block, err := NewBlock(1640000000000, 1640014400000, dir)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	seriesHash := uint64(12345)
	baseTime := int64(1640000000000)

	// Write multiple chunks for the same series
	for chunkIdx := 0; chunkIdx < 3; chunkIdx++ {
		chunk := NewChunk()
		for i := 0; i < 10; i++ {
			chunk.Append(series.Sample{
				Timestamp: baseTime + int64(chunkIdx*10+i)*60000,
				Value:     float64(100 + chunkIdx*10 + i),
			})
		}
		chunk.Seal()

		err = block.WriteChunk(seriesHash, chunk)
		if err != nil {
			t.Fatalf("WriteChunk %d failed: %v", chunkIdx, err)
		}
	}

	// Read all chunks
	chunks, err := block.ReadChunks(seriesHash)
	if err != nil {
		t.Fatalf("ReadChunks failed: %v", err)
	}

	if len(chunks) != 3 {
		t.Fatalf("Expected 3 chunks, got %d", len(chunks))
	}

	// Verify block stats
	meta := block.Meta()
	if meta.Stats.NumChunks != 3 {
		t.Errorf("Expected 3 chunks in stats, got %d", meta.Stats.NumChunks)
	}

	if meta.Stats.NumSamples != 30 {
		t.Errorf("Expected 30 samples in stats, got %d", meta.Stats.NumSamples)
	}
}

func TestBlock_WriteMeta(t *testing.T) {
	dir := t.TempDir()

	block, err := NewBlock(1640000000000, 1640007200000, dir)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	// Write some chunks to populate stats
	chunk := NewChunk()
	for i := 0; i < 10; i++ {
		chunk.Append(series.Sample{
			Timestamp: int64(1640000000000 + i*60000),
			Value:     float64(i),
		})
	}
	chunk.Seal()
	block.WriteChunk(12345, chunk)
	block.IncrementSeriesCount()

	// Write metadata
	err = block.WriteMeta()
	if err != nil {
		t.Fatalf("WriteMeta failed: %v", err)
	}

	// Verify meta file exists
	metaPath := filepath.Join(block.Dir(), MetaFilename)
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		t.Error("Meta file was not created")
	}
}

func TestOpenBlock(t *testing.T) {
	dir := t.TempDir()

	// Create and write a block
	block1, err := NewBlock(1640000000000, 1640007200000, dir)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	chunk := NewChunk()
	for i := 0; i < 10; i++ {
		chunk.Append(series.Sample{
			Timestamp: int64(1640000000000 + i*60000),
			Value:     float64(i),
		})
	}
	chunk.Seal()
	block1.WriteChunk(12345, chunk)
	block1.IncrementSeriesCount()

	err = block1.WriteMeta()
	if err != nil {
		t.Fatalf("WriteMeta failed: %v", err)
	}

	// Open the block
	block2, err := OpenBlock(block1.Dir())
	if err != nil {
		t.Fatalf("OpenBlock failed: %v", err)
	}

	// Verify metadata matches
	if block2.Meta().ULID != block1.Meta().ULID {
		t.Errorf("ULID mismatch: expected %s, got %s", block1.Meta().ULID, block2.Meta().ULID)
	}

	if block2.Meta().MinTime != block1.Meta().MinTime {
		t.Errorf("MinTime mismatch: expected %d, got %d", block1.Meta().MinTime, block2.Meta().MinTime)
	}

	if block2.Meta().Stats.NumSeries != block1.Meta().Stats.NumSeries {
		t.Errorf("NumSeries mismatch: expected %d, got %d", block1.Meta().Stats.NumSeries, block2.Meta().Stats.NumSeries)
	}

	// Read chunks from reopened block
	chunks, err := block2.ReadChunks(12345)
	if err != nil {
		t.Fatalf("ReadChunks failed: %v", err)
	}

	if len(chunks) != 1 {
		t.Fatalf("Expected 1 chunk, got %d", len(chunks))
	}
}

func TestBlock_Query(t *testing.T) {
	dir := t.TempDir()

	block, err := NewBlock(1640000000000, 1640007200000, dir)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	// Write test data
	seriesHash := uint64(12345)
	chunk := NewChunk()
	baseTime := int64(1640000000000)

	expectedSamples := make([]series.Sample, 20)
	for i := 0; i < 20; i++ {
		sample := series.Sample{
			Timestamp: baseTime + int64(i)*60000,
			Value:     float64(100 + i),
		}
		expectedSamples[i] = sample
		chunk.Append(sample)
	}
	chunk.Seal()
	block.WriteChunk(seriesHash, chunk)

	// Query full range
	samples, err := block.Query(seriesHash, baseTime, baseTime+19*60000)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(samples) != 20 {
		t.Errorf("Expected 20 samples, got %d", len(samples))
	}

	// Query partial range
	samples, err = block.Query(seriesHash, baseTime+5*60000, baseTime+10*60000)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(samples) != 6 { // Timestamps 5, 6, 7, 8, 9, 10
		t.Errorf("Expected 6 samples, got %d", len(samples))
	}

	// Verify sample values
	for i, sample := range samples {
		expectedIdx := 5 + i
		if sample.Timestamp != expectedSamples[expectedIdx].Timestamp {
			t.Errorf("Sample %d: timestamp mismatch", i)
		}
		if sample.Value != expectedSamples[expectedIdx].Value {
			t.Errorf("Sample %d: value mismatch", i)
		}
	}
}

func TestBlock_QueryOutOfRange(t *testing.T) {
	dir := t.TempDir()

	block, err := NewBlock(1640000000000, 1640007200000, dir)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	// Write test data
	chunk := NewChunk()
	baseTime := int64(1640001000000) // 1 million ms after block start
	for i := 0; i < 10; i++ {
		chunk.Append(series.Sample{
			Timestamp: baseTime + int64(i)*60000,
			Value:     float64(i),
		})
	}
	chunk.Seal()
	block.WriteChunk(12345, chunk)

	// Query before block range
	samples, err := block.Query(12345, 1630000000000, 1630001000000)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(samples) != 0 {
		t.Errorf("Expected 0 samples for out-of-range query, got %d", len(samples))
	}

	// Query after block range
	samples, err = block.Query(12345, 1650000000000, 1650001000000)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if len(samples) != 0 {
		t.Errorf("Expected 0 samples for out-of-range query, got %d", len(samples))
	}
}

func TestBlock_QueryNonexistentSeries(t *testing.T) {
	dir := t.TempDir()

	block, err := NewBlock(1640000000000, 1640007200000, dir)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	// Query series that doesn't exist
	samples, err := block.Query(99999, 1640000000000, 1640007200000)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}

	if samples != nil && len(samples) != 0 {
		t.Errorf("Expected nil or empty samples for nonexistent series, got %d", len(samples))
	}
}

func TestBlock_Contains(t *testing.T) {
	dir := t.TempDir()

	minTime := int64(1640000000000)
	maxTime := int64(1640007200000)

	block, err := NewBlock(minTime, maxTime, dir)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	tests := []struct {
		timestamp int64
		expected  bool
	}{
		{minTime - 1, false},
		{minTime, true},
		{minTime + 1000000, true},
		{maxTime, true},
		{maxTime + 1, false},
	}

	for _, tt := range tests {
		result := block.Contains(tt.timestamp)
		if result != tt.expected {
			t.Errorf("Contains(%d): expected %v, got %v", tt.timestamp, tt.expected, result)
		}
	}
}

func TestBlock_Overlaps(t *testing.T) {
	dir := t.TempDir()

	minTime := int64(1640000000000)
	maxTime := int64(1640007200000)

	block, err := NewBlock(minTime, maxTime, dir)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	tests := []struct {
		name     string
		minTime  int64
		maxTime  int64
		expected bool
	}{
		{"Before block", minTime - 10000000, minTime - 1, false},
		{"After block", maxTime + 1, maxTime + 10000000, false},
		{"Overlaps start", minTime - 1000000, minTime + 1000000, true},
		{"Overlaps end", maxTime - 1000000, maxTime + 1000000, true},
		{"Contains block", minTime - 1000000, maxTime + 1000000, true},
		{"Within block", minTime + 1000000, maxTime - 1000000, true},
		{"Exact match", minTime, maxTime, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := block.Overlaps(tt.minTime, tt.maxTime)
			if result != tt.expected {
				t.Errorf("Overlaps(%d, %d): expected %v, got %v", tt.minTime, tt.maxTime, tt.expected, result)
			}
		})
	}
}

func TestBlock_Validate(t *testing.T) {
	dir := t.TempDir()

	block, err := NewBlock(1640000000000, 1640007200000, dir)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	// Write some chunks
	for seriesIdx := 0; seriesIdx < 3; seriesIdx++ {
		chunk := NewChunk()
		for i := 0; i < 10; i++ {
			chunk.Append(series.Sample{
				Timestamp: int64(1640000000000 + i*60000),
				Value:     float64(i),
			})
		}
		chunk.Seal()
		block.WriteChunk(uint64(seriesIdx), chunk)
		block.IncrementSeriesCount()
	}

	block.WriteMeta()

	// Validate should pass
	err = block.Validate()
	if err != nil {
		t.Errorf("Validate failed: %v", err)
	}
}

func TestBlock_ValidateCorrupted(t *testing.T) {
	dir := t.TempDir()

	block, err := NewBlock(1640000000000, 1640007200000, dir)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	chunk := NewChunk()
	for i := 0; i < 10; i++ {
		chunk.Append(series.Sample{
			Timestamp: int64(1640000000000 + i*60000),
			Value:     float64(i),
		})
	}
	chunk.Seal()
	block.WriteChunk(12345, chunk)

	// Write incorrect stats
	block.meta.Stats.NumSamples = 999 // Wrong count
	block.WriteMeta()

	// Validate should fail
	err = block.Validate()
	if err == nil {
		t.Error("Expected validation error for corrupted stats")
	}
}

func TestBlock_Delete(t *testing.T) {
	dir := t.TempDir()

	block, err := NewBlock(1640000000000, 1640007200000, dir)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	blockDir := block.Dir()

	// Write some data
	chunk := NewChunk()
	chunk.Append(series.Sample{Timestamp: 1640000000000, Value: 1.0})
	chunk.Seal()
	block.WriteChunk(12345, chunk)
	block.WriteMeta()

	// Delete block
	err = block.Delete()
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify directory is gone
	if _, err := os.Stat(blockDir); !os.IsNotExist(err) {
		t.Error("Block directory should be deleted")
	}
}

func TestBlock_Size(t *testing.T) {
	dir := t.TempDir()

	block, err := NewBlock(1640000000000, 1640007200000, dir)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	// Write some chunks
	for i := 0; i < 5; i++ {
		chunk := NewChunk()
		for j := 0; j < 20; j++ {
			chunk.Append(series.Sample{
				Timestamp: int64(1640000000000 + (i*20+j)*60000),
				Value:     float64(i*20 + j),
			})
		}
		chunk.Seal()
		block.WriteChunk(uint64(i), chunk)
	}

	block.WriteMeta()

	size, err := block.Size()
	if err != nil {
		t.Fatalf("Size failed: %v", err)
	}

	if size == 0 {
		t.Error("Block size should be non-zero")
	}

	t.Logf("Block size: %d bytes", size)
}

func TestBlock_MultipleSeries(t *testing.T) {
	dir := t.TempDir()

	block, err := NewBlock(1640000000000, 1640007200000, dir)
	if err != nil {
		t.Fatalf("NewBlock failed: %v", err)
	}

	// Write data for multiple series
	seriesCount := 10
	samplesPerSeries := 50

	for seriesIdx := 0; seriesIdx < seriesCount; seriesIdx++ {
		chunk := NewChunk()
		baseTime := int64(1640000000000)

		for i := 0; i < samplesPerSeries; i++ {
			chunk.Append(series.Sample{
				Timestamp: baseTime + int64(i)*60000,
				Value:     float64(seriesIdx*1000 + i),
			})
		}
		chunk.Seal()

		err = block.WriteChunk(uint64(seriesIdx), chunk)
		if err != nil {
			t.Fatalf("WriteChunk for series %d failed: %v", seriesIdx, err)
		}

		block.IncrementSeriesCount()
	}

	block.WriteMeta()

	// Query each series and verify
	for seriesIdx := 0; seriesIdx < seriesCount; seriesIdx++ {
		samples, err := block.Query(uint64(seriesIdx), 1640000000000, 1640007200000)
		if err != nil {
			t.Fatalf("Query for series %d failed: %v", seriesIdx, err)
		}

		if len(samples) != samplesPerSeries {
			t.Errorf("Series %d: expected %d samples, got %d", seriesIdx, samplesPerSeries, len(samples))
		}
	}

	// Verify block stats
	meta := block.Meta()
	if meta.Stats.NumSeries != uint64(seriesCount) {
		t.Errorf("Expected %d series, got %d", seriesCount, meta.Stats.NumSeries)
	}

	expectedSamples := uint64(seriesCount * samplesPerSeries)
	if meta.Stats.NumSamples != expectedSamples {
		t.Errorf("Expected %d samples, got %d", expectedSamples, meta.Stats.NumSamples)
	}
}
