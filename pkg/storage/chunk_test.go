package storage

import (
	"bytes"
	"testing"

	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

// TestChunkAppendAndIterate tests basic chunk operations
func TestChunkAppendAndIterate(t *testing.T) {
	// Create sample data
	samples := []series.Sample{
		{Timestamp: 1000, Value: 1.5},
		{Timestamp: 2000, Value: 2.5},
		{Timestamp: 3000, Value: 3.5},
		{Timestamp: 4000, Value: 4.5},
		{Timestamp: 5000, Value: 5.5},
	}

	// Create and populate chunk
	chunk := NewChunk()
	if err := chunk.Append(samples); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Verify metadata
	if chunk.MinTime != 1000 {
		t.Errorf("MinTime: got %d, want 1000", chunk.MinTime)
	}
	if chunk.MaxTime != 5000 {
		t.Errorf("MaxTime: got %d, want 5000", chunk.MaxTime)
	}
	if chunk.NumSamples != 5 {
		t.Errorf("NumSamples: got %d, want 5", chunk.NumSamples)
	}

	// Iterate and verify samples
	iter, err := chunk.Iterator()
	if err != nil {
		t.Fatalf("Iterator failed: %v", err)
	}

	i := 0
	for iter.Next() {
		sample, err := iter.At()
		if err != nil {
			t.Fatalf("At failed: %v", err)
		}

		if sample.Timestamp != samples[i].Timestamp {
			t.Errorf("sample %d timestamp: got %d, want %d", i, sample.Timestamp, samples[i].Timestamp)
		}
		if sample.Value != samples[i].Value {
			t.Errorf("sample %d value: got %f, want %f", i, sample.Value, samples[i].Value)
		}

		i++
	}

	if iter.Err() != nil {
		t.Errorf("Iterator error: %v", iter.Err())
	}

	if i != len(samples) {
		t.Errorf("iterated %d samples, want %d", i, len(samples))
	}
}

// TestChunkMarshalUnmarshal tests serialization
func TestChunkMarshalUnmarshal(t *testing.T) {
	// Create sample data
	samples := []series.Sample{
		{Timestamp: 1000, Value: 1.5},
		{Timestamp: 2000, Value: 2.5},
		{Timestamp: 3000, Value: 3.5},
	}

	// Create and populate chunk
	original := NewChunk()
	if err := original.Append(samples); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Marshal
	data, err := original.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	// Unmarshal
	restored := NewChunk()
	if err := restored.UnmarshalBinary(data); err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	// Verify metadata
	if restored.MinTime != original.MinTime {
		t.Errorf("MinTime: got %d, want %d", restored.MinTime, original.MinTime)
	}
	if restored.MaxTime != original.MaxTime {
		t.Errorf("MaxTime: got %d, want %d", restored.MaxTime, original.MaxTime)
	}
	if restored.NumSamples != original.NumSamples {
		t.Errorf("NumSamples: got %d, want %d", restored.NumSamples, original.NumSamples)
	}
	if restored.Checksum != original.Checksum {
		t.Errorf("Checksum: got %d, want %d", restored.Checksum, original.Checksum)
	}

	// Verify samples by iteration
	iter, err := restored.Iterator()
	if err != nil {
		t.Fatalf("Iterator failed: %v", err)
	}

	i := 0
	for iter.Next() {
		sample, err := iter.At()
		if err != nil {
			t.Fatalf("At failed: %v", err)
		}

		if sample.Timestamp != samples[i].Timestamp {
			t.Errorf("sample %d timestamp: got %d, want %d", i, sample.Timestamp, samples[i].Timestamp)
		}
		if sample.Value != samples[i].Value {
			t.Errorf("sample %d value: got %f, want %f", i, sample.Value, samples[i].Value)
		}

		i++
	}
}

// TestChunkWriteRead tests I/O operations
func TestChunkWriteRead(t *testing.T) {
	// Create sample data
	samples := []series.Sample{
		{Timestamp: 1000, Value: 1.5},
		{Timestamp: 2000, Value: 2.5},
		{Timestamp: 3000, Value: 3.5},
	}

	// Create and populate chunk
	original := NewChunk()
	if err := original.Append(samples); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Write to buffer
	buf := &bytes.Buffer{}
	n, err := original.WriteTo(buf)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	if n != int64(original.Size()) {
		t.Errorf("WriteTo returned %d bytes, chunk size is %d", n, original.Size())
	}

	// Read from buffer
	restored := NewChunk()
	n2, err := restored.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	if n2 != n {
		t.Errorf("ReadFrom read %d bytes, WriteTo wrote %d", n2, n)
	}

	// Verify metadata
	if restored.MinTime != original.MinTime {
		t.Errorf("MinTime: got %d, want %d", restored.MinTime, original.MinTime)
	}
	if restored.NumSamples != original.NumSamples {
		t.Errorf("NumSamples: got %d, want %d", restored.NumSamples, original.NumSamples)
	}
}

// TestChunkCompressionRatio tests compression effectiveness
func TestChunkCompressionRatio(t *testing.T) {
	// Create regular-interval samples (should compress well)
	samples := make([]series.Sample, 120)
	baseTime := int64(1640000000000)
	baseValue := 0.75

	for i := 0; i < 120; i++ {
		samples[i] = series.Sample{
			Timestamp: baseTime + int64(i*60000), // +1 minute each
			Value:     baseValue + float64(i/60)*0.001, // Slowly changing
		}
	}

	chunk := NewChunk()
	if err := chunk.Append(samples); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	ratio := chunk.CompressionRatio()
	t.Logf("Compression ratio: %.2fx", ratio)
	t.Logf("Uncompressed: %d bytes", len(samples)*16)
	t.Logf("Compressed: %d bytes", len(chunk.Data))
	t.Logf("Bytes per sample: %.2f", float64(len(chunk.Data))/float64(len(samples)))

	// We expect at least 5x compression for this regular data
	if ratio < 5.0 {
		t.Errorf("compression ratio too low: got %.2fx, want >5x", ratio)
	}
}

// TestChunkBuilder tests the chunk builder
func TestChunkBuilder(t *testing.T) {
	builder := NewChunkBuilder(5)

	// Add samples
	for i := 0; i < 5; i++ {
		sample := series.Sample{
			Timestamp: int64(1000 + i*1000),
			Value:     float64(i) + 0.5,
		}

		added := builder.Add(sample)
		if !added {
			t.Errorf("failed to add sample %d", i)
		}
	}

	// Check if full
	if !builder.IsFull() {
		t.Error("builder should be full")
	}

	// Try to add one more (should fail)
	extraSample := series.Sample{Timestamp: 6000, Value: 5.5}
	if builder.Add(extraSample) {
		t.Error("should not be able to add sample to full builder")
	}

	// Build chunk
	chunk, err := builder.Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if chunk.NumSamples != 5 {
		t.Errorf("NumSamples: got %d, want 5", chunk.NumSamples)
	}

	// Reset and reuse
	builder.Reset()
	if builder.Count() != 0 {
		t.Errorf("Count after reset: got %d, want 0", builder.Count())
	}

	// Add new samples
	sample := series.Sample{Timestamp: 7000, Value: 6.5}
	if !builder.Add(sample) {
		t.Error("failed to add sample after reset")
	}

	if builder.Count() != 1 {
		t.Errorf("Count: got %d, want 1", builder.Count())
	}
}

// TestChunkEmptySamples tests error handling for empty samples
func TestChunkEmptySamples(t *testing.T) {
	chunk := NewChunk()
	err := chunk.Append([]series.Sample{})

	if err == nil {
		t.Error("Append should fail with empty samples")
	}
}

// TestChunkCorruption tests checksum verification
func TestChunkCorruption(t *testing.T) {
	// Create valid chunk
	samples := []series.Sample{
		{Timestamp: 1000, Value: 1.5},
		{Timestamp: 2000, Value: 2.5},
	}

	chunk := NewChunk()
	if err := chunk.Append(samples); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	data, err := chunk.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	// Corrupt the data
	corruptedData := make([]byte, len(data))
	copy(corruptedData, data)
	corruptedData[ChunkHeaderSize] ^= 0xFF // Flip bits in data section

	// Try to unmarshal corrupted data
	corrupted := NewChunk()
	err = corrupted.UnmarshalBinary(corruptedData)

	if err == nil {
		t.Error("UnmarshalBinary should fail with corrupted data")
	}
}

// TestChunkLargeDataset tests chunk with many samples
func TestChunkLargeDataset(t *testing.T) {
	// Create 1000 samples
	samples := make([]series.Sample, 1000)
	baseTime := int64(1640000000000)

	for i := 0; i < 1000; i++ {
		samples[i] = series.Sample{
			Timestamp: baseTime + int64(i*1000),
			Value:     float64(i) * 0.1,
		}
	}

	chunk := NewChunk()
	if err := chunk.Append(samples); err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	// Verify all samples can be read back
	iter, err := chunk.Iterator()
	if err != nil {
		t.Fatalf("Iterator failed: %v", err)
	}

	count := 0
	for iter.Next() {
		_, err := iter.At()
		if err != nil {
			t.Fatalf("At failed at sample %d: %v", count, err)
		}
		count++
	}

	if count != 1000 {
		t.Errorf("iterated %d samples, want 1000", count)
	}

	t.Logf("Large dataset compression: %.2fx", chunk.CompressionRatio())
}
