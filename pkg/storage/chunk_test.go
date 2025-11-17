package storage

import (
	"bytes"
	"math"
	"testing"

	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

func TestChunk_AppendAndSeal(t *testing.T) {
	chunk := NewChunk()

	// Append samples
	baseTime := int64(1640000000000)
	for i := 0; i < 10; i++ {
		sample := series.Sample{
			Timestamp: baseTime + int64(i)*60000,
			Value:     float64(100 + i),
		}
		err := chunk.Append(sample)
		if err != nil {
			t.Fatalf("Append failed: %v", err)
		}
	}

	// Verify chunk state
	if chunk.NumSamples != 10 {
		t.Errorf("Expected 10 samples, got %d", chunk.NumSamples)
	}

	if chunk.MinTime != baseTime {
		t.Errorf("Expected minTime %d, got %d", baseTime, chunk.MinTime)
	}

	expectedMaxTime := baseTime + 9*60000
	if chunk.MaxTime != expectedMaxTime {
		t.Errorf("Expected maxTime %d, got %d", expectedMaxTime, chunk.MaxTime)
	}

	// Seal the chunk
	err := chunk.Seal()
	if err != nil {
		t.Fatalf("Seal failed: %v", err)
	}

	if !chunk.IsSealed() {
		t.Error("Chunk should be sealed")
	}
}

func TestChunk_AppendOrdering(t *testing.T) {
	chunk := NewChunk()

	// Append first sample
	chunk.Append(series.Sample{Timestamp: 1000, Value: 1.0})

	// Try to append sample with earlier timestamp
	err := chunk.Append(series.Sample{Timestamp: 500, Value: 2.0})
	if err == nil {
		t.Error("Expected error when appending out-of-order sample")
	}

	// Try to append sample with same timestamp
	err = chunk.Append(series.Sample{Timestamp: 1000, Value: 2.0})
	if err == nil {
		t.Error("Expected error when appending duplicate timestamp")
	}
}

func TestChunk_AppendAfterSeal(t *testing.T) {
	chunk := NewChunk()
	chunk.Append(series.Sample{Timestamp: 1000, Value: 1.0})
	chunk.Seal()

	// Try to append after sealing
	err := chunk.Append(series.Sample{Timestamp: 2000, Value: 2.0})
	if err == nil {
		t.Error("Expected error when appending to sealed chunk")
	}
}

func TestChunk_Iterator(t *testing.T) {
	chunk := NewChunk()

	// Create test samples
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

	// Iterate and verify
	it, err := chunk.Iterator()
	if err != nil {
		t.Fatalf("Iterator creation failed: %v", err)
	}

	i := 0
	for it.Next() {
		sample, err := it.At()
		if err != nil {
			t.Fatalf("Iterator.At() failed: %v", err)
		}

		if sample.Timestamp != expectedSamples[i].Timestamp {
			t.Errorf("Sample %d: expected timestamp %d, got %d", i, expectedSamples[i].Timestamp, sample.Timestamp)
		}

		if sample.Value != expectedSamples[i].Value {
			t.Errorf("Sample %d: expected value %f, got %f", i, expectedSamples[i].Value, sample.Value)
		}

		i++
	}

	if i != len(expectedSamples) {
		t.Errorf("Expected %d samples, iterated %d", len(expectedSamples), i)
	}

	if it.Err() != nil {
		t.Errorf("Iterator error: %v", it.Err())
	}
}

func TestChunk_MarshalUnmarshal(t *testing.T) {
	// Create and populate chunk
	chunk := NewChunk()
	baseTime := int64(1640000000000)
	for i := 0; i < 50; i++ {
		chunk.Append(series.Sample{
			Timestamp: baseTime + int64(i)*60000,
			Value:     100.0 + float64(i)*0.5,
		})
	}
	chunk.Seal()

	// Marshal
	data, err := chunk.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	if len(data) == 0 {
		t.Fatal("Expected non-empty marshaled data")
	}

	// Unmarshal
	chunk2 := &Chunk{}
	err = chunk2.UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	// Verify metadata
	if chunk2.MinTime != chunk.MinTime {
		t.Errorf("MinTime mismatch: expected %d, got %d", chunk.MinTime, chunk2.MinTime)
	}

	if chunk2.MaxTime != chunk.MaxTime {
		t.Errorf("MaxTime mismatch: expected %d, got %d", chunk.MaxTime, chunk2.MaxTime)
	}

	if chunk2.NumSamples != chunk.NumSamples {
		t.Errorf("NumSamples mismatch: expected %d, got %d", chunk.NumSamples, chunk2.NumSamples)
	}

	// Verify samples by iterating both chunks
	it1, _ := chunk.Iterator()
	it2, _ := chunk2.Iterator()

	i := 0
	for it1.Next() && it2.Next() {
		s1, _ := it1.At()
		s2, _ := it2.At()

		if s1.Timestamp != s2.Timestamp {
			t.Errorf("Sample %d: timestamp mismatch: %d != %d", i, s1.Timestamp, s2.Timestamp)
		}

		if s1.Value != s2.Value {
			t.Errorf("Sample %d: value mismatch: %f != %f", i, s1.Value, s2.Value)
		}

		i++
	}
}

func TestChunk_CorruptedChecksum(t *testing.T) {
	chunk := NewChunk()
	chunk.Append(series.Sample{Timestamp: 1000, Value: 1.0})
	chunk.Seal()

	data, _ := chunk.MarshalBinary()

	// Corrupt the checksum (last 4 bytes)
	data[len(data)-1] ^= 0xFF

	// Try to unmarshal
	chunk2 := &Chunk{}
	err := chunk2.UnmarshalBinary(data)
	if err == nil {
		t.Error("Expected error for corrupted checksum")
	}
}

func TestChunk_CorruptedData(t *testing.T) {
	chunk := NewChunk()
	chunk.Append(series.Sample{Timestamp: 1000, Value: 1.0})
	chunk.Seal()

	data, _ := chunk.MarshalBinary()

	// Corrupt the data section
	data[ChunkHeaderSize+5] ^= 0xFF

	// Try to unmarshal
	chunk2 := &Chunk{}
	err := chunk2.UnmarshalBinary(data)
	if err == nil {
		t.Error("Expected error for corrupted data (checksum should fail)")
	}
}

func TestChunk_Contains(t *testing.T) {
	chunk := NewChunk()
	chunk.Append(series.Sample{Timestamp: 1000, Value: 1.0})
	chunk.Append(series.Sample{Timestamp: 2000, Value: 2.0})
	chunk.Append(series.Sample{Timestamp: 3000, Value: 3.0})
	chunk.Seal()

	tests := []struct {
		timestamp int64
		expected  bool
	}{
		{500, false},
		{1000, true},
		{1500, true},
		{2000, true},
		{2500, true},
		{3000, true},
		{3500, false},
	}

	for _, tt := range tests {
		result := chunk.Contains(tt.timestamp)
		if result != tt.expected {
			t.Errorf("Contains(%d): expected %v, got %v", tt.timestamp, tt.expected, result)
		}
	}
}

func TestChunk_Size(t *testing.T) {
	chunk := NewChunk()

	// Size should be 0 for unsealed chunk
	if chunk.Size() != 0 {
		t.Errorf("Expected size 0 for unsealed chunk, got %d", chunk.Size())
	}

	for i := 0; i < 10; i++ {
		chunk.Append(series.Sample{
			Timestamp: int64(1000 + i*1000),
			Value:     float64(i),
		})
	}
	chunk.Seal()

	size := chunk.Size()
	if size == 0 {
		t.Error("Expected non-zero size for sealed chunk")
	}

	// Size should match marshaled data length
	data, _ := chunk.MarshalBinary()
	if size != len(data) {
		t.Errorf("Size mismatch: Size() returned %d, marshaled data is %d bytes", size, len(data))
	}
}

func TestChunk_IsFull(t *testing.T) {
	chunk := NewChunk()

	if chunk.IsFull() {
		t.Error("New chunk should not be full")
	}

	// Fill to capacity
	for i := 0; i < DefaultChunkSize; i++ {
		chunk.Append(series.Sample{
			Timestamp: int64(1000 + i*1000),
			Value:     float64(i),
		})
	}

	if !chunk.IsFull() {
		t.Error("Chunk should be full after reaching capacity")
	}

	// Try to append beyond capacity
	err := chunk.Append(series.Sample{Timestamp: 999999, Value: 1.0})
	if err == nil {
		t.Error("Expected error when appending to full chunk")
	}
}

func TestChunk_WriteTo(t *testing.T) {
	chunk := NewChunk()
	for i := 0; i < 10; i++ {
		chunk.Append(series.Sample{
			Timestamp: int64(1000 + i*1000),
			Value:     float64(i),
		})
	}
	chunk.Seal()

	var buf bytes.Buffer
	n, err := chunk.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	if n != int64(chunk.Size()) {
		t.Errorf("WriteTo returned %d bytes, expected %d", n, chunk.Size())
	}

	// Verify data can be unmarshaled
	chunk2 := &Chunk{}
	err = chunk2.UnmarshalBinary(buf.Bytes())
	if err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	if chunk2.NumSamples != chunk.NumSamples {
		t.Errorf("NumSamples mismatch after WriteTo/Unmarshal")
	}
}

func TestChunk_ReadFrom(t *testing.T) {
	// Create and marshal a chunk
	chunk := NewChunk()
	for i := 0; i < 10; i++ {
		chunk.Append(series.Sample{
			Timestamp: int64(1000 + i*1000),
			Value:     float64(i),
		})
	}
	chunk.Seal()

	data, _ := chunk.MarshalBinary()

	// Read from buffer
	buf := bytes.NewReader(data)
	chunk2 := &Chunk{}
	n, err := chunk2.ReadFrom(buf)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	if n != int64(len(data)) {
		t.Errorf("ReadFrom returned %d bytes, expected %d", n, len(data))
	}

	if chunk2.NumSamples != chunk.NumSamples {
		t.Errorf("NumSamples mismatch after ReadFrom")
	}
}

func TestChunk_SpecialFloatValues(t *testing.T) {
	chunk := NewChunk()

	specialValues := []float64{
		0.0,
		-0.0,
		math.Inf(1),
		math.Inf(-1),
		math.NaN(),
		1e-100,
		1e100,
	}

	baseTime := int64(1000)
	for i, val := range specialValues {
		chunk.Append(series.Sample{
			Timestamp: baseTime + int64(i)*1000,
			Value:     val,
		})
	}

	chunk.Seal()

	// Iterate and verify
	it, _ := chunk.Iterator()
	i := 0
	for it.Next() {
		sample, _ := it.At()
		expected := specialValues[i]

		if math.IsNaN(expected) {
			if !math.IsNaN(sample.Value) {
				t.Errorf("Sample %d: expected NaN, got %f", i, sample.Value)
			}
		} else if sample.Value != expected {
			t.Errorf("Sample %d: expected %f, got %f", i, expected, sample.Value)
		}

		i++
	}
}

func TestChunk_EmptyChunkSeal(t *testing.T) {
	chunk := NewChunk()

	err := chunk.Seal()
	if err == nil {
		t.Error("Expected error when sealing empty chunk")
	}
}

func TestChunk_IteratorBeforeSeal(t *testing.T) {
	chunk := NewChunk()
	chunk.Append(series.Sample{Timestamp: 1000, Value: 1.0})

	_, err := chunk.Iterator()
	if err == nil {
		t.Error("Expected error when creating iterator for unsealed chunk")
	}
}

func TestChunk_CompressionRatio(t *testing.T) {
	chunk := NewChunk()

	// Create realistic time-series data
	baseTime := int64(1640000000000)
	count := DefaultChunkSize

	for i := 0; i < count; i++ {
		chunk.Append(series.Sample{
			Timestamp: baseTime + int64(i)*60000, // 1-minute intervals
			Value:     100.0 + float64(i)*0.1,    // Slowly changing values
		})
	}

	chunk.Seal()

	uncompressedSize := count * 16 // 8 bytes timestamp + 8 bytes value
	compressedSize := chunk.Size()
	ratio := float64(uncompressedSize) / float64(compressedSize)

	t.Logf("Compression stats:")
	t.Logf("  Samples: %d", count)
	t.Logf("  Uncompressed: %d bytes", uncompressedSize)
	t.Logf("  Compressed: %d bytes", compressedSize)
	t.Logf("  Ratio: %.2fx", ratio)
	t.Logf("  Bytes per sample: %.2f", float64(compressedSize)/float64(count))

	// We expect at least 2x compression for regular intervals
	if ratio < 2.0 {
		t.Logf("Warning: compression ratio %.2fx is lower than expected", ratio)
	}
}

func TestChunk_LargeDataset(t *testing.T) {
	chunk := NewChunk()

	baseTime := int64(1640000000000)
	count := DefaultChunkSize

	for i := 0; i < count; i++ {
		chunk.Append(series.Sample{
			Timestamp: baseTime + int64(i)*60000,
			Value:     100.0 + float64(i)*0.5,
		})
	}

	chunk.Seal()

	// Marshal and unmarshal
	data, err := chunk.MarshalBinary()
	if err != nil {
		t.Fatalf("MarshalBinary failed: %v", err)
	}

	chunk2 := &Chunk{}
	err = chunk2.UnmarshalBinary(data)
	if err != nil {
		t.Fatalf("UnmarshalBinary failed: %v", err)
	}

	// Verify all samples
	it1, _ := chunk.Iterator()
	it2, _ := chunk2.Iterator()

	i := 0
	for it1.Next() && it2.Next() {
		s1, _ := it1.At()
		s2, _ := it2.At()

		if s1 != s2 {
			t.Errorf("Sample %d mismatch: %+v != %+v", i, s1, s2)
		}
		i++
	}

	if i != count {
		t.Errorf("Expected %d samples, got %d", count, i)
	}
}

// Benchmark tests
func BenchmarkChunk_Append(b *testing.B) {
	baseTime := int64(1640000000000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chunk := NewChunk()
		for j := 0; j < DefaultChunkSize; j++ {
			chunk.Append(series.Sample{
				Timestamp: baseTime + int64(j)*60000,
				Value:     float64(j),
			})
		}
	}

	b.ReportMetric(float64(b.N*DefaultChunkSize)/b.Elapsed().Seconds(), "appends/sec")
}

func BenchmarkChunk_Seal(b *testing.B) {
	// Prepare chunks
	chunks := make([]*Chunk, b.N)
	baseTime := int64(1640000000000)

	for i := 0; i < b.N; i++ {
		chunk := NewChunk()
		for j := 0; j < DefaultChunkSize; j++ {
			chunk.Append(series.Sample{
				Timestamp: baseTime + int64(j)*60000,
				Value:     float64(j),
			})
		}
		chunks[i] = chunk
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chunks[i].Seal()
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "seals/sec")
}

func BenchmarkChunk_Iterator(b *testing.B) {
	chunk := NewChunk()
	baseTime := int64(1640000000000)

	for i := 0; i < DefaultChunkSize; i++ {
		chunk.Append(series.Sample{
			Timestamp: baseTime + int64(i)*60000,
			Value:     float64(i),
		})
	}
	chunk.Seal()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it, _ := chunk.Iterator()
		for it.Next() {
			it.At()
		}
	}

	b.ReportMetric(float64(b.N*DefaultChunkSize)/b.Elapsed().Seconds(), "samples/sec")
}

func BenchmarkChunk_MarshalBinary(b *testing.B) {
	chunk := NewChunk()
	baseTime := int64(1640000000000)

	for i := 0; i < DefaultChunkSize; i++ {
		chunk.Append(series.Sample{
			Timestamp: baseTime + int64(i)*60000,
			Value:     float64(i),
		})
	}
	chunk.Seal()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chunk.MarshalBinary()
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "marshals/sec")
}

func BenchmarkChunk_UnmarshalBinary(b *testing.B) {
	chunk := NewChunk()
	baseTime := int64(1640000000000)

	for i := 0; i < DefaultChunkSize; i++ {
		chunk.Append(series.Sample{
			Timestamp: baseTime + int64(i)*60000,
			Value:     float64(i),
		})
	}
	chunk.Seal()

	data, _ := chunk.MarshalBinary()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c := &Chunk{}
		c.UnmarshalBinary(data)
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "unmarshals/sec")
}
