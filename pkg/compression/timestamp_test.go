package compression

import (
	"math/rand"
	"testing"
	"time"
)

func TestTimestampEncoder_SingleTimestamp(t *testing.T) {
	enc := NewTimestampEncoder()
	ts := int64(1640000000000)

	err := enc.Encode(ts)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	data := enc.Finish()
	if len(data) == 0 {
		t.Fatal("Expected non-empty data")
	}

	// Decode
	dec := NewTimestampDecoder(data)
	decoded, err := dec.Decode()
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded != ts {
		t.Errorf("Expected %d, got %d", ts, decoded)
	}
}

func TestTimestampEncoder_TwoTimestamps(t *testing.T) {
	enc := NewTimestampEncoder()
	ts1 := int64(1640000000000)
	ts2 := int64(1640000060000) // 60 seconds later

	enc.Encode(ts1)
	enc.Encode(ts2)

	data := enc.Finish()

	// Decode
	dec := NewTimestampDecoder(data)

	decoded1, _ := dec.Decode()
	if decoded1 != ts1 {
		t.Errorf("Expected %d, got %d", ts1, decoded1)
	}

	decoded2, _ := dec.Decode()
	if decoded2 != ts2 {
		t.Errorf("Expected %d, got %d", ts2, decoded2)
	}
}

func TestTimestampEncoder_RegularInterval(t *testing.T) {
	enc := NewTimestampEncoder()

	// Generate timestamps at regular 60-second intervals
	baseTime := int64(1640000000000)
	interval := int64(60000) // 60 seconds in milliseconds
	count := 100

	timestamps := make([]int64, count)
	for i := 0; i < count; i++ {
		timestamps[i] = baseTime + int64(i)*interval
		err := enc.Encode(timestamps[i])
		if err != nil {
			t.Fatalf("Encode %d failed: %v", i, err)
		}
	}

	data := enc.Finish()

	// Decode and verify
	dec := NewTimestampDecoder(data)
	for i := 0; i < count; i++ {
		decoded, err := dec.Decode()
		if err != nil {
			t.Fatalf("Decode %d failed: %v", i, err)
		}
		if decoded != timestamps[i] {
			t.Errorf("Timestamp %d: expected %d, got %d", i, timestamps[i], decoded)
		}
	}

	// Check compression ratio
	uncompressedSize := count * 8 // 8 bytes per timestamp
	compressedSize := len(data)
	ratio := float64(uncompressedSize) / float64(compressedSize)

	t.Logf("Regular interval compression: %d -> %d bytes (%.2fx)", uncompressedSize, compressedSize, ratio)

	// For regular intervals, we expect excellent compression (should be > 10x)
	if ratio < 10.0 {
		t.Logf("Warning: compression ratio %.2fx is lower than expected for regular intervals", ratio)
	}
}

func TestTimestampEncoder_IrregularInterval(t *testing.T) {
	enc := NewTimestampEncoder()

	// Generate timestamps with varying intervals
	baseTime := int64(1640000000000)
	intervals := []int64{60000, 120000, 30000, 90000, 60000, 150000, 45000}

	timestamps := make([]int64, len(intervals)+1)
	timestamps[0] = baseTime
	enc.Encode(timestamps[0])

	currentTime := baseTime
	for i, interval := range intervals {
		currentTime += interval
		timestamps[i+1] = currentTime
		enc.Encode(timestamps[i+1])
	}

	data := enc.Finish()

	// Decode and verify
	dec := NewTimestampDecoder(data)
	for i := 0; i < len(timestamps); i++ {
		decoded, err := dec.Decode()
		if err != nil {
			t.Fatalf("Decode %d failed: %v", i, err)
		}
		if decoded != timestamps[i] {
			t.Errorf("Timestamp %d: expected %d, got %d", i, timestamps[i], decoded)
		}
	}
}

func TestTimestampEncoder_EdgeCases(t *testing.T) {
	tests := []struct {
		name       string
		timestamps []int64
	}{
		{
			name:       "Zero delta",
			timestamps: []int64{1000, 1000, 1000},
		},
		{
			name:       "Negative delta",
			timestamps: []int64{1000, 900, 800},
		},
		{
			name:       "Large delta",
			timestamps: []int64{0, 1000000000, 2000000000},
		},
		{
			name:       "Small deltas fitting in 7 bits",
			timestamps: []int64{1000, 1010, 1020, 1030},
		},
		{
			name:       "Medium deltas fitting in 9 bits",
			timestamps: []int64{1000, 1100, 1200, 1300},
		},
		{
			name:       "Large deltas fitting in 12 bits",
			timestamps: []int64{1000, 2000, 3000, 4000},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := NewTimestampEncoder()

			for _, ts := range tt.timestamps {
				enc.Encode(ts)
			}

			data := enc.Finish()

			// Decode and verify
			dec := NewTimestampDecoder(data)
			for i, expected := range tt.timestamps {
				decoded, err := dec.Decode()
				if err != nil {
					t.Fatalf("Decode %d failed: %v", i, err)
				}
				if decoded != expected {
					t.Errorf("Timestamp %d: expected %d, got %d", i, expected, decoded)
				}
			}
		})
	}
}

func TestTimestampEncoder_Reset(t *testing.T) {
	enc := NewTimestampEncoder()

	// First encoding
	enc.Encode(1000)
	enc.Encode(2000)
	data1 := enc.Finish()

	// Reset and encode again
	enc.Reset()
	enc.Encode(5000)
	enc.Encode(6000)
	data2 := enc.Finish()

	// Verify both are decodable
	dec := NewTimestampDecoder(data1)
	val, _ := dec.Decode()
	if val != 1000 {
		t.Errorf("First encoding: expected 1000, got %d", val)
	}

	dec.Reset(data2)
	val, _ = dec.Decode()
	if val != 5000 {
		t.Errorf("Second encoding: expected 5000, got %d", val)
	}
}

func TestTimestampEncoder_LargeDataset(t *testing.T) {
	enc := NewTimestampEncoder()

	// Simulate 24 hours of data at 15-second intervals
	baseTime := int64(1640000000000)
	interval := int64(15000) // 15 seconds
	count := 24 * 60 * 60 / 15 // 5,760 samples

	timestamps := make([]int64, count)
	for i := 0; i < count; i++ {
		timestamps[i] = baseTime + int64(i)*interval
		enc.Encode(timestamps[i])
	}

	data := enc.Finish()

	// Verify decoding
	dec := NewTimestampDecoder(data)
	for i := 0; i < count; i++ {
		decoded, err := dec.Decode()
		if err != nil {
			t.Fatalf("Decode %d failed: %v", i, err)
		}
		if decoded != timestamps[i] {
			t.Errorf("Timestamp %d: expected %d, got %d", i, timestamps[i], decoded)
		}
	}

	// Report compression stats
	uncompressedSize := count * 8
	compressedSize := len(data)
	ratio := float64(uncompressedSize) / float64(compressedSize)

	t.Logf("Large dataset compression:")
	t.Logf("  Count: %d timestamps", count)
	t.Logf("  Uncompressed: %d bytes", uncompressedSize)
	t.Logf("  Compressed: %d bytes", compressedSize)
	t.Logf("  Ratio: %.2fx", ratio)
	t.Logf("  Bytes per timestamp: %.2f", float64(compressedSize)/float64(count))
}

func TestTimestampEncoder_RealWorldPattern(t *testing.T) {
	enc := NewTimestampEncoder()

	// Simulate real-world pattern with mostly regular intervals but occasional jitter
	baseTime := int64(1640000000000)
	baseInterval := int64(60000) // 60 seconds
	count := 1000

	timestamps := make([]int64, count)
	currentTime := baseTime

	rand.Seed(42)
	for i := 0; i < count; i++ {
		// Add small random jitter (-5 to +5 seconds)
		jitter := int64(rand.Intn(11)-5) * 1000
		currentTime += baseInterval + jitter
		timestamps[i] = currentTime
		enc.Encode(timestamps[i])
	}

	data := enc.Finish()

	// Decode and verify
	dec := NewTimestampDecoder(data)
	for i := 0; i < count; i++ {
		decoded, err := dec.Decode()
		if err != nil {
			t.Fatalf("Decode %d failed: %v", i, err)
		}
		if decoded != timestamps[i] {
			t.Errorf("Timestamp %d: expected %d, got %d", i, timestamps[i], decoded)
		}
	}

	// Report compression stats
	uncompressedSize := count * 8
	compressedSize := len(data)
	ratio := float64(uncompressedSize) / float64(compressedSize)

	t.Logf("Real-world pattern compression: %d -> %d bytes (%.2fx)", uncompressedSize, compressedSize, ratio)
}

func TestSignExtend(t *testing.T) {
	tests := []struct {
		value    uint64
		bits     uint8
		expected int64
	}{
		{0b0000000, 7, 0},
		{0b0000001, 7, 1},
		{0b1111111, 7, -1},
		{0b1111110, 7, -2},
		{0b1000000, 7, -64},
		{0b0111111, 7, 63},
		{0b111111111, 9, -1},
		{0b100000000, 9, -256},
		{0b011111111, 9, 255},
	}

	for _, tt := range tests {
		result := signExtend(tt.value, tt.bits)
		if result != tt.expected {
			t.Errorf("signExtend(0b%b, %d) = %d, expected %d", tt.value, tt.bits, result, tt.expected)
		}
	}
}

// Benchmark tests
func BenchmarkTimestampEncoder(b *testing.B) {
	enc := NewTimestampEncoder()
	baseTime := int64(time.Now().UnixMilli())
	interval := int64(60000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		enc.Encode(baseTime + int64(i)*interval)
		if i%1000 == 0 {
			enc.Finish()
			enc.Reset()
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "encodes/sec")
}

func BenchmarkTimestampDecoder(b *testing.B) {
	// Prepare encoded data
	enc := NewTimestampEncoder()
	baseTime := int64(time.Now().UnixMilli())
	interval := int64(60000)

	for i := 0; i < 1000; i++ {
		enc.Encode(baseTime + int64(i)*interval)
	}
	data := enc.Finish()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dec := NewTimestampDecoder(data)
		for j := 0; j < 1000; j++ {
			dec.Decode()
		}
	}

	b.ReportMetric(float64(b.N*1000)/b.Elapsed().Seconds(), "decodes/sec")
}

func BenchmarkTimestampCompression(b *testing.B) {
	baseTime := int64(time.Now().UnixMilli())
	interval := int64(60000)

	// Generate test data
	timestamps := make([]int64, 1000)
	for i := 0; i < 1000; i++ {
		timestamps[i] = baseTime + int64(i)*interval
	}

	b.Run("Encode", func(b *testing.B) {
		enc := NewTimestampEncoder()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			enc.Reset()
			for _, ts := range timestamps {
				enc.Encode(ts)
			}
			enc.Finish()
		}
	})

	// Prepare encoded data for decode benchmark
	enc := NewTimestampEncoder()
	for _, ts := range timestamps {
		enc.Encode(ts)
	}
	data := enc.Finish()

	b.Run("Decode", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			dec := NewTimestampDecoder(data)
			for j := 0; j < len(timestamps); j++ {
				dec.Decode()
			}
		}
	})

	// Report compression ratio
	uncompressedSize := len(timestamps) * 8
	compressedSize := len(data)
	ratio := float64(uncompressedSize) / float64(compressedSize)
	b.ReportMetric(ratio, "compression_ratio")
}
