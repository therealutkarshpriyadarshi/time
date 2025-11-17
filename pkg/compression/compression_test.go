package compression

import (
	"bytes"
	"math"
	"testing"
)

// TestBitWriterReader tests basic bit-level operations
func TestBitWriterReader(t *testing.T) {
	tests := []struct {
		name     string
		bits     []uint8
		expected []uint8
	}{
		{
			name:     "single bits",
			bits:     []uint8{1, 0, 1, 1, 0, 0, 1, 0},
			expected: []uint8{1, 0, 1, 1, 0, 0, 1, 0},
		},
		{
			name:     "all zeros",
			bits:     []uint8{0, 0, 0, 0, 0, 0, 0, 0},
			expected: []uint8{0, 0, 0, 0, 0, 0, 0, 0},
		},
		{
			name:     "all ones",
			bits:     []uint8{1, 1, 1, 1, 1, 1, 1, 1},
			expected: []uint8{1, 1, 1, 1, 1, 1, 1, 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			bw := NewBitWriter(buf)

			// Write bits
			for _, bit := range tt.bits {
				if err := bw.WriteBit(bit); err != nil {
					t.Fatalf("WriteBit failed: %v", err)
				}
			}

			if err := bw.Flush(); err != nil {
				t.Fatalf("Flush failed: %v", err)
			}

			// Read bits
			br := NewBitReader(buf.Bytes())
			for i, expected := range tt.expected {
				bit, err := br.ReadBit()
				if err != nil {
					t.Fatalf("ReadBit failed at position %d: %v", i, err)
				}
				if bit != expected {
					t.Errorf("bit %d: got %d, want %d", i, bit, expected)
				}
			}
		})
	}
}

// TestBitWriterReaderMultiBits tests multi-bit operations
func TestBitWriterReaderMultiBits(t *testing.T) {
	tests := []struct {
		name  string
		value uint64
		bits  uint8
	}{
		{"5 bits", 0b10110, 5},
		{"8 bits", 0b11001010, 8},
		{"16 bits", 0xABCD, 16},
		{"32 bits", 0x12345678, 32},
		{"64 bits", 0x123456789ABCDEF0, 64},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := &bytes.Buffer{}
			bw := NewBitWriter(buf)

			if err := bw.WriteBits(tt.value, tt.bits); err != nil {
				t.Fatalf("WriteBits failed: %v", err)
			}

			if err := bw.Flush(); err != nil {
				t.Fatalf("Flush failed: %v", err)
			}

			br := NewBitReader(buf.Bytes())
			value, err := br.ReadBits(tt.bits)
			if err != nil {
				t.Fatalf("ReadBits failed: %v", err)
			}

			// Mask to compare only the relevant bits
			mask := uint64((1 << tt.bits) - 1)
			if (value & mask) != (tt.value & mask) {
				t.Errorf("got %064b, want %064b", value&mask, tt.value&mask)
			}
		})
	}
}

// TestTimestampEncoder tests timestamp compression
func TestTimestampEncoder(t *testing.T) {
	tests := []struct {
		name       string
		timestamps []int64
	}{
		{
			name:       "regular intervals",
			timestamps: []int64{1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000},
		},
		{
			name:       "same timestamps",
			timestamps: []int64{1000, 1000, 1000, 1000, 1000},
		},
		{
			name:       "irregular intervals",
			timestamps: []int64{1000, 1050, 2100, 2200, 3500, 3600, 4000},
		},
		{
			name:       "single timestamp",
			timestamps: []int64{12345678},
		},
		{
			name:       "two timestamps",
			timestamps: []int64{1000, 2000},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoder := NewTimestampEncoder()
			for _, ts := range tt.timestamps {
				if err := encoder.Encode(ts); err != nil {
					t.Fatalf("Encode failed: %v", err)
				}
			}

			compressed, err := encoder.Finish()
			if err != nil {
				t.Fatalf("Finish failed: %v", err)
			}

			// Decode
			decoder := NewTimestampDecoder(compressed)
			decoded, err := decoder.DecodeAll(len(tt.timestamps))
			if err != nil {
				t.Fatalf("DecodeAll failed: %v", err)
			}

			// Compare
			if len(decoded) != len(tt.timestamps) {
				t.Fatalf("length mismatch: got %d, want %d", len(decoded), len(tt.timestamps))
			}

			for i := range tt.timestamps {
				if decoded[i] != tt.timestamps[i] {
					t.Errorf("timestamp %d: got %d, want %d", i, decoded[i], tt.timestamps[i])
				}
			}

			// Check compression ratio
			uncompressed := len(tt.timestamps) * 8 // 8 bytes per timestamp
			ratio := float64(uncompressed) / float64(len(compressed))
			t.Logf("Compression ratio: %.2fx (%d bytes -> %d bytes)", ratio, uncompressed, len(compressed))
		})
	}
}

// TestValueEncoder tests float64 XOR compression
func TestValueEncoder(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
	}{
		{
			name:   "constant values",
			values: []float64{1.5, 1.5, 1.5, 1.5, 1.5, 1.5},
		},
		{
			name:   "slowly changing values",
			values: []float64{1.0, 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9},
		},
		{
			name:   "random values",
			values: []float64{3.14, 2.71, 1.41, 9.81, 6.67, 0.577, 1.618},
		},
		{
			name:   "single value",
			values: []float64{42.0},
		},
		{
			name:   "two values",
			values: []float64{10.0, 20.0},
		},
		{
			name:   "zero values",
			values: []float64{0.0, 0.0, 0.0, 0.0},
		},
		{
			name:   "special values",
			values: []float64{math.NaN(), math.Inf(1), math.Inf(-1), 0.0, -0.0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoder := NewValueEncoder()
			for _, val := range tt.values {
				if err := encoder.Encode(val); err != nil {
					t.Fatalf("Encode failed: %v", err)
				}
			}

			compressed, err := encoder.Finish()
			if err != nil {
				t.Fatalf("Finish failed: %v", err)
			}

			// Decode
			decoder := NewValueDecoder(compressed)
			decoded, err := decoder.DecodeAll(len(tt.values))
			if err != nil {
				t.Fatalf("DecodeAll failed: %v", err)
			}

			// Compare
			if len(decoded) != len(tt.values) {
				t.Fatalf("length mismatch: got %d, want %d", len(decoded), len(tt.values))
			}

			for i := range tt.values {
				expected := tt.values[i]
				got := decoded[i]

				// Handle NaN specially
				if math.IsNaN(expected) && math.IsNaN(got) {
					continue
				}

				if got != expected {
					t.Errorf("value %d: got %v, want %v", i, got, expected)
				}
			}

			// Check compression ratio
			uncompressed := len(tt.values) * 8 // 8 bytes per float64
			ratio := float64(uncompressed) / float64(len(compressed))
			t.Logf("Compression ratio: %.2fx (%d bytes -> %d bytes)", ratio, uncompressed, len(compressed))
		})
	}
}

// TestTimestampCompressionRatio tests realistic timestamp sequences
func TestTimestampCompressionRatio(t *testing.T) {
	// Simulate 1 hour of data at 10-second intervals
	numSamples := 360
	timestamps := make([]int64, numSamples)
	baseTime := int64(1640000000000) // Start time

	for i := 0; i < numSamples; i++ {
		timestamps[i] = baseTime + int64(i*10000) // +10 seconds each
	}

	encoder := NewTimestampEncoder()
	for _, ts := range timestamps {
		if err := encoder.Encode(ts); err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
	}

	compressed, err := encoder.Finish()
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	uncompressed := numSamples * 8
	ratio := float64(uncompressed) / float64(len(compressed))

	t.Logf("Regular interval compression:")
	t.Logf("  Samples: %d", numSamples)
	t.Logf("  Uncompressed: %d bytes", uncompressed)
	t.Logf("  Compressed: %d bytes", len(compressed))
	t.Logf("  Ratio: %.2fx", ratio)
	t.Logf("  Bytes per sample: %.2f", float64(len(compressed))/float64(numSamples))

	// For regular intervals, we expect excellent compression (>10x)
	if ratio < 10.0 {
		t.Errorf("compression ratio too low: got %.2fx, want >10x", ratio)
	}
}

// TestValueCompressionRatio tests realistic value sequences
func TestValueCompressionRatio(t *testing.T) {
	// Simulate slowly changing metric (e.g., CPU usage)
	numSamples := 360
	values := make([]float64, numSamples)
	baseValue := 0.75

	for i := 0; i < numSamples; i++ {
		// Add very small variation that changes slowly (realistic for metrics)
		variation := float64(i/60) * 0.001 // Changes every minute
		values[i] = baseValue + variation
	}

	encoder := NewValueEncoder()
	for _, val := range values {
		if err := encoder.Encode(val); err != nil {
			t.Fatalf("Encode failed: %v", err)
		}
	}

	compressed, err := encoder.Finish()
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}

	uncompressed := numSamples * 8
	ratio := float64(uncompressed) / float64(len(compressed))

	t.Logf("Slowly changing value compression:")
	t.Logf("  Samples: %d", numSamples)
	t.Logf("  Uncompressed: %d bytes", uncompressed)
	t.Logf("  Compressed: %d bytes", len(compressed))
	t.Logf("  Ratio: %.2fx", ratio)
	t.Logf("  Bytes per sample: %.2f", float64(len(compressed))/float64(numSamples))

	// For slowly changing values, we expect good compression (>3x)
	if ratio < 3.0 {
		t.Errorf("compression ratio too low: got %.2fx, want >3x", ratio)
	}
}

// TestLeadingTrailingZeros tests the helper functions
func TestLeadingTrailingZeros(t *testing.T) {
	tests := []struct {
		value    uint64
		leading  int
		trailing int
	}{
		{0x0000000000000000, 64, 64},
		{0xFFFFFFFFFFFFFFFF, 0, 0},
		{0x0000000000000001, 63, 0},
		{0x8000000000000000, 0, 63},
		{0x0000000000FF0000, 40, 16},
		{0x00000000FFFFFFFF, 32, 0},
		{0xFFFFFFFF00000000, 0, 32},
	}

	for _, tt := range tests {
		leading := countLeadingZeros(tt.value)
		trailing := countTrailingZeros(tt.value)

		if leading != tt.leading {
			t.Errorf("countLeadingZeros(0x%016X): got %d, want %d", tt.value, leading, tt.leading)
		}

		if trailing != tt.trailing {
			t.Errorf("countTrailingZeros(0x%016X): got %d, want %d", tt.value, trailing, tt.trailing)
		}
	}
}
