package compression

import (
	"math"
	"math/rand"
	"testing"
)

func TestValueEncoder_SingleValue(t *testing.T) {
	enc := NewValueEncoder()
	value := 42.5

	err := enc.Encode(value)
	if err != nil {
		t.Fatalf("Encode failed: %v", err)
	}

	data := enc.Finish()
	if len(data) == 0 {
		t.Fatal("Expected non-empty data")
	}

	// Decode
	dec := NewValueDecoder(data)
	decoded, err := dec.Decode()
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	if decoded != value {
		t.Errorf("Expected %f, got %f", value, decoded)
	}
}

func TestValueEncoder_TwoValues(t *testing.T) {
	enc := NewValueEncoder()
	val1 := 42.5
	val2 := 43.7

	enc.Encode(val1)
	enc.Encode(val2)

	data := enc.Finish()

	// Decode
	dec := NewValueDecoder(data)

	decoded1, _ := dec.Decode()
	if decoded1 != val1 {
		t.Errorf("Expected %f, got %f", val1, decoded1)
	}

	decoded2, _ := dec.Decode()
	if decoded2 != val2 {
		t.Errorf("Expected %f, got %f", val2, decoded2)
	}
}

func TestValueEncoder_IdenticalValues(t *testing.T) {
	enc := NewValueEncoder()
	value := 100.0

	// Encode same value multiple times
	for i := 0; i < 100; i++ {
		enc.Encode(value)
	}

	data := enc.Finish()

	// Decode and verify
	dec := NewValueDecoder(data)
	for i := 0; i < 100; i++ {
		decoded, err := dec.Decode()
		if err != nil {
			t.Fatalf("Decode %d failed: %v", i, err)
		}
		if decoded != value {
			t.Errorf("Value %d: expected %f, got %f", i, value, decoded)
		}
	}

	// Check compression - identical values should compress extremely well
	uncompressedSize := 100 * 8
	compressedSize := len(data)
	ratio := float64(uncompressedSize) / float64(compressedSize)

	t.Logf("Identical values compression: %d -> %d bytes (%.2fx)", uncompressedSize, compressedSize, ratio)

	// Should compress to nearly nothing (just the first value + 100 zero bits)
	if ratio < 20.0 {
		t.Logf("Warning: compression ratio %.2fx is lower than expected for identical values", ratio)
	}
}

func TestValueEncoder_SlowlyChanging(t *testing.T) {
	enc := NewValueEncoder()

	// Values that change slowly (good for XOR compression)
	baseValue := 100.0
	increment := 0.1
	count := 100

	values := make([]float64, count)
	for i := 0; i < count; i++ {
		values[i] = baseValue + float64(i)*increment
		enc.Encode(values[i])
	}

	data := enc.Finish()

	// Decode and verify
	dec := NewValueDecoder(data)
	for i := 0; i < count; i++ {
		decoded, err := dec.Decode()
		if err != nil {
			t.Fatalf("Decode %d failed: %v", i, err)
		}
		if decoded != values[i] {
			t.Errorf("Value %d: expected %f, got %f", i, values[i], decoded)
		}
	}

	// Report compression stats
	uncompressedSize := count * 8
	compressedSize := len(data)
	ratio := float64(uncompressedSize) / float64(compressedSize)

	t.Logf("Slowly changing values compression: %d -> %d bytes (%.2fx)", uncompressedSize, compressedSize, ratio)
}

func TestValueEncoder_SpecialFloatValues(t *testing.T) {
	tests := []struct {
		name   string
		values []float64
	}{
		{
			name:   "Zeros",
			values: []float64{0.0, 0.0, 0.0},
		},
		{
			name:   "Negative zeros",
			values: []float64{0.0, -0.0, 0.0},
		},
		{
			name:   "Infinities",
			values: []float64{math.Inf(1), math.Inf(-1), math.Inf(1)},
		},
		{
			name:   "NaN",
			values: []float64{math.NaN(), math.NaN(), math.NaN()},
		},
		{
			name:   "Very small numbers",
			values: []float64{1e-100, 2e-100, 3e-100},
		},
		{
			name:   "Very large numbers",
			values: []float64{1e100, 2e100, 3e100},
		},
		{
			name:   "Mixed signs",
			values: []float64{100.0, -100.0, 100.0, -100.0},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			enc := NewValueEncoder()

			for _, v := range tt.values {
				enc.Encode(v)
			}

			data := enc.Finish()

			// Decode and verify
			dec := NewValueDecoder(data)
			for i, expected := range tt.values {
				decoded, err := dec.Decode()
				if err != nil {
					t.Fatalf("Decode %d failed: %v", i, err)
				}

				// Special handling for NaN
				if math.IsNaN(expected) {
					if !math.IsNaN(decoded) {
						t.Errorf("Value %d: expected NaN, got %f", i, decoded)
					}
				} else if decoded != expected {
					t.Errorf("Value %d: expected %f, got %f", i, expected, decoded)
				}
			}
		})
	}
}

func TestValueEncoder_Reset(t *testing.T) {
	enc := NewValueEncoder()

	// First encoding
	enc.Encode(100.0)
	enc.Encode(200.0)
	data1 := enc.Finish()

	// Reset and encode again
	enc.Reset()
	enc.Encode(500.0)
	enc.Encode(600.0)
	data2 := enc.Finish()

	// Verify both are decodable
	dec := NewValueDecoder(data1)
	val, _ := dec.Decode()
	if val != 100.0 {
		t.Errorf("First encoding: expected 100.0, got %f", val)
	}

	dec.Reset(data2)
	val, _ = dec.Decode()
	if val != 500.0 {
		t.Errorf("Second encoding: expected 500.0, got %f", val)
	}
}

func TestValueEncoder_RandomValues(t *testing.T) {
	enc := NewValueEncoder()

	rand.Seed(42)
	count := 1000
	values := make([]float64, count)

	for i := 0; i < count; i++ {
		// Random values in a reasonable range
		values[i] = rand.Float64() * 1000
		enc.Encode(values[i])
	}

	data := enc.Finish()

	// Decode and verify
	dec := NewValueDecoder(data)
	for i := 0; i < count; i++ {
		decoded, err := dec.Decode()
		if err != nil {
			t.Fatalf("Decode %d failed: %v", i, err)
		}
		if decoded != values[i] {
			t.Errorf("Value %d: expected %f, got %f", i, values[i], decoded)
		}
	}

	// Report compression stats
	uncompressedSize := count * 8
	compressedSize := len(data)
	ratio := float64(uncompressedSize) / float64(compressedSize)

	t.Logf("Random values compression: %d -> %d bytes (%.2fx)", uncompressedSize, compressedSize, ratio)
}

func TestValueEncoder_SinWave(t *testing.T) {
	enc := NewValueEncoder()

	// Simulate a sin wave (periodic pattern, good for compression)
	count := 1000
	values := make([]float64, count)

	for i := 0; i < count; i++ {
		values[i] = math.Sin(float64(i) * 0.1)
		enc.Encode(values[i])
	}

	data := enc.Finish()

	// Decode and verify
	dec := NewValueDecoder(data)
	for i := 0; i < count; i++ {
		decoded, err := dec.Decode()
		if err != nil {
			t.Fatalf("Decode %d failed: %v", i, err)
		}
		if decoded != values[i] {
			t.Errorf("Value %d: expected %f, got %f", i, values[i], decoded)
		}
	}

	// Report compression stats
	uncompressedSize := count * 8
	compressedSize := len(data)
	ratio := float64(uncompressedSize) / float64(compressedSize)

	t.Logf("Sin wave compression: %d -> %d bytes (%.2fx)", uncompressedSize, compressedSize, ratio)
}

func TestValueEncoder_CPUUsagePattern(t *testing.T) {
	enc := NewValueEncoder()

	// Simulate CPU usage (0-100%, tends to stay in similar ranges)
	rand.Seed(42)
	count := 1000
	values := make([]float64, count)

	currentCPU := 50.0
	for i := 0; i < count; i++ {
		// Small random walk
		change := (rand.Float64() - 0.5) * 10
		currentCPU += change
		if currentCPU < 0 {
			currentCPU = 0
		}
		if currentCPU > 100 {
			currentCPU = 100
		}
		values[i] = currentCPU
		enc.Encode(values[i])
	}

	data := enc.Finish()

	// Decode and verify
	dec := NewValueDecoder(data)
	for i := 0; i < count; i++ {
		decoded, err := dec.Decode()
		if err != nil {
			t.Fatalf("Decode %d failed: %v", i, err)
		}
		if decoded != values[i] {
			t.Errorf("Value %d: expected %f, got %f", i, values[i], decoded)
		}
	}

	// Report compression stats
	uncompressedSize := count * 8
	compressedSize := len(data)
	ratio := float64(uncompressedSize) / float64(compressedSize)

	t.Logf("CPU usage pattern compression: %d -> %d bytes (%.2fx)", uncompressedSize, compressedSize, ratio)
}

func TestValueEncoder_LargeDataset(t *testing.T) {
	enc := NewValueEncoder()

	// Simulate 24 hours of metric data at 15-second intervals
	count := 24 * 60 * 60 / 15 // 5,760 samples
	values := make([]float64, count)

	// Slowly varying metric around 100
	rand.Seed(42)
	current := 100.0
	for i := 0; i < count; i++ {
		current += (rand.Float64() - 0.5) * 2
		values[i] = current
		enc.Encode(values[i])
	}

	data := enc.Finish()

	// Verify decoding
	dec := NewValueDecoder(data)
	for i := 0; i < count; i++ {
		decoded, err := dec.Decode()
		if err != nil {
			t.Fatalf("Decode %d failed: %v", i, err)
		}
		if decoded != values[i] {
			t.Errorf("Value %d: expected %f, got %f", i, values[i], decoded)
		}
	}

	// Report compression stats
	uncompressedSize := count * 8
	compressedSize := len(data)
	ratio := float64(uncompressedSize) / float64(compressedSize)

	t.Logf("Large dataset compression:")
	t.Logf("  Count: %d values", count)
	t.Logf("  Uncompressed: %d bytes", uncompressedSize)
	t.Logf("  Compressed: %d bytes", compressedSize)
	t.Logf("  Ratio: %.2fx", ratio)
	t.Logf("  Bytes per value: %.2f", float64(compressedSize)/float64(count))
}

// Benchmark tests
func BenchmarkValueEncoder(b *testing.B) {
	enc := NewValueEncoder()
	value := 100.0

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		value += 0.1
		enc.Encode(value)
		if i%1000 == 0 {
			enc.Finish()
			enc.Reset()
		}
	}

	b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "encodes/sec")
}

func BenchmarkValueDecoder(b *testing.B) {
	// Prepare encoded data
	enc := NewValueEncoder()
	value := 100.0

	for i := 0; i < 1000; i++ {
		value += 0.1
		enc.Encode(value)
	}
	data := enc.Finish()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dec := NewValueDecoder(data)
		for j := 0; j < 1000; j++ {
			dec.Decode()
		}
	}

	b.ReportMetric(float64(b.N*1000)/b.Elapsed().Seconds(), "decodes/sec")
}

func BenchmarkValueCompression(b *testing.B) {
	// Generate test data
	values := make([]float64, 1000)
	current := 100.0
	for i := 0; i < 1000; i++ {
		current += 0.1
		values[i] = current
	}

	b.Run("Encode", func(b *testing.B) {
		enc := NewValueEncoder()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			enc.Reset()
			for _, v := range values {
				enc.Encode(v)
			}
			enc.Finish()
		}
	})

	// Prepare encoded data for decode benchmark
	enc := NewValueEncoder()
	for _, v := range values {
		enc.Encode(v)
	}
	data := enc.Finish()

	b.Run("Decode", func(b *testing.B) {
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			dec := NewValueDecoder(data)
			for j := 0; j < len(values); j++ {
				dec.Decode()
			}
		}
	})

	// Report compression ratio
	uncompressedSize := len(values) * 8
	compressedSize := len(data)
	ratio := float64(uncompressedSize) / float64(compressedSize)
	b.ReportMetric(ratio, "compression_ratio")
}

func BenchmarkValueCompressionPatterns(b *testing.B) {
	patterns := map[string][]float64{
		"Constant": makeConstantValues(1000, 100.0),
		"Linear":   makeLinearValues(1000, 100.0, 0.1),
		"Random":   makeRandomValues(1000, 100.0, 10.0),
		"SinWave":  makeSinWaveValues(1000),
	}

	for name, values := range patterns {
		b.Run(name, func(b *testing.B) {
			enc := NewValueEncoder()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				enc.Reset()
				for _, v := range values {
					enc.Encode(v)
				}
				enc.Finish()
			}
		})
	}
}

// Helper functions for benchmarks
func makeConstantValues(count int, value float64) []float64 {
	values := make([]float64, count)
	for i := range values {
		values[i] = value
	}
	return values
}

func makeLinearValues(count int, start, increment float64) []float64 {
	values := make([]float64, count)
	for i := range values {
		values[i] = start + float64(i)*increment
	}
	return values
}

func makeRandomValues(count int, mean, stddev float64) []float64 {
	rand.Seed(42)
	values := make([]float64, count)
	for i := range values {
		values[i] = mean + (rand.Float64()-0.5)*2*stddev
	}
	return values
}

func makeSinWaveValues(count int) []float64 {
	values := make([]float64, count)
	for i := range values {
		values[i] = math.Sin(float64(i) * 0.1)
	}
	return values
}
