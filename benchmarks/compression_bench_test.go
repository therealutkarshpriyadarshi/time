package benchmarks

import (
	"testing"

	"github.com/therealutkarshpriyadarshi/time/pkg/compression"
	"github.com/therealutkarshpriyadarshi/time/pkg/series"
	"github.com/therealutkarshpriyadarshi/time/pkg/storage"
)

// BenchmarkTimestampCompression benchmarks timestamp compression
func BenchmarkTimestampCompression(b *testing.B) {
	// Generate regular-interval timestamps
	timestamps := make([]int64, 1000)
	baseTime := int64(1640000000000)
	for i := 0; i < 1000; i++ {
		timestamps[i] = baseTime + int64(i*60000) // 1-minute intervals
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder := compression.NewTimestampEncoder()
		for _, ts := range timestamps {
			encoder.Encode(ts)
		}
		encoder.Finish()
	}
}

// BenchmarkTimestampDecompression benchmarks timestamp decompression
func BenchmarkTimestampDecompression(b *testing.B) {
	// Prepare compressed data
	timestamps := make([]int64, 1000)
	baseTime := int64(1640000000000)
	for i := 0; i < 1000; i++ {
		timestamps[i] = baseTime + int64(i*60000)
	}

	encoder := compression.NewTimestampEncoder()
	for _, ts := range timestamps {
		encoder.Encode(ts)
	}
	compressed, _ := encoder.Finish()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoder := compression.NewTimestampDecoder(compressed)
		decoder.DecodeAll(len(timestamps))
	}
}

// BenchmarkValueCompression benchmarks value compression
func BenchmarkValueCompression(b *testing.B) {
	// Generate slowly changing values
	values := make([]float64, 1000)
	baseValue := 0.75
	for i := 0; i < 1000; i++ {
		values[i] = baseValue + float64(i/100)*0.001
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		encoder := compression.NewValueEncoder()
		for _, val := range values {
			encoder.Encode(val)
		}
		encoder.Finish()
	}
}

// BenchmarkValueDecompression benchmarks value decompression
func BenchmarkValueDecompression(b *testing.B) {
	// Prepare compressed data
	values := make([]float64, 1000)
	baseValue := 0.75
	for i := 0; i < 1000; i++ {
		values[i] = baseValue + float64(i/100)*0.001
	}

	encoder := compression.NewValueEncoder()
	for _, val := range values {
		encoder.Encode(val)
	}
	compressed, _ := encoder.Finish()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decoder := compression.NewValueDecoder(compressed)
		decoder.DecodeAll(len(values))
	}
}

// BenchmarkChunkCreation benchmarks chunk creation with compression
func BenchmarkChunkCreation(b *testing.B) {
	// Generate sample data
	samples := make([]series.Sample, 120)
	baseTime := int64(1640000000000)
	baseValue := 0.75

	for i := 0; i < 120; i++ {
		samples[i] = series.Sample{
			Timestamp: baseTime + int64(i*60000),
			Value:     baseValue + float64(i/60)*0.001,
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		chunk := storage.NewChunk()
		chunk.Append(samples)
	}
}

// BenchmarkChunkIteration benchmarks chunk iteration and decompression
func BenchmarkChunkIteration(b *testing.B) {
	// Prepare chunk
	samples := make([]series.Sample, 120)
	baseTime := int64(1640000000000)
	baseValue := 0.75

	for i := 0; i < 120; i++ {
		samples[i] = series.Sample{
			Timestamp: baseTime + int64(i*60000),
			Value:     baseValue + float64(i/60)*0.001,
		}
	}

	chunk := storage.NewChunk()
	chunk.Append(samples)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter, _ := chunk.Iterator()
		for iter.Next() {
			iter.At()
		}
	}
}

// BenchmarkBlockWrite benchmarks writing blocks to disk
func BenchmarkBlockWrite(b *testing.B) {
	tmpDir := b.TempDir()

	// Create MemTable with data
	mt := storage.NewMemTable()
	s := series.NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	})

	samples := make([]series.Sample, 1000)
	for i := 0; i < 1000; i++ {
		samples[i] = series.Sample{
			Timestamp: int64(i * 1000),
			Value:     0.5 + float64(i%100)*0.001,
		}
	}
	mt.Insert(s, samples)

	writer := storage.NewBlockWriter(tmpDir)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		writer.WriteMemTable(mt)
	}
}

// BenchmarkBlockRead benchmarks reading blocks from disk
func BenchmarkBlockRead(b *testing.B) {
	tmpDir := b.TempDir()

	// Create and persist a block
	mt := storage.NewMemTable()
	s := series.NewSeries(map[string]string{
		"__name__": "cpu_usage",
		"host":     "server1",
	})

	samples := make([]series.Sample, 1000)
	for i := 0; i < 1000; i++ {
		samples[i] = series.Sample{
			Timestamp: int64(i * 1000),
			Value:     0.5 + float64(i%100)*0.001,
		}
	}
	mt.Insert(s, samples)

	writer := storage.NewBlockWriter(tmpDir)
	block, _ := writer.WriteMemTable(mt)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		block.GetSeries(s.Hash, 0, 1000000)
	}
}
