package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/therealutkarshpriyadarshi/time/pkg/compression"
	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

// Chunk represents a compressed time-series chunk containing multiple samples.
// It uses delta-of-delta encoding for timestamps and XOR compression for values
// as described in Facebook's Gorilla paper.
//
// Format:
//   Header (24 bytes):
//     [8 bytes: minTime]
//     [8 bytes: maxTime]
//     [2 bytes: numSamples]
//     [4 bytes: dataLength]
//     [2 bytes: encoding flags]
//
//   Data:
//     [N bytes: compressed timestamps]
//     [M bytes: compressed values]
//
//   Footer:
//     [4 bytes: CRC32 checksum]
type Chunk struct {
	MinTime    int64    // Minimum timestamp in chunk
	MaxTime    int64    // Maximum timestamp in chunk
	NumSamples uint16   // Number of samples in chunk
	Encoding   uint16   // Encoding flags (reserved for future use)
	Data       []byte   // Compressed data (timestamps + values)
	Checksum   uint32   // CRC32 checksum of data
}

const (
	// ChunkHeaderSize is the size of the chunk header in bytes
	ChunkHeaderSize = 24

	// ChunkFooterSize is the size of the chunk footer in bytes
	ChunkFooterSize = 4

	// DefaultMaxSamplesPerChunk is the default maximum number of samples per chunk
	// 120 samples = 2 hours @ 1-minute intervals
	DefaultMaxSamplesPerChunk = 120

	// EncodingGorilla indicates Gorilla compression (delta-of-delta + XOR)
	EncodingGorilla uint16 = 1
)

// NewChunk creates a new empty chunk
func NewChunk() *Chunk {
	return &Chunk{
		Encoding: EncodingGorilla,
	}
}

// Append compresses and appends samples to the chunk.
// This creates a new chunk with the provided samples.
func (c *Chunk) Append(samples []series.Sample) error {
	if len(samples) == 0 {
		return fmt.Errorf("cannot append zero samples")
	}

	if len(samples) > int(^uint16(0)) {
		return fmt.Errorf("too many samples: %d (max %d)", len(samples), ^uint16(0))
	}

	// Update metadata
	c.MinTime = samples[0].Timestamp
	c.MaxTime = samples[len(samples)-1].Timestamp
	c.NumSamples = uint16(len(samples))

	// Compress timestamps
	tsEncoder := compression.NewTimestampEncoder()
	for _, sample := range samples {
		if err := tsEncoder.Encode(sample.Timestamp); err != nil {
			return fmt.Errorf("failed to encode timestamp: %w", err)
		}
	}

	compressedTS, err := tsEncoder.Finish()
	if err != nil {
		return fmt.Errorf("failed to finish timestamp encoding: %w", err)
	}

	// Compress values
	valEncoder := compression.NewValueEncoder()
	for _, sample := range samples {
		if err := valEncoder.Encode(sample.Value); err != nil {
			return fmt.Errorf("failed to encode value: %w", err)
		}
	}

	compressedVals, err := valEncoder.Finish()
	if err != nil {
		return fmt.Errorf("failed to finish value encoding: %w", err)
	}

	// Combine compressed data: [4 bytes: ts length][timestamps][values]
	tsLen := uint32(len(compressedTS))
	c.Data = make([]byte, 4+len(compressedTS)+len(compressedVals))

	binary.BigEndian.PutUint32(c.Data[0:4], tsLen)
	copy(c.Data[4:4+tsLen], compressedTS)
	copy(c.Data[4+tsLen:], compressedVals)

	// Calculate checksum
	c.Checksum = crc32.ChecksumIEEE(c.Data)

	return nil
}

// Iterator returns an iterator over the samples in the chunk
func (c *Chunk) Iterator() (*ChunkIterator, error) {
	if len(c.Data) < 4 {
		return nil, fmt.Errorf("invalid chunk data: too short")
	}

	// Extract timestamp and value data
	tsLen := binary.BigEndian.Uint32(c.Data[0:4])
	if len(c.Data) < int(4+tsLen) {
		return nil, fmt.Errorf("invalid chunk data: timestamp length mismatch")
	}

	compressedTS := c.Data[4 : 4+tsLen]
	compressedVals := c.Data[4+tsLen:]

	// Verify checksum
	checksum := crc32.ChecksumIEEE(c.Data)
	if checksum != c.Checksum {
		return nil, fmt.Errorf("chunk checksum mismatch: got %d, want %d", checksum, c.Checksum)
	}

	// Create decoders
	tsDecoder := compression.NewTimestampDecoder(compressedTS)
	valDecoder := compression.NewValueDecoder(compressedVals)

	return &ChunkIterator{
		tsDecoder:  tsDecoder,
		valDecoder: valDecoder,
		numSamples: int(c.NumSamples),
		index:      0,
	}, nil
}

// MarshalBinary serializes the chunk to bytes
func (c *Chunk) MarshalBinary() ([]byte, error) {
	totalSize := ChunkHeaderSize + len(c.Data) + ChunkFooterSize
	buf := make([]byte, totalSize)

	// Write header
	binary.BigEndian.PutUint64(buf[0:8], uint64(c.MinTime))
	binary.BigEndian.PutUint64(buf[8:16], uint64(c.MaxTime))
	binary.BigEndian.PutUint16(buf[16:18], c.NumSamples)
	binary.BigEndian.PutUint32(buf[18:22], uint32(len(c.Data)))
	binary.BigEndian.PutUint16(buf[22:24], c.Encoding)

	// Write data
	copy(buf[ChunkHeaderSize:ChunkHeaderSize+len(c.Data)], c.Data)

	// Write footer (checksum)
	binary.BigEndian.PutUint32(buf[ChunkHeaderSize+len(c.Data):], c.Checksum)

	return buf, nil
}

// UnmarshalBinary deserializes the chunk from bytes
func (c *Chunk) UnmarshalBinary(data []byte) error {
	if len(data) < ChunkHeaderSize+ChunkFooterSize {
		return fmt.Errorf("chunk data too short: %d bytes", len(data))
	}

	// Read header
	c.MinTime = int64(binary.BigEndian.Uint64(data[0:8]))
	c.MaxTime = int64(binary.BigEndian.Uint64(data[8:16]))
	c.NumSamples = binary.BigEndian.Uint16(data[16:18])
	dataLength := binary.BigEndian.Uint32(data[18:22])
	c.Encoding = binary.BigEndian.Uint16(data[22:24])

	// Validate data length
	expectedSize := ChunkHeaderSize + int(dataLength) + ChunkFooterSize
	if len(data) != expectedSize {
		return fmt.Errorf("chunk size mismatch: got %d, expected %d", len(data), expectedSize)
	}

	// Read data
	c.Data = make([]byte, dataLength)
	copy(c.Data, data[ChunkHeaderSize:ChunkHeaderSize+dataLength])

	// Read footer (checksum)
	c.Checksum = binary.BigEndian.Uint32(data[ChunkHeaderSize+dataLength:])

	// Verify checksum
	checksum := crc32.ChecksumIEEE(c.Data)
	if checksum != c.Checksum {
		return fmt.Errorf("chunk checksum verification failed: got %d, want %d", checksum, c.Checksum)
	}

	return nil
}

// Size returns the total size of the chunk in bytes
func (c *Chunk) Size() int {
	return ChunkHeaderSize + len(c.Data) + ChunkFooterSize
}

// CompressionRatio returns the compression ratio (uncompressed / compressed)
func (c *Chunk) CompressionRatio() float64 {
	uncompressed := int(c.NumSamples) * (8 + 8) // timestamp + value
	compressed := len(c.Data)
	if compressed == 0 {
		return 0
	}
	return float64(uncompressed) / float64(compressed)
}

// WriteTo writes the chunk to a writer
func (c *Chunk) WriteTo(w io.Writer) (int64, error) {
	data, err := c.MarshalBinary()
	if err != nil {
		return 0, err
	}

	n, err := w.Write(data)
	return int64(n), err
}

// ReadFrom reads a chunk from a reader
func (c *Chunk) ReadFrom(r io.Reader) (int64, error) {
	// Read header first to get data length
	header := make([]byte, ChunkHeaderSize)
	n, err := io.ReadFull(r, header)
	if err != nil {
		return int64(n), err
	}

	dataLength := binary.BigEndian.Uint32(header[18:22])

	// Read data and footer
	remaining := make([]byte, dataLength+ChunkFooterSize)
	n2, err := io.ReadFull(r, remaining)
	if err != nil {
		return int64(n + n2), err
	}

	// Combine and unmarshal
	fullData := append(header, remaining...)
	if err := c.UnmarshalBinary(fullData); err != nil {
		return int64(n + n2), err
	}

	return int64(n + n2), nil
}

// ChunkIterator iterates over samples in a chunk
type ChunkIterator struct {
	tsDecoder  *compression.TimestampDecoder
	valDecoder *compression.ValueDecoder
	numSamples int
	index      int
	err        error
}

// Next advances the iterator to the next sample
func (it *ChunkIterator) Next() bool {
	if it.err != nil || it.index >= it.numSamples {
		return false
	}
	it.index++
	return true
}

// At returns the current sample
func (it *ChunkIterator) At() (series.Sample, error) {
	if it.index == 0 || it.index > it.numSamples {
		return series.Sample{}, fmt.Errorf("iterator not positioned on a valid sample")
	}

	// Decode timestamp
	ts, err := it.tsDecoder.Decode()
	if err != nil {
		it.err = err
		return series.Sample{}, fmt.Errorf("failed to decode timestamp: %w", err)
	}

	// Decode value
	val, err := it.valDecoder.Decode()
	if err != nil {
		it.err = err
		return series.Sample{}, fmt.Errorf("failed to decode value: %w", err)
	}

	return series.Sample{
		Timestamp: ts,
		Value:     val,
	}, nil
}

// Err returns any error that occurred during iteration
func (it *ChunkIterator) Err() error {
	return it.err
}

// ChunkBuilder helps build chunks incrementally
type ChunkBuilder struct {
	samples    []series.Sample
	maxSamples int
}

// NewChunkBuilder creates a new chunk builder
func NewChunkBuilder(maxSamples int) *ChunkBuilder {
	if maxSamples <= 0 {
		maxSamples = DefaultMaxSamplesPerChunk
	}

	return &ChunkBuilder{
		samples:    make([]series.Sample, 0, maxSamples),
		maxSamples: maxSamples,
	}
}

// Add adds a sample to the builder
func (cb *ChunkBuilder) Add(sample series.Sample) bool {
	if len(cb.samples) >= cb.maxSamples {
		return false // Chunk is full
	}

	cb.samples = append(cb.samples, sample)
	return true
}

// IsFull returns true if the chunk is full
func (cb *ChunkBuilder) IsFull() bool {
	return len(cb.samples) >= cb.maxSamples
}

// Build creates a chunk from the accumulated samples
func (cb *ChunkBuilder) Build() (*Chunk, error) {
	if len(cb.samples) == 0 {
		return nil, fmt.Errorf("cannot build chunk with zero samples")
	}

	chunk := NewChunk()
	if err := chunk.Append(cb.samples); err != nil {
		return nil, err
	}

	return chunk, nil
}

// Reset clears the builder for reuse
func (cb *ChunkBuilder) Reset() {
	cb.samples = cb.samples[:0]
}

// Count returns the number of samples in the builder
func (cb *ChunkBuilder) Count() int {
	return len(cb.samples)
}
