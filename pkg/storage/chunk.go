package storage

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"

	"github.com/therealutkarshpriyadarshi/time/pkg/compression"
	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

const (
	// ChunkEncodingGorilla uses Gorilla compression (delta-of-delta + XOR)
	ChunkEncodingGorilla uint16 = 1

	// DefaultChunkSize is the default maximum number of samples per chunk (120 samples ≈ 2 hours @ 1m interval)
	DefaultChunkSize = 120

	// ChunkHeaderSize is the size of the chunk header in bytes
	ChunkHeaderSize = 24 // 8 + 8 + 2 + 4 + 2 = 24 bytes

	// ChunkFooterSize is the size of the chunk footer (CRC32 checksum)
	ChunkFooterSize = 4
)

// Chunk represents a compressed time-series chunk.
// It stores samples using Gorilla compression for efficient storage.
//
// On-disk format:
//
//	Header (24 bytes):
//	  [8 bytes: minTime]       - Minimum timestamp in the chunk
//	  [8 bytes: maxTime]       - Maximum timestamp in the chunk
//	  [2 bytes: numSamples]    - Number of samples in the chunk
//	  [4 bytes: dataLength]    - Length of compressed data in bytes
//	  [2 bytes: encoding]      - Encoding type (1 = Gorilla)
//	Data (variable):
//	  [N bytes: compressed timestamps]
//	  [M bytes: compressed values]
//	Footer (4 bytes):
//	  [4 bytes: CRC32 checksum] - Checksum of header + data
type Chunk struct {
	MinTime    int64          // Minimum timestamp
	MaxTime    int64          // Maximum timestamp
	NumSamples uint16         // Number of samples
	Encoding   uint16         // Encoding type
	Data       []byte         // Compressed data
	samples    []series.Sample // In-memory samples (before compression)
	sealed     bool           // Whether chunk is sealed (immutable)
}

// NewChunk creates a new empty chunk.
func NewChunk() *Chunk {
	return &Chunk{
		samples:  make([]series.Sample, 0, DefaultChunkSize),
		Encoding: ChunkEncodingGorilla,
	}
}

// Append adds a sample to the chunk.
// Returns an error if the chunk is full or sealed.
func (c *Chunk) Append(sample series.Sample) error {
	if c.sealed {
		return fmt.Errorf("cannot append to sealed chunk")
	}

	if len(c.samples) >= DefaultChunkSize {
		return fmt.Errorf("chunk is full (max %d samples)", DefaultChunkSize)
	}

	// Validate timestamp ordering
	if len(c.samples) > 0 && sample.Timestamp <= c.samples[len(c.samples)-1].Timestamp {
		return fmt.Errorf("sample timestamp must be greater than previous timestamp")
	}

	// Update min/max times
	if len(c.samples) == 0 {
		c.MinTime = sample.Timestamp
	}
	c.MaxTime = sample.Timestamp

	c.samples = append(c.samples, sample)
	c.NumSamples = uint16(len(c.samples))

	return nil
}

// Seal finalizes the chunk and compresses the samples.
// After sealing, no more samples can be added.
func (c *Chunk) Seal() error {
	if c.sealed {
		return fmt.Errorf("chunk already sealed")
	}

	if len(c.samples) == 0 {
		return fmt.Errorf("cannot seal empty chunk")
	}

	// Compress timestamps
	tsEnc := compression.NewTimestampEncoder()
	for _, sample := range c.samples {
		if err := tsEnc.Encode(sample.Timestamp); err != nil {
			return fmt.Errorf("timestamp encoding failed: %w", err)
		}
	}
	tsData := tsEnc.Finish()

	// Compress values
	valEnc := compression.NewValueEncoder()
	for _, sample := range c.samples {
		if err := valEnc.Encode(sample.Value); err != nil {
			return fmt.Errorf("value encoding failed: %w", err)
		}
	}
	valData := valEnc.Finish()

	// Combine compressed data: [timestamp data length][timestamp data][value data]
	c.Data = make([]byte, 4+len(tsData)+len(valData))
	binary.BigEndian.PutUint32(c.Data[0:4], uint32(len(tsData)))
	copy(c.Data[4:], tsData)
	copy(c.Data[4+len(tsData):], valData)

	c.sealed = true

	// Clear in-memory samples to save memory
	c.samples = nil

	return nil
}

// Iterator returns an iterator over the chunk's samples.
func (c *Chunk) Iterator() (*ChunkIterator, error) {
	if !c.sealed {
		return nil, fmt.Errorf("cannot iterate unsealed chunk")
	}

	if len(c.Data) < 4 {
		return nil, fmt.Errorf("invalid chunk data: too short")
	}

	// Extract timestamp and value data
	tsLen := binary.BigEndian.Uint32(c.Data[0:4])
	if int(4+tsLen) > len(c.Data) {
		return nil, fmt.Errorf("invalid chunk data: timestamp length exceeds data size")
	}

	tsData := c.Data[4 : 4+tsLen]
	valData := c.Data[4+tsLen:]

	return &ChunkIterator{
		tsDec:  compression.NewTimestampDecoder(tsData),
		valDec: compression.NewValueDecoder(valData),
		count:  int(c.NumSamples),
		pos:    0,
	}, nil
}

// MarshalBinary encodes the chunk to binary format for storage.
// Format: [Header][Data][Checksum]
func (c *Chunk) MarshalBinary() ([]byte, error) {
	if !c.sealed {
		return nil, fmt.Errorf("cannot marshal unsealed chunk")
	}

	totalSize := ChunkHeaderSize + len(c.Data) + ChunkFooterSize
	buf := make([]byte, totalSize)

	// Write header
	binary.BigEndian.PutUint64(buf[0:8], uint64(c.MinTime))
	binary.BigEndian.PutUint64(buf[8:16], uint64(c.MaxTime))
	binary.BigEndian.PutUint16(buf[16:18], c.NumSamples)
	binary.BigEndian.PutUint32(buf[18:22], uint32(len(c.Data)))
	binary.BigEndian.PutUint16(buf[22:24], c.Encoding)

	// Write data
	copy(buf[ChunkHeaderSize:], c.Data)

	// Compute and write checksum (header + data)
	checksum := crc32.ChecksumIEEE(buf[:ChunkHeaderSize+len(c.Data)])
	binary.BigEndian.PutUint32(buf[ChunkHeaderSize+len(c.Data):], checksum)

	return buf, nil
}

// UnmarshalBinary decodes the chunk from binary format.
func (c *Chunk) UnmarshalBinary(data []byte) error {
	if len(data) < ChunkHeaderSize+ChunkFooterSize {
		return fmt.Errorf("invalid chunk data: too short")
	}

	// Read header
	c.MinTime = int64(binary.BigEndian.Uint64(data[0:8]))
	c.MaxTime = int64(binary.BigEndian.Uint64(data[8:16]))
	c.NumSamples = binary.BigEndian.Uint16(data[16:18])
	dataLen := binary.BigEndian.Uint32(data[18:22])
	c.Encoding = binary.BigEndian.Uint16(data[22:24])

	// Validate encoding
	if c.Encoding != ChunkEncodingGorilla {
		return fmt.Errorf("unsupported encoding: %d", c.Encoding)
	}

	// Validate data length
	expectedSize := ChunkHeaderSize + int(dataLen) + ChunkFooterSize
	if len(data) != expectedSize {
		return fmt.Errorf("invalid chunk size: expected %d, got %d", expectedSize, len(data))
	}

	// Read data
	c.Data = make([]byte, dataLen)
	copy(c.Data, data[ChunkHeaderSize:ChunkHeaderSize+dataLen])

	// Verify checksum
	expectedChecksum := binary.BigEndian.Uint32(data[ChunkHeaderSize+dataLen:])
	actualChecksum := crc32.ChecksumIEEE(data[:ChunkHeaderSize+dataLen])
	if expectedChecksum != actualChecksum {
		return fmt.Errorf("checksum mismatch: expected 0x%08X, got 0x%08X", expectedChecksum, actualChecksum)
	}

	c.sealed = true
	return nil
}

// Size returns the serialized size of the chunk in bytes.
func (c *Chunk) Size() int {
	if !c.sealed {
		return 0
	}
	return ChunkHeaderSize + len(c.Data) + ChunkFooterSize
}

// IsFull returns true if the chunk has reached its maximum capacity.
func (c *Chunk) IsFull() bool {
	return len(c.samples) >= DefaultChunkSize
}

// IsSealed returns true if the chunk has been sealed.
func (c *Chunk) IsSealed() bool {
	return c.sealed
}

// Contains returns true if the given timestamp falls within the chunk's time range.
func (c *Chunk) Contains(timestamp int64) bool {
	return timestamp >= c.MinTime && timestamp <= c.MaxTime
}

// ChunkIterator iterates over samples in a chunk.
type ChunkIterator struct {
	tsDec  *compression.TimestampDecoder
	valDec *compression.ValueDecoder
	count  int
	pos    int
	err    error
}

// Next advances the iterator and returns true if there is a next sample.
func (it *ChunkIterator) Next() bool {
	return it.pos < it.count && it.err == nil
}

// At returns the current sample.
func (it *ChunkIterator) At() (series.Sample, error) {
	if it.err != nil {
		return series.Sample{}, it.err
	}

	if it.pos >= it.count {
		return series.Sample{}, io.EOF
	}

	// Decode timestamp
	timestamp, err := it.tsDec.Decode()
	if err != nil {
		it.err = fmt.Errorf("timestamp decode failed: %w", err)
		return series.Sample{}, it.err
	}

	// Decode value
	value, err := it.valDec.Decode()
	if err != nil {
		it.err = fmt.Errorf("value decode failed: %w", err)
		return series.Sample{}, it.err
	}

	it.pos++

	return series.Sample{
		Timestamp: timestamp,
		Value:     value,
	}, nil
}

// Err returns the last error encountered by the iterator.
func (it *ChunkIterator) Err() error {
	return it.err
}

// Count returns the total number of samples in the chunk.
func (it *ChunkIterator) Count() int {
	return it.count
}

// WriteTo writes the chunk to a writer.
func (c *Chunk) WriteTo(w io.Writer) (int64, error) {
	data, err := c.MarshalBinary()
	if err != nil {
		return 0, err
	}

	n, err := w.Write(data)
	return int64(n), err
}

// ReadFrom reads a chunk from a reader.
func (c *Chunk) ReadFrom(r io.Reader) (int64, error) {
	// Read header first to know data length
	header := make([]byte, ChunkHeaderSize)
	n, err := io.ReadFull(r, header)
	if err != nil {
		return int64(n), err
	}

	dataLen := binary.BigEndian.Uint32(header[18:22])

	// Read data and footer
	remaining := make([]byte, dataLen+ChunkFooterSize)
	n2, err := io.ReadFull(r, remaining)
	totalRead := int64(n + n2)
	if err != nil {
		return totalRead, err
	}

	// Combine header + data + footer
	fullData := make([]byte, ChunkHeaderSize+dataLen+ChunkFooterSize)
	copy(fullData, header)
	copy(fullData[ChunkHeaderSize:], remaining)

	// Unmarshal
	err = c.UnmarshalBinary(fullData)
	return totalRead, err
}
