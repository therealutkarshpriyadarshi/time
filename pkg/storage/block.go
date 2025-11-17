package storage

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

const (
	// DefaultBlockDuration is the default time span for a block (2 hours)
	DefaultBlockDuration = 2 * time.Hour

	// MetaFilename is the name of the metadata file within a block directory
	MetaFilename = "meta.json"

	// ChunksDirName is the name of the chunks directory within a block
	ChunksDirName = "chunks"

	// BlockVersion is the current block format version
	BlockVersion = 1
)

// BlockMeta contains metadata about a block.
// Stored as meta.json in the block directory.
type BlockMeta struct {
	// ULID is the unique identifier for this block (sortable by time)
	ULID string `json:"ulid"`

	// MinTime is the minimum timestamp in the block (Unix milliseconds)
	MinTime int64 `json:"minTime"`

	// MaxTime is the maximum timestamp in the block (Unix milliseconds)
	MaxTime int64 `json:"maxTime"`

	// Stats contains statistics about the block
	Stats BlockStats `json:"stats"`

	// Version is the block format version
	Version int `json:"version"`
}

// BlockStats contains statistics about block contents.
type BlockStats struct {
	// NumSamples is the total number of samples in the block
	NumSamples uint64 `json:"numSamples"`

	// NumSeries is the number of unique series in the block
	NumSeries uint64 `json:"numSeries"`

	// NumChunks is the total number of chunks in the block
	NumChunks uint64 `json:"numChunks"`
}

// Block represents a time-partitioned immutable block of data.
//
// Directory structure:
//
//	01H8XABC00000000/          # Block directory (ULID)
//	├── meta.json              # Block metadata
//	└── chunks/                # Chunks directory
//	    ├── 000000000000000001 # Chunk file (series hash as filename)
//	    ├── 000000000000000002
//	    └── ...
type Block struct {
	meta      BlockMeta
	dir       string
	chunksDir string
}

// NewBlock creates a new block with the given time range.
func NewBlock(minTime, maxTime int64, dir string) (*Block, error) {
	// Generate ULID based on minTime
	entropy := ulid.Monotonic(rand.Reader, 0)
	timestamp := time.UnixMilli(minTime)
	id := ulid.MustNew(ulid.Timestamp(timestamp), entropy)

	blockDir := filepath.Join(dir, id.String())
	chunksDir := filepath.Join(blockDir, ChunksDirName)

	// Create directories
	if err := os.MkdirAll(chunksDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create block directories: %w", err)
	}

	b := &Block{
		meta: BlockMeta{
			ULID:    id.String(),
			MinTime: minTime,
			MaxTime: maxTime,
			Version: BlockVersion,
		},
		dir:       blockDir,
		chunksDir: chunksDir,
	}

	return b, nil
}

// OpenBlock opens an existing block from disk.
func OpenBlock(dir string) (*Block, error) {
	// Read metadata
	metaPath := filepath.Join(dir, MetaFilename)
	f, err := os.Open(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open meta file: %w", err)
	}
	defer f.Close()

	var meta BlockMeta
	if err := json.NewDecoder(f).Decode(&meta); err != nil {
		return nil, fmt.Errorf("failed to decode meta file: %w", err)
	}

	// Validate version
	if meta.Version != BlockVersion {
		return nil, fmt.Errorf("unsupported block version: %d", meta.Version)
	}

	chunksDir := filepath.Join(dir, ChunksDirName)

	return &Block{
		meta:      meta,
		dir:       dir,
		chunksDir: chunksDir,
	}, nil
}

// WriteChunk writes a chunk for a given series to the block.
// The chunk must be sealed before writing.
func (b *Block) WriteChunk(seriesHash uint64, chunk *Chunk) error {
	if !chunk.IsSealed() {
		return fmt.Errorf("chunk must be sealed before writing")
	}

	// Create chunk filename from series hash
	chunkPath := filepath.Join(b.chunksDir, fmt.Sprintf("%016x", seriesHash))

	// Open file (create or append)
	f, err := os.OpenFile(chunkPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open chunk file: %w", err)
	}
	defer f.Close()

	// Write chunk
	if _, err := chunk.WriteTo(f); err != nil {
		return fmt.Errorf("failed to write chunk: %w", err)
	}

	// Update stats
	b.meta.Stats.NumSamples += uint64(chunk.NumSamples)
	b.meta.Stats.NumChunks++

	return nil
}

// ReadChunks reads all chunks for a given series from the block.
func (b *Block) ReadChunks(seriesHash uint64) ([]*Chunk, error) {
	chunkPath := filepath.Join(b.chunksDir, fmt.Sprintf("%016x", seriesHash))

	// Check if file exists
	if _, err := os.Stat(chunkPath); os.IsNotExist(err) {
		// No chunks for this series
		return nil, nil
	}

	f, err := os.Open(chunkPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open chunk file: %w", err)
	}
	defer f.Close()

	chunks := make([]*Chunk, 0)

	for {
		chunk := &Chunk{}
		_, err := chunk.ReadFrom(f)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read chunk: %w", err)
		}
		chunks = append(chunks, chunk)
	}

	return chunks, nil
}

// Query returns samples for a given series within the specified time range.
func (b *Block) Query(seriesHash uint64, minTime, maxTime int64) ([]series.Sample, error) {
	// Check if block overlaps with query range
	if maxTime < b.meta.MinTime || minTime > b.meta.MaxTime {
		// No overlap
		return nil, nil
	}

	chunks, err := b.ReadChunks(seriesHash)
	if err != nil {
		return nil, err
	}

	if len(chunks) == 0 {
		return nil, nil
	}

	result := make([]series.Sample, 0)

	for _, chunk := range chunks {
		// Skip chunks that don't overlap with query range
		if chunk.MaxTime < minTime || chunk.MinTime > maxTime {
			continue
		}

		it, err := chunk.Iterator()
		if err != nil {
			return nil, fmt.Errorf("failed to create chunk iterator: %w", err)
		}

		for it.Next() {
			sample, err := it.At()
			if err != nil {
				return nil, fmt.Errorf("failed to read sample: %w", err)
			}

			// Filter by time range
			if sample.Timestamp >= minTime && sample.Timestamp <= maxTime {
				result = append(result, sample)
			}
		}
	}

	return result, nil
}

// WriteMeta writes the block metadata to disk.
func (b *Block) WriteMeta() error {
	metaPath := filepath.Join(b.dir, MetaFilename)

	f, err := os.Create(metaPath)
	if err != nil {
		return fmt.Errorf("failed to create meta file: %w", err)
	}
	defer f.Close()

	encoder := json.NewEncoder(f)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(&b.meta); err != nil {
		return fmt.Errorf("failed to encode meta file: %w", err)
	}

	return nil
}

// Meta returns the block metadata.
func (b *Block) Meta() BlockMeta {
	return b.meta
}

// Dir returns the block directory path.
func (b *Block) Dir() string {
	return b.dir
}

// ULID returns the block's ULID.
func (b *Block) ULID() string {
	return b.meta.ULID
}

// IncrementSeriesCount increments the series count in block stats.
func (b *Block) IncrementSeriesCount() {
	b.meta.Stats.NumSeries++
}

// Contains returns true if the given timestamp falls within the block's time range.
func (b *Block) Contains(timestamp int64) bool {
	return timestamp >= b.meta.MinTime && timestamp <= b.meta.MaxTime
}

// Overlaps returns true if the given time range overlaps with the block's time range.
func (b *Block) Overlaps(minTime, maxTime int64) bool {
	return !(maxTime < b.meta.MinTime || minTime > b.meta.MaxTime)
}

// Delete removes the block directory and all its contents.
func (b *Block) Delete() error {
	return os.RemoveAll(b.dir)
}

// Validate checks the block for consistency and corruption.
func (b *Block) Validate() error {
	// Check if meta file exists
	metaPath := filepath.Join(b.dir, MetaFilename)
	if _, err := os.Stat(metaPath); os.IsNotExist(err) {
		return fmt.Errorf("meta file not found")
	}

	// Check if chunks directory exists
	if _, err := os.Stat(b.chunksDir); os.IsNotExist(err) {
		return fmt.Errorf("chunks directory not found")
	}

	// Read all chunk files and validate
	entries, err := os.ReadDir(b.chunksDir)
	if err != nil {
		return fmt.Errorf("failed to read chunks directory: %w", err)
	}

	totalSamples := uint64(0)
	totalChunks := uint64(0)

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		chunkPath := filepath.Join(b.chunksDir, entry.Name())
		f, err := os.Open(chunkPath)
		if err != nil {
			return fmt.Errorf("failed to open chunk file %s: %w", entry.Name(), err)
		}

		// Read and validate all chunks in the file
		for {
			chunk := &Chunk{}
			_, err := chunk.ReadFrom(f)
			if err == io.EOF {
				break
			}
			if err != nil {
				f.Close()
				return fmt.Errorf("failed to read chunk from %s: %w", entry.Name(), err)
			}

			totalSamples += uint64(chunk.NumSamples)
			totalChunks++
		}

		f.Close()
	}

	// Verify stats match
	if b.meta.Stats.NumSamples != totalSamples {
		return fmt.Errorf("sample count mismatch: meta has %d, actual is %d", b.meta.Stats.NumSamples, totalSamples)
	}

	if b.meta.Stats.NumChunks != totalChunks {
		return fmt.Errorf("chunk count mismatch: meta has %d, actual is %d", b.meta.Stats.NumChunks, totalChunks)
	}

	return nil
}

// Size returns the total size of the block directory in bytes.
func (b *Block) Size() (int64, error) {
	var size int64

	err := filepath.Walk(b.dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			size += info.Size()
		}
		return nil
	})

	return size, err
}
