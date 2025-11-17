package storage

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

// Block represents a time-partitioned immutable block of time-series data.
// Blocks organize data into 2-hour windows (configurable) and use ULID
// for sortable, time-based identification.
//
// Directory structure:
//   data/
//   ├── 01H8XABC00000000/    # Block ULID (sortable by time)
//   │   ├── meta.json         # Block metadata
//   │   ├── chunks/           # Compressed chunks directory
//   │   │   ├── 000001        # Chunk file for series 1
//   │   │   ├── 000002        # Chunk file for series 2
//   │   │   └── ...
//   │   └── index             # Series index (future: inverted index)
//   └── 01H8XDEF00000000/
//       └── ...
type Block struct {
	// Metadata
	ULID    ulid.ULID // Unique, time-sortable identifier
	MinTime int64     // Minimum timestamp in block
	MaxTime int64     // Maximum timestamp in block

	// Statistics
	NumSamples int64 // Total number of samples
	NumSeries  int64 // Total number of unique series
	NumChunks  int64 // Total number of chunks

	// Directory path
	dir string

	// In-memory series data (seriesHash -> chunk data)
	chunks       map[uint64]*Chunk
	series       map[uint64]*series.Series
	seriesChunks map[uint64]int // seriesHash -> chunkFile number (for lazy loading)

	mu sync.RWMutex
}

// BlockMeta contains block metadata stored in meta.json
type BlockMeta struct {
	ULID         string            `json:"ulid"`
	MinTime      int64             `json:"minTime"`
	MaxTime      int64             `json:"maxTime"`
	Stats        BlockStats        `json:"stats"`
	Version      int               `json:"version"`
	Labels       map[string]string `json:"labels,omitempty"`
	SeriesChunks map[string]int    `json:"seriesChunks"` // seriesHash -> chunkFile number
}

// BlockStats contains block statistics
type BlockStats struct {
	NumSamples int64 `json:"numSamples"`
	NumSeries  int64 `json:"numSeries"`
	NumChunks  int64 `json:"numChunks"`
}

const (
	// BlockVersion is the current block format version
	BlockVersion = 1

	// ChunksDir is the subdirectory for chunks
	ChunksDir = "chunks"

	// MetaFile is the metadata file name
	MetaFile = "meta.json"

	// IndexFile is the index file name (placeholder for Phase 4)
	IndexFile = "index"

	// DefaultBlockDuration is the default block time window (2 hours)
	DefaultBlockDuration = 2 * time.Hour
)

// NewBlock creates a new empty block
func NewBlock(minTime, maxTime int64) (*Block, error) {
	// Generate ULID based on minTime
	entropy := rand.New(rand.NewSource(time.Now().UnixNano()))
	blockULID, err := ulid.New(uint64(minTime), entropy)
	if err != nil {
		return nil, fmt.Errorf("failed to generate ULID: %w", err)
	}

	return &Block{
		ULID:         blockULID,
		MinTime:      minTime,
		MaxTime:      maxTime,
		chunks:       make(map[uint64]*Chunk),
		series:       make(map[uint64]*series.Series),
		seriesChunks: make(map[uint64]int),
	}, nil
}

// OpenBlock opens an existing block from disk
func OpenBlock(dir string) (*Block, error) {
	// Read metadata
	metaPath := filepath.Join(dir, MetaFile)
	metaData, err := os.ReadFile(metaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read block metadata: %w", err)
	}

	var meta BlockMeta
	if err := json.Unmarshal(metaData, &meta); err != nil {
		return nil, fmt.Errorf("failed to parse block metadata: %w", err)
	}

	// Parse ULID
	blockULID, err := ulid.Parse(meta.ULID)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ULID: %w", err)
	}

	// Convert SeriesChunks map from string keys to uint64 keys
	seriesChunks := make(map[uint64]int)
	for hashStr, chunkNum := range meta.SeriesChunks {
		var hash uint64
		fmt.Sscanf(hashStr, "%d", &hash)
		seriesChunks[hash] = chunkNum
	}

	block := &Block{
		ULID:         blockULID,
		MinTime:      meta.MinTime,
		MaxTime:      meta.MaxTime,
		NumSamples:   meta.Stats.NumSamples,
		NumSeries:    meta.Stats.NumSeries,
		NumChunks:    meta.Stats.NumChunks,
		dir:          dir,
		chunks:       make(map[uint64]*Chunk),
		series:       make(map[uint64]*series.Series),
		seriesChunks: seriesChunks,
	}

	return block, nil
}

// AddSeries adds a series with its samples to the block
func (b *Block) AddSeries(s *series.Series, samples []series.Sample) error {
	if len(samples) == 0 {
		return fmt.Errorf("cannot add series with zero samples")
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	// Store series metadata
	b.series[s.Hash] = s

	// Create chunk from samples
	chunk := NewChunk()
	if err := chunk.Append(samples); err != nil {
		return fmt.Errorf("failed to create chunk: %w", err)
	}

	// Store chunk
	b.chunks[s.Hash] = chunk

	// Update statistics
	b.NumSamples += int64(len(samples))
	b.NumChunks++

	// Update time range if needed
	if len(samples) > 0 {
		if samples[0].Timestamp < b.MinTime || b.MinTime == 0 {
			b.MinTime = samples[0].Timestamp
		}
		if samples[len(samples)-1].Timestamp > b.MaxTime {
			b.MaxTime = samples[len(samples)-1].Timestamp
		}
	}

	return nil
}

// GetSeries retrieves samples for a series within a time range
func (b *Block) GetSeries(seriesHash uint64, minTime, maxTime int64) ([]series.Sample, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	chunk, ok := b.chunks[seriesHash]
	if !ok {
		// Try to load chunk from disk (lazy loading)
		chunkNum, exists := b.seriesChunks[seriesHash]
		if !exists {
			return nil, nil // Series not found in this block
		}

		// Load chunk from disk
		chunkFile := filepath.Join(b.dir, ChunksDir, fmt.Sprintf("%06d", chunkNum))
		loadedChunk, err := b.LoadChunk(chunkFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load chunk: %w", err)
		}

		// Cache the loaded chunk
		b.chunks[seriesHash] = loadedChunk
		chunk = loadedChunk
	}

	// Check if time range overlaps with chunk
	if maxTime < chunk.MinTime || minTime > chunk.MaxTime {
		return nil, nil // No overlap
	}

	// Iterate through chunk and filter by time range
	iter, err := chunk.Iterator()
	if err != nil {
		return nil, fmt.Errorf("failed to create iterator: %w", err)
	}

	var result []series.Sample
	for iter.Next() {
		sample, err := iter.At()
		if err != nil {
			return nil, fmt.Errorf("failed to read sample: %w", err)
		}

		// Filter by time range
		if sample.Timestamp >= minTime && sample.Timestamp <= maxTime {
			result = append(result, sample)
		}
	}

	if iter.Err() != nil {
		return nil, iter.Err()
	}

	return result, nil
}

// Persist writes the block to disk
func (b *Block) Persist(dataDir string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Create block directory
	blockDir := filepath.Join(dataDir, b.ULID.String())
	if err := os.MkdirAll(blockDir, 0755); err != nil {
		return fmt.Errorf("failed to create block directory: %w", err)
	}

	// Create chunks directory
	chunksDir := filepath.Join(blockDir, ChunksDir)
	if err := os.MkdirAll(chunksDir, 0755); err != nil {
		return fmt.Errorf("failed to create chunks directory: %w", err)
	}

	// Write chunks and build seriesChunks mapping
	chunkNum := 1
	seriesChunksMap := make(map[string]int)
	for seriesHash, chunk := range b.chunks {
		chunkFile := filepath.Join(chunksDir, fmt.Sprintf("%06d", chunkNum))
		f, err := os.Create(chunkFile)
		if err != nil {
			return fmt.Errorf("failed to create chunk file: %w", err)
		}

		if _, err := chunk.WriteTo(f); err != nil {
			f.Close()
			return fmt.Errorf("failed to write chunk: %w", err)
		}

		f.Close()

		// Store mapping for lazy loading
		b.seriesChunks[seriesHash] = chunkNum
		seriesChunksMap[fmt.Sprintf("%d", seriesHash)] = chunkNum

		chunkNum++
	}

	// Update series count
	b.NumSeries = int64(len(b.series))

	// Write metadata
	meta := BlockMeta{
		ULID:         b.ULID.String(),
		MinTime:      b.MinTime,
		MaxTime:      b.MaxTime,
		Stats: BlockStats{
			NumSamples: b.NumSamples,
			NumSeries:  b.NumSeries,
			NumChunks:  b.NumChunks,
		},
		Version:      BlockVersion,
		SeriesChunks: seriesChunksMap,
	}

	metaData, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal metadata: %w", err)
	}

	metaPath := filepath.Join(blockDir, MetaFile)
	if err := os.WriteFile(metaPath, metaData, 0644); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	// Create placeholder index file (will be implemented in Phase 4)
	indexPath := filepath.Join(blockDir, IndexFile)
	if err := os.WriteFile(indexPath, []byte{}, 0644); err != nil {
		return fmt.Errorf("failed to create index file: %w", err)
	}

	b.dir = blockDir
	return nil
}

// Delete removes the block from disk
func (b *Block) Delete() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.dir == "" {
		return fmt.Errorf("block not persisted to disk")
	}

	return os.RemoveAll(b.dir)
}

// Dir returns the block directory path
func (b *Block) Dir() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.dir
}

// String returns a human-readable representation of the block
func (b *Block) String() string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return fmt.Sprintf("Block{ULID: %s, TimeRange: [%d, %d], Series: %d, Samples: %d, Chunks: %d}",
		b.ULID.String(),
		b.MinTime,
		b.MaxTime,
		b.NumSeries,
		b.NumSamples,
		b.NumChunks,
	)
}

// Overlaps checks if the block overlaps with the given time range
func (b *Block) Overlaps(minTime, maxTime int64) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.MinTime <= maxTime && b.MaxTime >= minTime
}

// Size returns the approximate size of the block in bytes
func (b *Block) Size() int64 {
	b.mu.RLock()
	defer b.mu.RUnlock()

	var size int64
	for _, chunk := range b.chunks {
		size += int64(chunk.Size())
	}
	return size
}

// BlockWriter helps write MemTable data to blocks
type BlockWriter struct {
	dataDir       string
	blockDuration time.Duration
}

// NewBlockWriter creates a new block writer
func NewBlockWriter(dataDir string) *BlockWriter {
	return &BlockWriter{
		dataDir:       dataDir,
		blockDuration: DefaultBlockDuration,
	}
}

// WriteMemTable writes a MemTable to disk as a block
func (bw *BlockWriter) WriteMemTable(mt *MemTable) (*Block, error) {
	minTime, maxTime := mt.TimeRange()
	if minTime == 0 && maxTime == 0 {
		return nil, fmt.Errorf("memtable is empty")
	}

	// Create new block
	block, err := NewBlock(minTime, maxTime)
	if err != nil {
		return nil, fmt.Errorf("failed to create block: %w", err)
	}

	// Get all series from MemTable
	allSeriesHashes := mt.AllSeries()

	// Add each series to the block
	for _, hash := range allSeriesHashes {
		// Get series metadata
		s, ok := mt.GetSeries(hash)
		if !ok {
			continue
		}

		// Query samples for this series
		samples, err := mt.Query(hash, minTime, maxTime)
		if err != nil {
			return nil, fmt.Errorf("failed to query series %d: %w", hash, err)
		}

		if len(samples) > 0 {
			if err := block.AddSeries(s, samples); err != nil {
				return nil, fmt.Errorf("failed to add series to block: %w", err)
			}
		}
	}

	// Persist block to disk
	if err := block.Persist(bw.dataDir); err != nil {
		return nil, fmt.Errorf("failed to persist block: %w", err)
	}

	return block, nil
}

// BlockReader helps read blocks from disk
type BlockReader struct {
	dataDir string
	blocks  []*Block
	mu      sync.RWMutex
}

// NewBlockReader creates a new block reader
func NewBlockReader(dataDir string) *BlockReader {
	return &BlockReader{
		dataDir: dataDir,
		blocks:  make([]*Block, 0),
	}
}

// LoadBlocks loads all blocks from the data directory
func (br *BlockReader) LoadBlocks() error {
	br.mu.Lock()
	defer br.mu.Unlock()

	// List block directories
	entries, err := os.ReadDir(br.dataDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No blocks yet
		}
		return fmt.Errorf("failed to read data directory: %w", err)
	}

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		// Check if it's a valid ULID
		if _, err := ulid.Parse(entry.Name()); err != nil {
			continue // Skip non-ULID directories
		}

		// Open block
		blockDir := filepath.Join(br.dataDir, entry.Name())
		block, err := OpenBlock(blockDir)
		if err != nil {
			return fmt.Errorf("failed to open block %s: %w", entry.Name(), err)
		}

		br.blocks = append(br.blocks, block)
	}

	// Sort blocks by time (ULID is time-sortable)
	sort.Slice(br.blocks, func(i, j int) bool {
		return br.blocks[i].ULID.Time() < br.blocks[j].ULID.Time()
	})

	return nil
}

// Query retrieves samples for a series across all blocks
func (br *BlockReader) Query(seriesHash uint64, minTime, maxTime int64) ([]series.Sample, error) {
	br.mu.RLock()
	defer br.mu.RUnlock()

	var result []series.Sample

	// Query each overlapping block
	for _, block := range br.blocks {
		if !block.Overlaps(minTime, maxTime) {
			continue
		}

		samples, err := block.GetSeries(seriesHash, minTime, maxTime)
		if err != nil {
			return nil, fmt.Errorf("failed to query block %s: %w", block.ULID.String(), err)
		}

		result = append(result, samples...)
	}

	return result, nil
}

// Blocks returns all loaded blocks
func (br *BlockReader) Blocks() []*Block {
	br.mu.RLock()
	defer br.mu.RUnlock()

	blocks := make([]*Block, len(br.blocks))
	copy(blocks, br.blocks)
	return blocks
}

// LoadChunk loads a specific chunk from a block
func (b *Block) LoadChunk(chunkFile string) (*Chunk, error) {
	f, err := os.Open(chunkFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open chunk file: %w", err)
	}
	defer f.Close()

	chunk := NewChunk()
	if _, err := chunk.ReadFrom(f); err != nil && err != io.EOF {
		return nil, fmt.Errorf("failed to read chunk: %w", err)
	}

	return chunk, nil
}
