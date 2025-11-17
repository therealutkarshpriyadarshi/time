package wal

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

const (
	// DefaultSegmentSize is the default size for WAL segments (128MB)
	DefaultSegmentSize = 128 * 1024 * 1024

	// WAL file format constants
	walVersion      = 1
	entryHeaderSize = 20 // version(1) + type(1) + length(4) + checksum(4) + timestamp(8) + reserved(2)

	// Entry types
	entryTypeSamples = 1
	entryTypeFlush   = 2
	entryTypeTruncate = 3
)

var (
	// ErrCorrupted indicates the WAL file is corrupted
	ErrCorrupted = fmt.Errorf("wal: corrupted entry")

	// ErrClosed indicates the WAL is closed
	ErrClosed = fmt.Errorf("wal: closed")
)

// Entry represents a single WAL entry
type Entry struct {
	Type      uint8
	Timestamp int64
	Series    *series.Series
	Samples   []series.Sample
}

// WAL implements a write-ahead log for durability
type WAL struct {
	dir           string
	segmentSize   int64
	currentSegment int
	file          *os.File
	writer        *bufio.Writer
	size          int64
	mu            sync.Mutex
	closed        bool
}

// Options configures the WAL
type Options struct {
	SegmentSize int64
}

// DefaultOptions returns default WAL options
func DefaultOptions() *Options {
	return &Options{
		SegmentSize: DefaultSegmentSize,
	}
}

// Open opens or creates a WAL in the specified directory
func Open(dir string, opts *Options) (*WAL, error) {
	if opts == nil {
		opts = DefaultOptions()
	}

	// Create directory if it doesn't exist
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("wal: failed to create directory: %w", err)
	}

	w := &WAL{
		dir:         dir,
		segmentSize: opts.SegmentSize,
	}

	// Find the latest segment or create a new one
	segments, err := w.listSegments()
	if err != nil {
		return nil, err
	}

	if len(segments) > 0 {
		w.currentSegment = segments[len(segments)-1]
	} else {
		w.currentSegment = 0
	}

	// Open or create the current segment
	if err := w.openSegment(w.currentSegment); err != nil {
		return nil, err
	}

	return w, nil
}

// Append writes an entry to the WAL
func (w *WAL) Append(s *series.Series, samples []series.Sample) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrClosed
	}

	entry := &Entry{
		Type:      entryTypeSamples,
		Timestamp: time.Now().UnixMilli(),
		Series:    s,
		Samples:   samples,
	}

	data, err := encodeEntry(entry)
	if err != nil {
		return fmt.Errorf("wal: failed to encode entry: %w", err)
	}

	// Check if we need to rotate
	if w.size+int64(len(data)) > w.segmentSize {
		if err := w.rotate(); err != nil {
			return err
		}
	}

	// Write to buffer
	n, err := w.writer.Write(data)
	if err != nil {
		return fmt.Errorf("wal: failed to write entry: %w", err)
	}

	w.size += int64(n)

	// Flush to ensure durability
	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("wal: failed to flush: %w", err)
	}

	// Sync to disk for durability
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: failed to sync: %w", err)
	}

	return nil
}

// LogFlush writes a flush marker to the WAL
func (w *WAL) LogFlush(timestamp int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrClosed
	}

	entry := &Entry{
		Type:      entryTypeFlush,
		Timestamp: timestamp,
	}

	data, err := encodeEntry(entry)
	if err != nil {
		return fmt.Errorf("wal: failed to encode flush entry: %w", err)
	}

	if _, err := w.writer.Write(data); err != nil {
		return fmt.Errorf("wal: failed to write flush entry: %w", err)
	}

	if err := w.writer.Flush(); err != nil {
		return fmt.Errorf("wal: failed to flush: %w", err)
	}

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: failed to sync: %w", err)
	}

	return nil
}

// Replay reads all WAL entries and returns them for recovery
func (w *WAL) Replay() ([]Entry, error) {
	segments, err := w.listSegments()
	if err != nil {
		return nil, err
	}

	var entries []Entry

	for _, segNum := range segments {
		segmentEntries, err := w.replaySegment(segNum)
		if err != nil {
			return nil, fmt.Errorf("wal: failed to replay segment %d: %w", segNum, err)
		}
		entries = append(entries, segmentEntries...)
	}

	return entries, nil
}

// Truncate removes WAL segments older than the specified timestamp
func (w *WAL) Truncate(beforeTimestamp int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return ErrClosed
	}

	segments, err := w.listSegments()
	if err != nil {
		return err
	}

	// Keep at least the current segment
	for _, segNum := range segments {
		if segNum >= w.currentSegment {
			continue
		}

		// Check if this segment contains data before the timestamp
		path := w.segmentPath(segNum)
		lastEntry, err := w.getLastEntryTimestamp(path)
		if err != nil {
			continue // Skip segments we can't read
		}

		// Only delete if all entries are older than the timestamp
		if lastEntry < beforeTimestamp {
			if err := os.Remove(path); err != nil {
				return fmt.Errorf("wal: failed to remove segment %d: %w", segNum, err)
			}
		}
	}

	return nil
}

// Close closes the WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}

	w.closed = true

	if w.writer != nil {
		if err := w.writer.Flush(); err != nil {
			return err
		}
	}

	if w.file != nil {
		return w.file.Close()
	}

	return nil
}

// rotate creates a new WAL segment
func (w *WAL) rotate() error {
	// Close current file
	if w.writer != nil {
		if err := w.writer.Flush(); err != nil {
			return err
		}
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return err
		}
	}

	// Increment segment number
	w.currentSegment++

	// Open new segment
	return w.openSegment(w.currentSegment)
}

// openSegment opens a specific segment file
func (w *WAL) openSegment(segNum int) error {
	path := w.segmentPath(segNum)

	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("wal: failed to open segment: %w", err)
	}

	// Get current file size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return fmt.Errorf("wal: failed to stat segment: %w", err)
	}

	w.file = file
	w.writer = bufio.NewWriter(file)
	w.size = stat.Size()

	return nil
}

// segmentPath returns the file path for a segment
func (w *WAL) segmentPath(segNum int) string {
	return filepath.Join(w.dir, fmt.Sprintf("wal-%08d", segNum))
}

// listSegments returns all segment numbers in ascending order
func (w *WAL) listSegments() ([]int, error) {
	files, err := os.ReadDir(w.dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("wal: failed to list segments: %w", err)
	}

	var segments []int
	for _, file := range files {
		if file.IsDir() {
			continue
		}

		var segNum int
		if _, err := fmt.Sscanf(file.Name(), "wal-%08d", &segNum); err == nil {
			segments = append(segments, segNum)
		}
	}

	sort.Ints(segments)
	return segments, nil
}

// replaySegment reads all entries from a specific segment
func (w *WAL) replaySegment(segNum int) ([]Entry, error) {
	path := w.segmentPath(segNum)

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("wal: failed to open segment for replay: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var entries []Entry

	for {
		entry, err := decodeEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			// Log corruption but continue
			fmt.Printf("wal: corrupted entry in segment %d: %v\n", segNum, err)
			break
		}
		entries = append(entries, *entry)
	}

	return entries, nil
}

// getLastEntryTimestamp returns the timestamp of the last entry in a segment
func (w *WAL) getLastEntryTimestamp(path string) (int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	var lastTimestamp int64

	for {
		entry, err := decodeEntry(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
		lastTimestamp = entry.Timestamp
	}

	return lastTimestamp, nil
}

// encodeEntry serializes an entry to bytes
func encodeEntry(entry *Entry) ([]byte, error) {
	// Calculate payload size
	payloadSize := 0

	if entry.Series != nil {
		// Series labels
		payloadSize += 4 // number of labels
		for k, v := range entry.Series.Labels {
			payloadSize += 4 + len(k) + 4 + len(v)
		}
		payloadSize += 8 // hash
	}

	if entry.Samples != nil {
		// Samples
		payloadSize += 4 // number of samples
		payloadSize += len(entry.Samples) * 16 // timestamp(8) + value(8)
	}

	totalSize := entryHeaderSize + payloadSize
	buf := make([]byte, totalSize)

	// Write header
	offset := 0
	buf[offset] = walVersion
	offset++
	buf[offset] = entry.Type
	offset++
	binary.BigEndian.PutUint32(buf[offset:], uint32(payloadSize))
	offset += 4
	// Checksum will be filled later
	offset += 4
	binary.BigEndian.PutUint64(buf[offset:], uint64(entry.Timestamp))
	offset += 8
	// Reserved
	offset += 2

	// Write payload
	if entry.Series != nil {
		// Write labels
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(entry.Series.Labels)))
		offset += 4

		// Sort labels for deterministic encoding
		keys := make([]string, 0, len(entry.Series.Labels))
		for k := range entry.Series.Labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, k := range keys {
			v := entry.Series.Labels[k]
			binary.BigEndian.PutUint32(buf[offset:], uint32(len(k)))
			offset += 4
			copy(buf[offset:], k)
			offset += len(k)
			binary.BigEndian.PutUint32(buf[offset:], uint32(len(v)))
			offset += 4
			copy(buf[offset:], v)
			offset += len(v)
		}

		// Write hash
		binary.BigEndian.PutUint64(buf[offset:], entry.Series.Hash)
		offset += 8
	}

	if entry.Samples != nil {
		// Write samples
		binary.BigEndian.PutUint32(buf[offset:], uint32(len(entry.Samples)))
		offset += 4

		for _, sample := range entry.Samples {
			binary.BigEndian.PutUint64(buf[offset:], uint64(sample.Timestamp))
			offset += 8
			binary.BigEndian.PutUint64(buf[offset:], uint64(sample.Value))
			offset += 8
		}
	}

	// Calculate and write checksum (skip version, type, length, and checksum fields)
	checksum := crc32.ChecksumIEEE(buf[10:])
	binary.BigEndian.PutUint32(buf[6:], checksum)

	return buf, nil
}

// decodeEntry deserializes an entry from a reader
func decodeEntry(r *bufio.Reader) (*Entry, error) {
	// Read header
	header := make([]byte, entryHeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	// Parse header
	version := header[0]
	if version != walVersion {
		return nil, fmt.Errorf("wal: unsupported version %d", version)
	}

	entryType := header[1]
	payloadLen := binary.BigEndian.Uint32(header[2:6])
	storedChecksum := binary.BigEndian.Uint32(header[6:10])
	timestamp := int64(binary.BigEndian.Uint64(header[10:18]))

	// Read payload
	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("wal: failed to read payload: %w", err)
	}

	// Verify checksum
	computedChecksum := crc32.ChecksumIEEE(append(header[10:], payload...))
	if storedChecksum != computedChecksum {
		return nil, ErrCorrupted
	}

	entry := &Entry{
		Type:      entryType,
		Timestamp: timestamp,
	}

	// Decode payload based on type
	if entryType == entryTypeSamples {
		offset := 0

		// Read labels
		if offset+4 > len(payload) {
			return nil, ErrCorrupted
		}
		numLabels := binary.BigEndian.Uint32(payload[offset:])
		offset += 4

		labels := make(map[string]string, numLabels)
		for i := 0; i < int(numLabels); i++ {
			if offset+4 > len(payload) {
				return nil, ErrCorrupted
			}
			keyLen := binary.BigEndian.Uint32(payload[offset:])
			offset += 4

			if offset+int(keyLen) > len(payload) {
				return nil, ErrCorrupted
			}
			key := string(payload[offset : offset+int(keyLen)])
			offset += int(keyLen)

			if offset+4 > len(payload) {
				return nil, ErrCorrupted
			}
			valLen := binary.BigEndian.Uint32(payload[offset:])
			offset += 4

			if offset+int(valLen) > len(payload) {
				return nil, ErrCorrupted
			}
			val := string(payload[offset : offset+int(valLen)])
			offset += int(valLen)

			labels[key] = val
		}

		if offset+8 > len(payload) {
			return nil, ErrCorrupted
		}
		hash := binary.BigEndian.Uint64(payload[offset:])
		offset += 8

		entry.Series = &series.Series{
			Labels: labels,
			Hash:   hash,
		}

		// Read samples
		if offset+4 > len(payload) {
			return nil, ErrCorrupted
		}
		numSamples := binary.BigEndian.Uint32(payload[offset:])
		offset += 4

		samples := make([]series.Sample, numSamples)
		for i := 0; i < int(numSamples); i++ {
			if offset+16 > len(payload) {
				return nil, ErrCorrupted
			}
			samples[i].Timestamp = int64(binary.BigEndian.Uint64(payload[offset:]))
			offset += 8
			samples[i].Value = float64(binary.BigEndian.Uint64(payload[offset:]))
			offset += 8
		}

		entry.Samples = samples
	}

	return entry, nil
}
