package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/therealutkarshpriyadarshi/time/pkg/series"
)

// InvertedIndex is an inverted index for fast label-based series lookup.
// It maps label name-value pairs to posting lists (sets of series IDs).
//
// Structure:
//   labelName -> labelValue -> PostingList (roaring bitmap of series IDs)
//
// Example:
//   "host" -> "server1" -> [1, 5, 42, 100, ...]
//   "host" -> "server2" -> [2, 6, 43, 101, ...]
//   "metric" -> "cpu" -> [1, 2, 3, 4, ...]
//
// This enables fast queries like:
//   - Find all series with host="server1"
//   - Find all series with host="server1" AND metric="cpu"
//   - Find all series with host=~"server.*"
type InvertedIndex struct {
	mu sync.RWMutex

	// index maps label name -> label value -> posting list (bitmap of series IDs)
	index map[string]map[string]*roaring.Bitmap

	// labelNames tracks all unique label names for efficient iteration
	labelNames map[string]struct{}

	// labelValues tracks all unique label name-value pairs for cardinality tracking
	labelValues map[string]map[string]struct{}

	// seriesCount is the total number of series indexed
	seriesCount int
}

// NewInvertedIndex creates a new inverted index.
func NewInvertedIndex() *InvertedIndex {
	return &InvertedIndex{
		index:       make(map[string]map[string]*roaring.Bitmap),
		labelNames:  make(map[string]struct{}),
		labelValues: make(map[string]map[string]struct{}),
	}
}

// Add adds a series to the index with the given series ID and labels.
// If the series already exists, it updates the index (idempotent).
func (idx *InvertedIndex) Add(id series.SeriesID, labels map[string]string) error {
	if id == 0 {
		return fmt.Errorf("invalid series ID: 0")
	}
	if len(labels) == 0 {
		return fmt.Errorf("labels cannot be empty")
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Add to posting lists for each label
	for name, value := range labels {
		// Ensure the label name exists in the index
		if _, exists := idx.index[name]; !exists {
			idx.index[name] = make(map[string]*roaring.Bitmap)
			idx.labelNames[name] = struct{}{}
		}

		// Ensure the label value exists
		if _, exists := idx.index[name][value]; !exists {
			idx.index[name][value] = roaring.New()
		}

		// Track label value for cardinality
		if _, exists := idx.labelValues[name]; !exists {
			idx.labelValues[name] = make(map[string]struct{})
		}
		idx.labelValues[name][value] = struct{}{}

		// Add series ID to the posting list
		idx.index[name][value].Add(uint32(id))
	}

	idx.seriesCount++
	return nil
}

// Lookup finds all series IDs that match the given matchers.
// All matchers must be satisfied (AND operation).
// Returns a roaring bitmap of matching series IDs.
func (idx *InvertedIndex) Lookup(matchers Matchers) (*roaring.Bitmap, error) {
	if len(matchers) == 0 {
		return nil, fmt.Errorf("at least one matcher required")
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Start with all series (universal set)
	var result *roaring.Bitmap

	// Process each matcher and intersect results
	for i, m := range matchers {
		matchedIDs := idx.lookupMatcher(m)

		if i == 0 {
			result = matchedIDs
		} else {
			// Intersect with previous results (AND operation)
			result = roaring.And(result, matchedIDs)
		}

		// Early exit if no matches
		if result.IsEmpty() {
			return roaring.New(), nil
		}
	}

	return result, nil
}

// lookupMatcher finds all series IDs that match a single matcher.
// Must be called with read lock held.
func (idx *InvertedIndex) lookupMatcher(m *Matcher) *roaring.Bitmap {
	switch m.Type {
	case MatchEqual:
		return idx.lookupEqual(m.Name, m.Value)

	case MatchNotEqual:
		return idx.lookupNotEqual(m.Name, m.Value)

	case MatchRegexp:
		return idx.lookupRegexp(m)

	case MatchNotRegexp:
		return idx.lookupNotRegexp(m)

	default:
		return roaring.New() // empty result
	}
}

// lookupEqual finds series with exact label match.
func (idx *InvertedIndex) lookupEqual(name, value string) *roaring.Bitmap {
	if values, exists := idx.index[name]; exists {
		if bitmap, exists := values[value]; exists {
			return bitmap.Clone()
		}
	}
	return roaring.New() // empty result
}

// lookupNotEqual finds series that don't have the label value.
// This includes series without the label at all.
func (idx *InvertedIndex) lookupNotEqual(name, value string) *roaring.Bitmap {
	result := idx.allSeries()

	if values, exists := idx.index[name]; exists {
		if bitmap, exists := values[value]; exists {
			// Remove series with the exact label value
			result = roaring.AndNot(result, bitmap)
		}
	}

	return result
}

// lookupRegexp finds series where label value matches the regex.
func (idx *InvertedIndex) lookupRegexp(m *Matcher) *roaring.Bitmap {
	result := roaring.New()

	if values, exists := idx.index[m.Name]; exists {
		for value, bitmap := range values {
			if m.Matches(value) {
				result = roaring.Or(result, bitmap)
			}
		}
	}

	return result
}

// lookupNotRegexp finds series where label value doesn't match the regex.
func (idx *InvertedIndex) lookupNotRegexp(m *Matcher) *roaring.Bitmap {
	matched := idx.lookupRegexp(m)

	// Result = all series - series matching the regex
	// This gives us series without the label OR with a non-matching value
	result := idx.allSeries()
	result = roaring.AndNot(result, matched)

	return result
}

// allSeries returns a bitmap of all series IDs in the index.
func (idx *InvertedIndex) allSeries() *roaring.Bitmap {
	result := roaring.New()

	// Collect all series IDs from all posting lists
	for _, values := range idx.index {
		for _, bitmap := range values {
			result = roaring.Or(result, bitmap)
		}
	}

	return result
}

// allSeriesWithLabel returns a bitmap of all series that have the given label.
func (idx *InvertedIndex) allSeriesWithLabel(name string) *roaring.Bitmap {
	result := roaring.New()

	if values, exists := idx.index[name]; exists {
		for _, bitmap := range values {
			result = roaring.Or(result, bitmap)
		}
	}

	return result
}

// Delete removes a series from the index.
func (idx *InvertedIndex) Delete(id series.SeriesID) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Remove from all posting lists
	for name := range idx.index {
		for value := range idx.index[name] {
			idx.index[name][value].Remove(uint32(id))

			// Clean up empty bitmaps
			if idx.index[name][value].IsEmpty() {
				delete(idx.index[name], value)
				if values, exists := idx.labelValues[name]; exists {
					delete(values, value)
				}
			}
		}

		// Clean up empty label names
		if len(idx.index[name]) == 0 {
			delete(idx.index, name)
			delete(idx.labelNames, name)
			delete(idx.labelValues, name)
		}
	}

	idx.seriesCount--
}

// LabelNames returns all unique label names in the index.
func (idx *InvertedIndex) LabelNames() []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	names := make([]string, 0, len(idx.labelNames))
	for name := range idx.labelNames {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// LabelValues returns all unique values for a given label name.
func (idx *InvertedIndex) LabelValues(name string) []string {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if values, exists := idx.labelValues[name]; exists {
		result := make([]string, 0, len(values))
		for value := range values {
			result = append(result, value)
		}
		sort.Strings(result)
		return result
	}

	return nil
}

// Stats returns statistics about the index.
type IndexStats struct {
	SeriesCount       int            // Total number of series
	LabelCount        int            // Number of unique label names
	LabelValueCount   map[string]int // Number of unique values per label
	PostingListSizes  map[string]map[string]int // Size of each posting list
	TotalPostingLists int            // Total number of posting lists
	MemoryBytes       uint64         // Approximate memory usage in bytes
}

// Stats returns current index statistics.
func (idx *InvertedIndex) Stats() IndexStats {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	stats := IndexStats{
		SeriesCount:      idx.seriesCount,
		LabelCount:       len(idx.labelNames),
		LabelValueCount:  make(map[string]int),
		PostingListSizes: make(map[string]map[string]int),
	}

	var memoryBytes uint64

	for name, values := range idx.index {
		stats.LabelValueCount[name] = len(values)
		stats.PostingListSizes[name] = make(map[string]int)

		for value, bitmap := range values {
			size := int(bitmap.GetCardinality())
			stats.PostingListSizes[name][value] = size
			stats.TotalPostingLists++

			// Approximate memory: bitmap size in bytes
			memoryBytes += bitmap.GetSizeInBytes()
		}
	}

	stats.MemoryBytes = memoryBytes
	return stats
}

// WriteTo writes the index to the given writer in a compact binary format.
// Format:
//   - Header: magic number (4 bytes) + version (4 bytes)
//   - Series count (8 bytes)
//   - Number of label names (4 bytes)
//   - For each label name:
//     - Name length (4 bytes) + name bytes
//     - Number of values (4 bytes)
//     - For each value:
//       - Value length (4 bytes) + value bytes
//       - Roaring bitmap serialized bytes
func (idx *InvertedIndex) WriteTo(w io.Writer) (int64, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	buf := new(bytes.Buffer)

	// Write header
	magic := uint32(0x54534458) // "TSDX" in hex
	version := uint32(1)
	if err := binary.Write(buf, binary.LittleEndian, magic); err != nil {
		return 0, err
	}
	if err := binary.Write(buf, binary.LittleEndian, version); err != nil {
		return 0, err
	}

	// Write series count
	if err := binary.Write(buf, binary.LittleEndian, uint64(idx.seriesCount)); err != nil {
		return 0, err
	}

	// Get sorted label names for deterministic output
	labelNames := make([]string, 0, len(idx.index))
	for name := range idx.index {
		labelNames = append(labelNames, name)
	}
	sort.Strings(labelNames)

	// Write number of label names
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(labelNames))); err != nil {
		return 0, err
	}

	// Write each label name and its values
	for _, name := range labelNames {
		// Write label name
		if err := writeString(buf, name); err != nil {
			return 0, err
		}

		values := idx.index[name]

		// Get sorted values for deterministic output
		sortedValues := make([]string, 0, len(values))
		for value := range values {
			sortedValues = append(sortedValues, value)
		}
		sort.Strings(sortedValues)

		// Write number of values
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(sortedValues))); err != nil {
			return 0, err
		}

		// Write each value and its bitmap
		for _, value := range sortedValues {
			// Write value
			if err := writeString(buf, value); err != nil {
				return 0, err
			}

			// Serialize bitmap
			bitmap := values[value]
			bitmapBytes, err := bitmap.ToBytes()
			if err != nil {
				return 0, fmt.Errorf("failed to serialize bitmap: %w", err)
			}

			// Write bitmap length and data
			if err := binary.Write(buf, binary.LittleEndian, uint32(len(bitmapBytes))); err != nil {
				return 0, err
			}
			if _, err := buf.Write(bitmapBytes); err != nil {
				return 0, err
			}
		}
	}

	// Write to the actual writer
	n, err := w.Write(buf.Bytes())
	return int64(n), err
}

// ReadFrom reads the index from the given reader.
func (idx *InvertedIndex) ReadFrom(r io.Reader) (int64, error) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	// Read all data into buffer
	buf := new(bytes.Buffer)
	n, err := buf.ReadFrom(r)
	if err != nil {
		return n, err
	}

	// Read and verify header
	var magic, version uint32
	if err := binary.Read(buf, binary.LittleEndian, &magic); err != nil {
		return n, err
	}
	if magic != 0x54534458 {
		return n, fmt.Errorf("invalid magic number: 0x%x", magic)
	}
	if err := binary.Read(buf, binary.LittleEndian, &version); err != nil {
		return n, err
	}
	if version != 1 {
		return n, fmt.Errorf("unsupported version: %d", version)
	}

	// Read series count
	var seriesCount uint64
	if err := binary.Read(buf, binary.LittleEndian, &seriesCount); err != nil {
		return n, err
	}

	// Read number of label names
	var labelCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &labelCount); err != nil {
		return n, err
	}

	// Clear existing index
	idx.index = make(map[string]map[string]*roaring.Bitmap)
	idx.labelNames = make(map[string]struct{})
	idx.labelValues = make(map[string]map[string]struct{})

	// Read each label name and its values
	for i := 0; i < int(labelCount); i++ {
		// Read label name
		name, err := readString(buf)
		if err != nil {
			return n, err
		}

		idx.index[name] = make(map[string]*roaring.Bitmap)
		idx.labelNames[name] = struct{}{}
		idx.labelValues[name] = make(map[string]struct{})

		// Read number of values
		var valueCount uint32
		if err := binary.Read(buf, binary.LittleEndian, &valueCount); err != nil {
			return n, err
		}

		// Read each value and its bitmap
		for j := 0; j < int(valueCount); j++ {
			// Read value
			value, err := readString(buf)
			if err != nil {
				return n, err
			}

			idx.labelValues[name][value] = struct{}{}

			// Read bitmap length
			var bitmapLen uint32
			if err := binary.Read(buf, binary.LittleEndian, &bitmapLen); err != nil {
				return n, err
			}

			// Read bitmap data
			bitmapBytes := make([]byte, bitmapLen)
			if _, err := io.ReadFull(buf, bitmapBytes); err != nil {
				return n, err
			}

			// Deserialize bitmap
			bitmap := roaring.New()
			if err := bitmap.UnmarshalBinary(bitmapBytes); err != nil {
				return n, fmt.Errorf("failed to deserialize bitmap: %w", err)
			}

			idx.index[name][value] = bitmap
		}
	}

	idx.seriesCount = int(seriesCount)
	return n, nil
}

// writeString writes a length-prefixed string to the buffer.
func writeString(buf *bytes.Buffer, s string) error {
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(s))); err != nil {
		return err
	}
	_, err := buf.WriteString(s)
	return err
}

// readString reads a length-prefixed string from the buffer.
func readString(buf *bytes.Buffer) (string, error) {
	var length uint32
	if err := binary.Read(buf, binary.LittleEndian, &length); err != nil {
		return "", err
	}

	bytes := make([]byte, length)
	if _, err := io.ReadFull(buf, bytes); err != nil {
		return "", err
	}

	return string(bytes), nil
}
