package compression

import (
	"fmt"
	"math"
	"math/bits"
)

// ValueEncoder implements Gorilla value compression using XOR-based encoding.
// Based on the Facebook Gorilla paper: https://www.vldb.org/pvldb/vol8/p1816-teller.pdf
//
// Encoding scheme:
// - First value: stored as full 64-bit float
// - Subsequent values: XOR with previous value
//   - If XOR == 0: write 1 bit (0)
//   - If XOR != 0: write 1 bit (1) followed by:
//     - If leading/trailing zeros match previous pattern:
//       - Write control bit (0)
//       - Write significant bits using previous pattern
//     - Otherwise:
//       - Write control bit (1)
//       - Write 5 bits for number of leading zeros
//       - Write 6 bits for number of significant bits
//       - Write significant bits
type ValueEncoder struct {
	w            *BitWriter
	prevValue    uint64 // Previous value as uint64
	prevLeading  uint8  // Number of leading zeros in previous XOR
	prevTrailing uint8  // Number of trailing zeros in previous XOR
	count        int    // Number of values encoded
	finalized    bool   // Whether Finish() has been called
}

// NewValueEncoder creates a new value encoder.
func NewValueEncoder() *ValueEncoder {
	return &ValueEncoder{
		w: NewBitWriter(128),
	}
}

// Encode adds a float64 value to the encoder.
func (e *ValueEncoder) Encode(v float64) error {
	if e.finalized {
		return fmt.Errorf("encoder already finalized")
	}

	// Convert float64 to uint64 bits
	vBits := math.Float64bits(v)

	if e.count == 0 {
		// First value: store as-is
		e.prevValue = vBits
		e.w.WriteUint64(vBits)
		e.count++
		return nil
	}

	// XOR with previous value
	xor := vBits ^ e.prevValue

	if xor == 0 {
		// Value hasn't changed: write single '0' bit
		e.w.WriteBit(0)
	} else {
		// Value changed: write '1' bit
		e.w.WriteBit(1)

		// Count leading and trailing zeros
		leading := uint8(bits.LeadingZeros64(xor))
		trailing := uint8(bits.TrailingZeros64(xor))
		significant := 64 - leading - trailing

		// Check if we can reuse the previous block information
		if e.count > 1 && leading >= e.prevLeading && trailing >= e.prevTrailing {
			// Use previous block: write control bit '0'
			e.w.WriteBit(0)

			// Write significant bits using previous block size
			blockSize := 64 - e.prevLeading - e.prevTrailing
			significantBits := (xor >> e.prevTrailing) & ((1 << blockSize) - 1)
			e.w.WriteBits(significantBits, blockSize)
		} else {
			// New block: write control bit '1'
			e.w.WriteBit(1)

			// Write leading zeros (5 bits, max value 31 to leave room for data)
			if leading > 31 {
				leading = 31
			}
			e.w.WriteBits(uint64(leading), 5)

			// Write significant bits length (6 bits, supports up to 64 bits)
			if significant > 64 {
				significant = 64
			}
			e.w.WriteBits(uint64(significant), 6)

			// Write the significant bits
			significantBits := (xor >> trailing) & ((1 << significant) - 1)
			e.w.WriteBits(significantBits, significant)

			// Update block information for next value
			e.prevLeading = leading
			e.prevTrailing = trailing
		}
	}

	e.prevValue = vBits
	e.count++
	return nil
}

// Finish finalizes the encoding and returns the compressed bytes.
// Returns a copy to avoid issues with buffer reuse after Reset().
func (e *ValueEncoder) Finish() []byte {
	e.finalized = true
	data := e.w.Bytes()
	result := make([]byte, len(data))
	copy(result, data)
	return result
}

// Count returns the number of values encoded.
func (e *ValueEncoder) Count() int {
	return e.count
}

// Reset resets the encoder for reuse.
func (e *ValueEncoder) Reset() {
	e.w.Reset()
	e.prevValue = 0
	e.prevLeading = 0
	e.prevTrailing = 0
	e.count = 0
	e.finalized = false
}

// ValueDecoder decodes values compressed with ValueEncoder.
type ValueDecoder struct {
	r            *BitReader
	prevValue    uint64 // Previous value as uint64
	prevLeading  uint8  // Number of leading zeros in previous XOR
	prevTrailing uint8  // Number of trailing zeros in previous XOR
	count        int    // Number of values decoded
	err          error  // Last error encountered
}

// NewValueDecoder creates a new value decoder from compressed data.
func NewValueDecoder(data []byte) *ValueDecoder {
	return &ValueDecoder{
		r: NewBitReader(data),
	}
}

// Decode reads the next float64 value.
func (d *ValueDecoder) Decode() (float64, error) {
	if d.err != nil {
		return 0, d.err
	}

	if d.count == 0 {
		// First value: read as-is
		val, err := d.r.ReadUint64()
		if err != nil {
			d.err = err
			return 0, err
		}
		d.prevValue = val
		d.count++
		return math.Float64frombits(val), nil
	}

	// Read control bit
	bit, err := d.r.ReadBit()
	if err != nil {
		d.err = err
		return 0, err
	}

	var xor uint64

	if bit == 0 {
		// Value hasn't changed
		xor = 0
	} else {
		// Read second control bit
		controlBit, err := d.r.ReadBit()
		if err != nil {
			d.err = err
			return 0, err
		}

		if controlBit == 0 {
			// Use previous block
			blockSize := 64 - d.prevLeading - d.prevTrailing
			significantBits, err := d.r.ReadBits(blockSize)
			if err != nil {
				d.err = err
				return 0, err
			}

			xor = significantBits << d.prevTrailing
		} else {
			// Read new block
			leading, err := d.r.ReadBits(5)
			if err != nil {
				d.err = err
				return 0, err
			}

			significant, err := d.r.ReadBits(6)
			if err != nil {
				d.err = err
				return 0, err
			}

			if significant == 0 {
				significant = 64
			}

			significantBits, err := d.r.ReadBits(uint8(significant))
			if err != nil {
				d.err = err
				return 0, err
			}

			// Calculate trailing zeros
			trailing := 64 - uint8(leading) - uint8(significant)

			xor = significantBits << trailing

			// Update block information
			d.prevLeading = uint8(leading)
			d.prevTrailing = trailing
		}
	}

	// XOR with previous value to get current value
	valueBits := xor ^ d.prevValue
	d.prevValue = valueBits
	d.count++

	return math.Float64frombits(valueBits), nil
}

// Count returns the number of values decoded so far.
func (d *ValueDecoder) Count() int {
	return d.count
}

// Err returns the last error encountered during decoding.
func (d *ValueDecoder) Err() error {
	return d.err
}

// Reset resets the decoder to decode a new buffer.
func (d *ValueDecoder) Reset(data []byte) {
	d.r.Reset(data)
	d.prevValue = 0
	d.prevLeading = 0
	d.prevTrailing = 0
	d.count = 0
	d.err = nil
}
